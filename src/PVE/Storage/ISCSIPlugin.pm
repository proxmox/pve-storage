package PVE::Storage::ISCSIPlugin;

use strict;
use warnings;

use Digest::SHA qw(sha256_hex);
use Fcntl qw(LOCK_EX LOCK_NB);
use IO::Dir;
use IO::File;

use PVE::ISCSI::Topology;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Multipath;
use PVE::Multipath::Identity;
use PVE::Storage::Plugin;
use PVE::Tools
    qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach $IPV4RE $IPV6RE);

use base qw(PVE::Storage::Plugin);

# iscsi helper function

my $ISCSIADM = '/usr/bin/iscsiadm';
our $INITIATOR_NAME_FILE = '/etc/iscsi/initiatorname.iscsi';
our $SYSFS = '/sys';

sub read_initiator_info {
    my $content = eval { PVE::Tools::file_get_contents($INITIATOR_NAME_FILE, 64 * 1024) };
    return undef if !defined($content);

    my $res = {};
    for my $line (split(/\n/, $content)) {
        if ($line =~ m/^\s*InitiatorName\s*=\s*(\S+)\s*$/) {
            $res->{name} = $1;
        } elsif ($line =~ m/^\s*InitiatorAlias\s*=\s*(.+?)\s*$/) {
            $res->{alias} = $1;
        }
    }
    return defined($res->{name}) ? $res : undef;
}

my $found_iscsi_adm_exe;
my sub assert_iscsi_support {
    my ($noerr) = @_;
    return $found_iscsi_adm_exe if $found_iscsi_adm_exe; # assume it won't be removed if ever found

    $found_iscsi_adm_exe = -x $ISCSIADM;

    if (!$found_iscsi_adm_exe) {
        die "error: no iscsi support - please install open-iscsi\n" if !$noerr;
        warn "warning: no iscsi support - please install open-iscsi\n";
    }
    return $found_iscsi_adm_exe;
}

# Example: 192.168.122.252:3260,1 iqn.2003-01.org.linux-iscsi.proxmox-nfs.x8664:sn.00567885ba8f
my $ISCSI_TARGET_RE = qr/^(\S+:\d+)\,\S+\s+(\S+)\s*$/;

sub iscsi_session_list {
    assert_iscsi_support();

    my $cmd = [$ISCSIADM, '--mode', 'session'];

    my $res = {};
    eval {
        run_command(
            $cmd,
            errmsg => 'iscsi session scan failed',
            outfunc => sub {
                my $line = shift;
                # example: tcp: [1] 192.168.122.252:3260,1 iqn.2003-01.org.linux-iscsi.proxmox-nfs.x8664:sn.00567885ba8f (non-flash)
                if ($line =~ m/^\S+:\s+\[(\S+)\]\s+(\S+:\d+)\,\S+\s+(\S+)\s+\S+?\s*$/) {
                    my ($session_id, $portal, $target) = ($1, $2, $3);
                    # there can be several sessions per target (multipath)
                    push @{ $res->{$target} }, { session_id => $session_id, portal => $portal };
                }
            },
        );
    };
    if (my $err = $@) {
        die $err if $err !~ m/: No active sessions.$/i;
    }

    return $res;
}

sub iscsi_node_list {
    assert_iscsi_support();

    my $res = [];
    eval {
        run_command(
            [$ISCSIADM, '--mode', 'node'],
            errmsg => 'iscsi node record scan failed',
            outfunc => sub {
                my $line = shift;
                if ($line =~ $ISCSI_TARGET_RE) {
                    push @$res, { portal => $1, target => $2 };
                }
            },
        );
    };
    if (my $err = $@) {
        die $err if $err !~ m/: No records found/i;
    }

    return $res;
}

sub iscsi_supported {
    return -x $ISCSIADM ? 1 : 0;
}

sub iscsi_test_session {
    my ($sid) = @_;

    if ($sid !~ m/^[0-9]+$/) {
        die "session_id: '$sid' is not a number\n";
    }
    my $state = file_read_firstline("/sys/class/iscsi_session/session${sid}/state");
    return defined($state) && $state eq 'LOGGED_IN';
}

sub iscsi_test_portal {
    my ($target, $portal, $cache) = @_;
    $cache //= {};

    if (defined($target)) {
        # check session state instead if available
        my $sessions = iscsi_session($cache, $target);
        for my $session ($sessions->@*) {
            next if $session->{portal} ne $portal;
            my $state = iscsi_test_session($session->{session_id});
            return $state if $state;
        }
    }
    # check portal via tcp
    my ($server, $port) = PVE::Tools::parse_host_and_port($portal);
    return 0 if !$server;
    return PVE::Network::tcp_ping($server, $port || 3260, 2);
}

sub iscsi_portals {
    my ($target, $portal_in) = @_;

    assert_iscsi_support();

    my $res = [];
    my $cmd = [$ISCSIADM, '--mode', 'node'];
    eval {
        run_command(
            $cmd,
            outfunc => sub {
                my $line = shift;

                if ($line =~ $ISCSI_TARGET_RE) {
                    my ($portal, $portal_target) = ($1, $2);
                    if ($portal_target eq $target) {
                        push @{$res}, $portal;
                    }
                }
            },
        );
    };

    my $err = $@;
    warn $err if $err;

    if ($err || !scalar(@$res)) {
        return [$portal_in];
    } else {
        return $res;
    }
}

sub iscsi_discovery {
    my ($target_in, $portals, $cache) = @_;

    assert_iscsi_support();

    my $res = {};
    my ($answered, $succeeded, $last_error);
    for my $portal ($portals->@*) {
        next if !iscsi_test_portal($target_in, $portal, $cache);
        $answered = 1;

        my $cmd = [$ISCSIADM, '--mode', 'discovery', '--type', 'sendtargets', '--portal', $portal];
        eval {
            run_command(
                $cmd,
                errmsg => $portal,
                outfunc => sub {
                    my $line = shift;

                    if ($line =~ $ISCSI_TARGET_RE) {
                        my ($portal, $target) = ($1, $2);
                        # one target can have more than one portal (multipath)
                        # and sendtargets should return all of them in single call
                        push @{ $res->{$target} }, $portal;
                    }
                },
            );
        };

        if (my $err = $@) {
            # iscsiadm fails with this message if the portal reports no targets
            if ($err !~ m/: No portals found$/) {
                $last_error = $err;
                $res = {};
                next;
            }
        }
        $succeeded = 1;

        # In case of multipath we can stop after receiving targets from any available portal
        last if scalar(keys %$res) > 0;
    }

    die "no portal of the target answered: " . join(', ', $portals->@*) . "\n" if !$answered;
    die "discovery failed on every portal - $last_error" if !$succeeded;

    return $res;
}

sub iscsi_login {
    my ($target, $portals, $cache) = @_;

    assert_iscsi_support();

    eval { iscsi_discovery($target, $portals, $cache); };
    warn $@ if $@;

    # Disable retries to avoid blocking pvestatd for too long, next iteration will retry anyway
    eval {
        my $cmd = [
            $ISCSIADM,
            '--mode',
            'node',
            '--targetname',
            $target,
            '--op',
            'update',
            '--name',
            'node.session.initial_login_retry_max',
            '--value',
            '0',
        ];
        run_command($cmd);
    };
    warn $@ if $@;

    run_command([$ISCSIADM, '--mode', 'node', '--targetname', $target, '--login']);
}

sub iscsi_logout {
    my ($target) = @_;

    assert_iscsi_support();

    run_command([$ISCSIADM, '--mode', 'node', '--targetname', $target, '--logout']);
}

our $ISCSI_RESCAN_DIR = '/run';

sub claim_iscsi_rescan {
    my ($target) = @_;

    my $filename = "$ISCSI_RESCAN_DIR/pve-iscsi-rescan-" . sha256_hex($target);
    my $claimed = eval {
        my $fh = IO::File->new($filename, '+>>') or die "cannot open '$filename' - $!\n";
        if (!flock($fh, LOCK_EX | LOCK_NB)) {
            die "cannot lock '$filename' - $!\n" if !$!{EWOULDBLOCK};
            # a concurrent claim is updating the timestamp, and rescans if one is due
            $fh->close();
            return 0;
        }
        $fh->seek(0, 0) or die "cannot seek '$filename' - $!\n";
        my $last = $fh->getline();
        die "cannot read '$filename' - $!\n" if $fh->error();
        # a timestamp left incomplete, for example by a crash while writing it, counts as none
        $last = undef if defined($last) && $last !~ m/^\d+\n$/;
        my $now = time();
        my $elapsed = defined($last) ? $now - $last : undef;
        my $claim = !defined($elapsed) || $elapsed < 0 || $elapsed >= 10;

        if ($claim) {
            $fh->truncate(0) or die "cannot update '$filename' - $!\n";
            $fh->print("$now\n") && $fh->flush() or die "cannot update '$filename' - $!\n";
        }
        $fh->close() or die "cannot close '$filename' - $!\n";
        $claim;
    };
    if (my $err = $@) {
        warn "skipping iSCSI rescan of target '$target' - $err";
        return 0;
    }
    return $claimed;
}

sub iscsi_session_rescan {
    my ($target, $session_list) = @_;

    assert_iscsi_support();
    return if !claim_iscsi_rescan($target);

    foreach my $session (@$session_list) {
        my $cmd = [$ISCSIADM, '--mode', 'session', '--sid', $session->{session_id}, '--rescan'];
        eval {
            run_command($cmd, outfunc => sub { });
        };
        warn $@ if $@;
    }
}

sub load_stable_scsi_paths {

    my $stable_paths = {};

    my $stabledir = "/dev/disk/by-id";

    if (my $dh = IO::Dir->new($stabledir)) {
        foreach my $tmp (sort $dh->read) {
            # exclude filenames with part in name (same disk but partitions)
            # use only filenames with scsi(with multipath i have the same device
            # with dm-uuid-mpath , dm-name and scsi in name)
            if ($tmp !~ m/-part\d+$/ && ($tmp =~ m/^scsi-/ || $tmp =~ m/^dm-uuid-mpath-/)) {
                my $path = "$stabledir/$tmp";
                my $bdevdest = readlink($path);
                if ($bdevdest && $bdevdest =~ m|^../../([^/]+)|) {
                    $stable_paths->{$1} = $tmp;
                }
            }
        }
        $dh->close;
    }
    return $stable_paths;
}

# The LUNs of the iSCSI sessions, { target => { volname => info } }. With $map_names a LUN is named
# after its multipath map, see PVE::Multipath::Identity, and only the LUNs of $target are listed.
sub iscsi_device_list {
    my ($map_names, $target) = @_;

    my $res = {};

    my $stable_paths = load_stable_scsi_paths();
    my $ids = $map_names ? PVE::Multipath::Identity::collect(targets => { $target => 1 }) : undef;

    my $device_map = PVE::ISCSI::Topology::device_map($SYSFS);

    for my $bdev (sort keys %$device_map) {
        my $info = $device_map->{$bdev};
        my ($channel, $id, $lun) = $info->@{qw(channel id lun)};

        my $path_dev = $bdev;

        #check multipath
        if (-d "$SYSFS/block/$bdev/holders") {
            my $multipathdev = dir_glob_regex("$SYSFS/block/$bdev/holders", '[A-Za-z]\S*');
            $bdev = $multipathdev if $multipathdev;
        }

        my $blockdev = $stable_paths->{$bdev};
        # scsi-<wwid> can name a member before assembly; dm-uuid-mpath-<wwid> only names the map.
        # Managed volumes keep the map name even while assembly is pending.
        if ($map_names) {
            my $wwid =
                $info->{target} eq $target
                ? PVE::Multipath::Identity::path_wwid($ids, $path_dev)
                : undef;
            $blockdev = defined($wwid) ? "dm-uuid-mpath-$wwid" : undef;
        }
        next if !$blockdev;

        my $size = file_read_firstline("$SYSFS/block/$bdev/size");
        next if !$size;

        my $volid = "$channel.$id.$lun.$blockdev";

        $res->{ $info->{target} }->{$volid} = {
            'format' => 'raw',
            'size' => int($size * 512),
            'vmid' => 0, # not assigned to any vm
            'channel' => $channel,
            'id' => $id,
            'lun' => $lun,
        };
    }

    return $res;
}

# Each portal of the connection storage contributes one expected path.
sub multipath_lun_wwids {
    my ($class, $scfg, $ids) = @_;

    my $res = { wwids => {} };
    my $target = $scfg->{target};

    my @wwids =
        map { $_->{wwid} } grep { ($_->{target} // '') eq $target } values $ids->{paths}->%*;
    # the portal lookup needs iscsiadm, which a node without paths of the target may lack
    return $res if !@wwids;
    my $portals = iscsi_portals($target, $scfg->{portal});
    $res->{wwids}->{$_} = { 'paths-expected' => scalar(@$portals) } for @wwids;

    return $res;
}

# Configuration

sub type {
    return 'iscsi';
}

sub plugindata {
    return {
        content => [{ images => 1, 'vm-vol' => 1, none => 1 }, { images => 1 }],
        select_existing => 1,
        'advanced-properties' => {},
        'hidden-properties' => {
            bwlimit => 1,
        },
        'sensitive-properties' => {},
    };
}

sub properties {
    return {
        target => {
            title => "Target",
            description => "iSCSI target name (an iSCSI Qualified Name - IQN).",
            type => 'string',
        },
        portal => {
            title => "Portal",
            description => "iSCSI portal (IP or hostname, optionally with port).",
            type => 'string',
            format => 'pve-storage-portal-dns',
        },
        'managed-multipath' => {
            title => "Multipath",
            description => "Manage multipath access to the storage's LUNs and include them in"
                . " the cluster-wide path-health monitoring.",
            type => 'boolean',
        },
    };
}

sub options {
    return {
        portal => { fixed => 1 },
        target => { fixed => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        content => { optional => 1 },
        bwlimit => { optional => 1 },
        'managed-multipath' => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^\d+\.\d+\.\d+\.([^/\s]+)$!) {
        return ($class->guest_volume_vtype('raw'), $1, undef, undef, undef, undef, 'raw');
    }

    die "unable to parse iscsi volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    die "snapshot is not possible on iscsi storage\n" if defined($snapname);

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $path = "/dev/disk/by-id/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in iscsi storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in iscsi storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "can't allocate space in iscsi storage\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    die "can't free space in iscsi storage\n";
}

# list all luns regardless of set content_types, since we need it for
# listing in the gui and we can only have images anyway
sub list_volumes {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    my $res = $class->list_images($storeid, $scfg, $vmid);

    for my $item (@$res) {
        $item->{content} = 'images'; # we only have images
    }

    return $res;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $res = [];

    my $cache_key =
        $scfg->{'managed-multipath'} ? "iscsi_devices_mpath:$scfg->{target}" : 'iscsi_devices';
    $cache->{$cache_key} //= iscsi_device_list($scfg->{'managed-multipath'}, $scfg->{target});

    # we have no owner for iscsi devices

    my $target = $scfg->{target};

    if (my $dat = $cache->{$cache_key}->{$target}) {

        foreach my $volname (keys %$dat) {

            my $volid = "$storeid:$volname";

            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                # we have no owner for iscsi devices
                next if defined($vmid);
            }

            my $info = $dat->{$volname};
            $info->{volid} = $volid;

            push @$res, $info;
        }
    }

    return $res;
}

sub iscsi_session {
    my ($cache, $target) = @_;
    $cache->{iscsi_sessions} = iscsi_session_list() if !$cache->{iscsi_sessions};
    return $cache->{iscsi_sessions}->{$target};
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $session = iscsi_session($cache, $scfg->{target});
    my $active = defined($session) ? 1 : 0;

    return (0, 0, 0, $active);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return if !assert_iscsi_support(1);

    my $sessions = iscsi_session($cache, $scfg->{target});
    my $portals = iscsi_portals($scfg->{target}, $scfg->{portal});
    my $do_login = !defined($sessions);

    if (!$do_login) {
        # We should check that sessions for all portals are available
        my $session_portals = [map { $_->{portal} } (@$sessions)];

        for my $portal (@$portals) {
            if (!grep(/^\Q$portal\E$/, @$session_portals)) {
                $do_login = 1;
                last;
            }
        }
    }

    if ($do_login) {
        eval { iscsi_login($scfg->{target}, $portals, $cache); };
        warn $@ if $@;
    } else {
        # make sure we get all devices
        iscsi_session_rescan($scfg->{target}, $sessions);
    }
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return if !assert_iscsi_support(1);

    if (defined(iscsi_session($cache, $scfg->{target}))) {
        iscsi_logout($scfg->{target});
    }
}

my $check_devices_part_of_target = sub {
    my ($device_paths, $target) = @_;

    my $found = 0;
    for my $path (@$device_paths) {
        if ($path =~ m!/host\d+/session(\d+)/target\d+:\d+:\d+!) {
            my $session_id = $1;

            my $targetname = file_read_firstline(
                "/sys/class/iscsi_session/session$session_id/targetname",
            );
            if ($targetname && ($targetname eq $target)) {
                $found = 1;
                last;
            }
        }
    }
    return $found;
};

my $udev_query_path = sub {
    my ($dev) = @_;

    # only accept device names (see `man udevadm`)
    ($dev) = $dev =~ m!^(/dev/.+)$!; # untaint
    die "invalid device for udevadm path query\n" if !defined($dev);

    my $device_path;
    my $cmd = [
        'udevadm', 'info', '--query=path', $dev,
    ];
    eval {
        run_command(
            $cmd,
            outfunc => sub {
                $device_path = shift;
            },
        );
    };
    die "failed to query device path for '$dev': $@\n" if $@;

    ($device_path) = $device_path =~ m!^(/devices/.+)$!; # untaint
    die "invalid resolved device path\n" if !defined($device_path);

    return $device_path;
};

my $resolve_virtual_devices;
$resolve_virtual_devices = sub {
    my ($dev, $visited) = @_;

    $visited = {} if !defined($visited);

    my $resolved = [];
    if ($dev =~ m!^/devices/virtual/block/!) {
        # All paths of a multipath map lead to one LUN, possibly through other target names. Other
        # virtual devices, such as linear maps or logical volumes, can include disks outside the
        # target, so one disk of the target does not make them part of it.
        my $uuid = file_read_firstline("/sys/$dev/dm/uuid") // '';
        my ($name) = $dev =~ m!([^/]+)$!;
        die "'$name' is neither a multipath map nor a partition of one\n"
            if !PVE::Multipath::parse_dm_uuid($uuid);

        dir_glob_foreach(
            "/sys/$dev/slaves",
            '([^.].+)',
            sub {
                my ($slave) = @_;

                # don't check devices multiple times
                return if $visited->{$slave};
                $visited->{$slave} = 1;

                my $path;
                eval { $path = $udev_query_path->("/dev/$slave"); };
                return if $@;

                my $nested_resolved = $resolve_virtual_devices->($path, $visited);

                push @$resolved, @$nested_resolved;
            },
        );
    } else {
        push @$resolved, $dev;
    }

    return $resolved;
};

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    if ($scfg->{'managed-multipath'}) {
        my ($wwid) = $volname =~ m/\.dm-uuid-mpath-(\S+)$/;
        if (defined($wwid) && !-b "/dev/disk/by-id/dm-uuid-mpath-$wwid") {
            # A status pass has more storages to query and looks again in a few seconds anyway. So
            # it neither waits for the map nor asks multipathd for the paths of the LUN.
            if ($cache && $cache->{status_only}) {
                die "multipath map for WWID '$wwid' is not assembled"
                    . " - check the multipath status of this node\n";
            }
            # Activation can race map assembly at login or boot. Only wait for a LUN that is there.
            my $ids = PVE::Multipath::Identity::collect(targets => { $scfg->{target} => 1 });
            die "LUN with WWID '$wwid' is not visible on this node\n"
                if !grep { $_->{wwid} eq $wwid } values $ids->{paths}->%*;
            PVE::Multipath::wait_for_map($wwid)
                or die "multipath map for WWID '$wwid' did not assemble in time"
                . " - check the multipath status of this node\n";
        }
    }

    my $path = $class->filesystem_path($scfg, $volname, $snapname);
    my $real_path = Cwd::realpath($path);
    die "failed to get realpath for '$path': $!\n" if !$real_path;
    # in case $path does not exist or is not a symlink, check if the returned
    # $real_path is a block device
    die "resolved realpath '$real_path' is not a block device\n" if !-b $real_path;

    my $device_path = $udev_query_path->($real_path);
    my $resolved_paths = eval { $resolve_virtual_devices->($device_path) };
    die "volume '$volname' not part of target '$scfg->{target}' - $@" if $@;

    my $found = $check_devices_part_of_target->($resolved_paths, $scfg->{target});
    die "volume '$volname' not part of target '$scfg->{target}'\n" if !$found;
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;
    my $cache = {};
    my $portals = iscsi_portals($scfg->{target}, $scfg->{portal});

    for my $portal (@$portals) {
        my $result = iscsi_test_portal($scfg->{target}, $portal, $cache);
        return $result if $result;
    }

    return 0;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running, $snapname) = @_;
    die "volume resize is not possible on iscsi device";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => { current => 1 },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) = $class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    } else {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    return () if defined($snapshot); # not supported
    return () if defined($base_snapshot); # not supported
    return () if $with_snapshots; # not supported
    return ('raw+size');
}

sub volume_export {
    my (
        $class,
        $scfg,
        $storeid,
        $fh,
        $volname,
        $format,
        $snapshot,
        $base_snapshot,
        $with_snapshots,
    ) = @_;

    die "volume export format $format not available for $class\n" if $format ne 'raw+size';
    die "cannot export volumes together with their snapshots in $class\n" if $with_snapshots;
    die "cannot export an incremental stream in $class\n" if defined($base_snapshot);
    die "cannot export a snapshot in $class\n" if defined($snapshot);

    my $file = $class->filesystem_path($scfg, $volname, $snapshot);
    my $size;
    run_command(
        ['/sbin/blockdev', '--getsize64', $file],
        outfunc => sub {
            my ($line) = @_;
            die "unexpected output from /sbin/blockdev: $line\n" if $line !~ /^(\d+)$/;
            $size = int($1);
        },
    );
    PVE::Storage::Plugin::write_common_header($fh, $size);
    run_command(
        ['dd', "if=$file", "bs=64k", "status=progress"],
        output => '>&' . fileno($fh),
        # split dd's carriage-return driven progress output into individual log lines
        errfunc => sub { print STDERR "$_[0]\n" },
    );
    return;
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    return ();
}

sub volume_import {
    die "volume import is not possible on iscsi storage\n";
}

1;
