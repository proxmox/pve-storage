package PVE::Storage;

use strict;
use warnings;
use Data::Dumper;

use POSIX;
use IO::Select;
use IO::File;
use IO::Socket::IP;
use File::Basename;
use File::Path;
use Cwd 'abs_path';
use Socket;

use PVE::Tools qw(run_command file_read_firstline dir_glob_foreach $IPV6RE);
use PVE::Cluster qw(cfs_read_file cfs_write_file cfs_lock_file);
use PVE::Exception qw(raise_param_exc);
use PVE::JSONSchema;
use PVE::INotify;
use PVE::RPCEnvironment;

use PVE::Storage::Plugin;
use PVE::Storage::DirPlugin;
use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;
use PVE::Storage::NFSPlugin;
use PVE::Storage::CIFSPlugin;
use PVE::Storage::ISCSIPlugin;
use PVE::Storage::RBDPlugin;
use PVE::Storage::SheepdogPlugin;
use PVE::Storage::ISCSIDirectPlugin;
use PVE::Storage::GlusterfsPlugin;
use PVE::Storage::ZFSPoolPlugin;
use PVE::Storage::ZFSPlugin;
use PVE::Storage::DRBDPlugin;

# Storage API version. Icrement it on changes in storage API interface.
use constant APIVER => 1;

# load standard plugins
PVE::Storage::DirPlugin->register();
PVE::Storage::LVMPlugin->register();
PVE::Storage::LvmThinPlugin->register();
PVE::Storage::NFSPlugin->register();
PVE::Storage::CIFSPlugin->register();
PVE::Storage::ISCSIPlugin->register();
PVE::Storage::RBDPlugin->register();
PVE::Storage::SheepdogPlugin->register();
PVE::Storage::ISCSIDirectPlugin->register();
PVE::Storage::GlusterfsPlugin->register();
PVE::Storage::ZFSPoolPlugin->register();
PVE::Storage::ZFSPlugin->register();
PVE::Storage::DRBDPlugin->register();

# load third-party plugins
if ( -d '/usr/share/perl5/PVE/Storage/Custom' ) {
    dir_glob_foreach('/usr/share/perl5/PVE/Storage/Custom', '.*\.pm$', sub {
	my ($file) = @_;
	my $modname = 'PVE::Storage::Custom::' . $file;
	$modname =~ s!\.pm$!!;
	$file = 'PVE/Storage/Custom/' . $file;

	eval {
	    require $file;
	};
	if ($@) {
	    warn $@;
	# Check storage API version and that file is really storage plugin.
	} elsif ($modname->isa('PVE::Storage::Plugin') && $modname->can('api') && $modname->api() == APIVER) {
            eval {
	        import $file;
	        $modname->register();
            };
            warn $@ if $@;
	} else {
	    warn "Error loading storage plugin \"$modname\" because of API version mismatch. Please, update it.\n"
	}
    });
}

# initialize all plugins
PVE::Storage::Plugin->init();

my $UDEVADM = '/sbin/udevadm';

#  PVE::Storage utility functions

sub config {
    return cfs_read_file("storage.cfg");
}

sub write_config {
    my ($cfg) = @_;

    cfs_write_file('storage.cfg', $cfg);
}

sub lock_storage_config {
    my ($code, $errmsg) = @_;

    cfs_lock_file("storage.cfg", undef, $code);
    my $err = $@;
    if ($err) {
	$errmsg ? die "$errmsg: $err" : die $err;
    }
}

sub storage_config {
    my ($cfg, $storeid, $noerr) = @_;

    die "no storage ID specified\n" if !$storeid;

    my $scfg = $cfg->{ids}->{$storeid};

    die "storage '$storeid' does not exists\n" if (!$noerr && !$scfg);

    return $scfg;
}

sub storage_check_node {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config($cfg, $storeid);

    if ($scfg->{nodes}) {
	$node = PVE::INotify::nodename() if !$node || ($node eq 'localhost');
	if (!$scfg->{nodes}->{$node}) {
	    die "storage '$storeid' is not available on node '$node'\n" if !$noerr;
	    return undef;
	}
    }

    return $scfg;
}

sub storage_check_enabled {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config($cfg, $storeid);

    if ($scfg->{disable}) {
	die "storage '$storeid' is disabled\n" if !$noerr;
	return undef;
    }

    return storage_check_node($cfg, $storeid, $node, $noerr);
}

# storage_can_replicate:
# return true if storage supports replication
# (volumes alocated with vdisk_alloc() has replication feature)
sub storage_can_replicate {
    my ($cfg, $storeid, $format) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->storage_can_replicate($scfg, $storeid, $format);
}

sub storage_ids {
    my ($cfg) = @_;

    return keys %{$cfg->{ids}};
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    return PVE::Storage::Plugin::file_size_info($filename, $timeout);
}

sub volume_size_info {
    my ($cfg, $volid, $timeout) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	return $plugin->volume_size_info($scfg, $storeid, $volname, $timeout);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	return file_size_info($volid, $timeout);
    } else {
	return 0;
    }
}

sub volume_resize {
    my ($cfg, $volid, $size, $running) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_resize($scfg, $storeid, $volname, $size, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "resize file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_rollback_is_possible {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_rollback_is_possible($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot rollback file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_snapshot($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot_rollback {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$plugin->volume_rollback_is_possible($scfg, $storeid, $volname, $snap);
        return $plugin->volume_snapshot_rollback($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot rollback file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot_delete {
    my ($cfg, $volid, $snap, $running) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_snapshot_delete($scfg, $storeid, $volname, $snap, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot delete file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_has_feature {
    my ($cfg, $feature, $volid, $snap, $running) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_has_feature($scfg, $feature, $storeid, $volname, $snap, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	return undef;
    } else {
	return undef;
    }
}

sub volume_snapshot_list {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	return $plugin->volume_snapshot_list($scfg, $storeid, $volname);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	die "send file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
    # return an empty array if dataset does not exist.
}

sub get_image_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $path = $plugin->get_subdir($scfg, 'images');

    return $vmid ? "$path/$vmid" : $path;
}

sub get_private_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $path = $plugin->get_subdir($scfg, 'rootdir');

    return $vmid ? "$path/$vmid" : $path;
}

sub get_iso_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'iso');
}

sub get_vztmpl_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'vztmpl');
}

sub get_backup_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'backup');
}

# library implementation

sub parse_vmid {
    my $vmid = shift;

    die "VMID '$vmid' contains illegal characters\n" if $vmid !~ m/^\d+$/;

    return int($vmid);
}

# NOTE: basename and basevmid are always undef for LVM-thin, where the
# clone -> base reference is not encoded in the volume ID.
# see note in PVE::Storage::LvmThinPlugin for details.
sub parse_volname {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    # returns ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format)

    return $plugin->parse_volname($volname);
}

sub parse_volume_id {
    my ($volid, $noerr) = @_;

    return PVE::Storage::Plugin::parse_volume_id($volid, $noerr);
}

# test if we have read access to volid
sub check_volume_access {
    my ($rpcenv, $user, $cfg, $vmid, $volid) = @_;

    my ($sid, $volname) = parse_volume_id($volid, 1);
    if ($sid) {
	my ($vtype, undef, $ownervm) = parse_volname($cfg, $volid);
	if ($vtype eq 'iso' || $vtype eq 'vztmpl') {
	    # we simply allow access
	} elsif (defined($ownervm) && defined($vmid) && ($ownervm == $vmid)) {
	    # we are owner - allow access
	} elsif ($vtype eq 'backup' && $ownervm) {
	    $rpcenv->check($user, "/storage/$sid", ['Datastore.AllocateSpace']);
	    $rpcenv->check($user, "/vms/$ownervm", ['VM.Backup']);
	} else {
	    # allow if we are Datastore administrator
	    $rpcenv->check($user, "/storage/$sid", ['Datastore.Allocate']);
	}
    } else {
	die "Only root can pass arbitrary filesystem paths."
	    if $user ne 'root@pam';
    }

    return undef;
}

my $volume_is_base_and_used__no_lock = sub {
    my ($scfg, $storeid, $plugin, $volname) = @_;

    my ($vtype, $name, $vmid, undef, undef, $isBase, undef) =
	$plugin->parse_volname($volname);

    if ($isBase) {
	my $vollist = $plugin->list_images($storeid, $scfg);
	foreach my $info (@$vollist) {
	    my (undef, $tmpvolname) = parse_volume_id($info->{volid});
	    my $basename = undef;
	    my $basevmid = undef;

	    eval{
		(undef, undef, undef, $basename, $basevmid) =
		    $plugin->parse_volname($tmpvolname);
	    };

	    if ($basename && defined($basevmid) && $basevmid == $vmid && $basename eq $name) {
		return 1;
	    }
	}
    }
    return 0;
};

# NOTE: this check does not work for LVM-thin, where the clone -> base
# reference is not encoded in the volume ID.
# see note in PVE::Storage::LvmThinPlugin for details.
sub volume_is_base_and_used {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	return &$volume_is_base_and_used__no_lock($scfg, $storeid, $plugin, $volname);
    });
}

# try to map a filesystem path to a volume identifier
sub path_to_volume_id {
    my ($cfg, $path) = @_;

    my $ids = $cfg->{ids};

    my ($sid, $volname) = parse_volume_id($path, 1);
    if ($sid) {
	if (my $scfg = $ids->{$sid}) {
	    if ($scfg->{path}) {
		my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
		my ($vtype, $name, $vmid) = $plugin->parse_volname($volname);
		return ($vtype, $path);
	    }
	}
	return ('');
    }

    # Note: abs_path() return undef if $path doesn not exist
    # for example when nfs storage is not mounted
    $path = abs_path($path) || $path;

    foreach my $sid (keys %$ids) {
	my $scfg = $ids->{$sid};
	next if !$scfg->{path};
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	my $imagedir = $plugin->get_subdir($scfg, 'images');
	my $isodir = $plugin->get_subdir($scfg, 'iso');
	my $tmpldir = $plugin->get_subdir($scfg, 'vztmpl');
	my $backupdir = $plugin->get_subdir($scfg, 'backup');
	my $privatedir = $plugin->get_subdir($scfg, 'rootdir');

	if ($path =~ m!^$imagedir/(\d+)/([^/\s]+)$!) {
	    my $vmid = $1;
	    my $name = $2;

	    my $vollist = $plugin->list_images($sid, $scfg, $vmid);
	    foreach my $info (@$vollist) {
		my ($storeid, $volname) = parse_volume_id($info->{volid});
		my $volpath = $plugin->path($scfg, $volname, $storeid);
		if ($volpath eq $path) {
		    return ('images', $info->{volid});
		}
	    }
	} elsif ($path =~ m!^$isodir/([^/]+\.[Ii][Ss][Oo])$!) {
	    my $name = $1;
	    return ('iso', "$sid:iso/$name");
	} elsif ($path =~ m!^$tmpldir/([^/]+\.tar\.gz)$!) {
	    my $name = $1;
	    return ('vztmpl', "$sid:vztmpl/$name");
	} elsif ($path =~ m!^$privatedir/(\d+)$!) {
	    my $vmid = $1;
	    return ('rootdir', "$sid:rootdir/$vmid");
	} elsif ($path =~ m!^$backupdir/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz|vma|vma\.gz|vma\.lzo))$!) {
	    my $name = $1;
	    return ('iso', "$sid:backup/$name");
	}
    }

    # can't map path to volume id
    return ('');
}

sub path {
    my ($cfg, $volid, $snapname) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    my ($path, $owner, $vtype) = $plugin->path($scfg, $volname, $storeid, $snapname);
    return wantarray ? ($path, $owner, $vtype) : $path;
}

sub abs_filesystem_path {
    my ($cfg, $volid) = @_;

    my $path;
    if (PVE::Storage::parse_volume_id ($volid, 1)) {
	PVE::Storage::activate_volumes($cfg, [ $volid ]);
	$path = PVE::Storage::path($cfg, $volid);
    } else {
	if (-f $volid) {
	    my $abspath = abs_path($volid);
	    if ($abspath && $abspath =~ m|^(/.+)$|) {
		$path = $1; # untaint any path
	    }
	}
    }

    die "can't find file '$volid'\n" if !($path && -f $path);

    return $path;
}

sub storage_migrate {
    my ($cfg, $volid, $target_sshinfo, $target_storeid, $target_volname, $base_snapshot, $snapshot, $ratelimit_bps, $insecure, $with_snapshots, $logfunc) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    $target_volname = $volname if !$target_volname;

    my $scfg = storage_config($cfg, $storeid);

    # no need to migrate shared content
    return if $storeid eq $target_storeid && $scfg->{shared};

    my $tcfg = storage_config($cfg, $target_storeid);

    my $target_volid = "${target_storeid}:${target_volname}";

    my $target_ip = $target_sshinfo->{ip};
    my $errstr = "unable to migrate '$volid' to '${target_volid}' on host '$target_sshinfo->{name}'";

    my $ssh = PVE::Cluster::ssh_info_to_command($target_sshinfo);
    my $ssh_base = PVE::Cluster::ssh_info_to_command_base($target_sshinfo);
    local $ENV{RSYNC_RSH} = PVE::Tools::cmd2string($ssh_base);

    my @cstream = ([ '/usr/bin/cstream', '-t', $ratelimit_bps ])
	if defined($ratelimit_bps);

    my $migration_snapshot;
    if (!defined($snapshot)) {
	if ($scfg->{type} eq 'zfspool') {
	    $migration_snapshot = 1;
	    $snapshot = '__migration__';
	}
    }

    my @formats = volume_transfer_formats($cfg, $volid, $volid, $snapshot, $base_snapshot, $with_snapshots);
    die "cannot migrate from storage type '$scfg->{type}' to '$tcfg->{type}'\n" if !@formats;
    my $format = $formats[0];

    my @insecurecmd;
    if ($insecure) {
	@insecurecmd = ('pvecm', 'mtunnel', '-run-command', 1);
	if (my $network = $target_sshinfo->{network}) {
	    push @insecurecmd, '-migration_network', $network;
	}
    }

    $with_snapshots = $with_snapshots ? 1 : 0; # sanitize for passing as cli parameter
    my $send = ['pvesm', 'export', $volid, $format, '-', '-with-snapshots', $with_snapshots];
    my $recv = [@$ssh, @insecurecmd, '--', 'pvesm', 'import', $volid, $format, '-', '-with-snapshots', $with_snapshots];
    if (defined($snapshot)) {
	push @$send, '-snapshot', $snapshot
    }
    if ($migration_snapshot) {
	push @$recv, '-delete-snapshot', $snapshot;
    }

    if (defined($base_snapshot)) {
	# Check if the snapshot exists on the remote side:
	push @$send, '-base', $base_snapshot;
	push @$recv, '-base', $base_snapshot;
    }

    volume_snapshot($cfg, $volid, $snapshot) if $migration_snapshot;
    eval {
	if ($insecure) {
	    open(my $info, '-|', @$recv)
		or die "receive command failed: $!\n";
	    my ($ip) = <$info> =~ /^($PVE::Tools::IPRE)$/ or die "no tunnel IP received\n";
	    my ($port) = <$info> =~ /^(\d+)$/ or die "no tunnel port received\n";
	    my $socket = IO::Socket::IP->new(PeerHost => $ip, PeerPort => $port, Type => SOCK_STREAM)
		or die "failed to connect to tunnel at $ip:$port\n";
	    # we won't be reading from the socket
	    shutdown($socket, 0);
	    run_command([$send, @cstream], output => '>&'.fileno($socket));
	    # don't close the connection entirely otherwise the receiving end
	    # might not get all buffered data (and fails with 'connection reset by peer')
	    shutdown($socket, 1);
	    1 while <$info>; # wait for the remote process to finish
	    # now close the socket
	    close($socket);
	    if (!close($info)) { # does waitpid()
		die "import failed: $!\n" if $!;
		die "import failed: exit code ".($?>>8)."\n";
	    }
	} else {
	    run_command([$send, @cstream, $recv], logfunc => $logfunc);
	}
    };
    my $err = $@;
    warn "send/receive failed, cleaning up snapshot(s)..\n" if $err;
    if ($migration_snapshot) {
	eval { volume_snapshot_delete($cfg, $volid, $snapshot, 0) };
	warn "could not remove source snapshot: $@\n" if $@;
    }
    die $err if $err;
}

sub vdisk_clone {
    my ($cfg, $volid, $vmid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $volname = $plugin->clone_image($scfg, $storeid, $volname, $vmid, $snap);
	return "$storeid:$volname";
    });
}

sub vdisk_create_base {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $volname = $plugin->create_base($storeid, $scfg, $volname);
	return "$storeid:$volname";
    });
}

sub vdisk_alloc {
    my ($cfg, $storeid, $vmid, $fmt, $name, $size) = @_;

    die "no storage ID specified\n" if !$storeid;

    PVE::JSONSchema::parse_storage_id($storeid);

    my $scfg = storage_config($cfg, $storeid);

    die "no VMID specified\n" if !$vmid;

    $vmid = parse_vmid($vmid);

    my $defformat = PVE::Storage::Plugin::default_format($scfg);

    $fmt = $defformat if !$fmt;

    activate_storage($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $old_umask = umask(umask|0037);
	my $volname = eval { $plugin->alloc_image($storeid, $scfg, $vmid, $fmt, $name, $size) };
	my $err = $@;
	umask $old_umask;
	die $err if $err;
	return "$storeid:$volname";
    });
}

sub vdisk_free {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    my $cleanup_worker;

    # lock shared storage
    $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	# LVM-thin allows deletion of still referenced base volumes!
	die "base volume '$volname' is still in use by linked clones\n"
	    if &$volume_is_base_and_used__no_lock($scfg, $storeid, $plugin, $volname);

	my (undef, undef, undef, undef, undef, $isBase, $format) =
	    $plugin->parse_volname($volname);
	$cleanup_worker = $plugin->free_image($storeid, $scfg, $volname, $isBase, $format);
    });

    return if !$cleanup_worker;

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();

    $rpcenv->fork_worker('imgdel', undef, $authuser, $cleanup_worker);
}

#list iso or openvz template ($tt = <iso|vztmpl|backup>)
sub template_list {
    my ($cfg, $storeid, $tt) = @_;

    die "unknown template type '$tt'\n"
	if !($tt eq 'iso' || $tt eq 'vztmpl' || $tt eq 'backup');

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # query the storage

    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;

	my $scfg = $ids->{$sid};
	my $type = $scfg->{type};

	next if !storage_check_enabled($cfg, $sid, undef, 1);

	next if $tt eq 'iso' && !$scfg->{content}->{iso};
	next if $tt eq 'vztmpl' && !$scfg->{content}->{vztmpl};
	next if $tt eq 'backup' && !$scfg->{content}->{backup};

	activate_storage($cfg, $sid);

	if ($scfg->{path}) {
	    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

	    my $path = $plugin->get_subdir($scfg, $tt);

	    foreach my $fn (<$path/*>) {

		my $info;

		if ($tt eq 'iso') {
		    next if $fn !~ m!/([^/]+\.[Ii][Ss][Oo])$!;

		    $info = { volid => "$sid:iso/$1", format => 'iso' };

		} elsif ($tt eq 'vztmpl') {
		    next if $fn !~ m!/([^/]+\.tar\.([gx]z))$!;

		    $info = { volid => "$sid:vztmpl/$1", format => "t$2" };

		} elsif ($tt eq 'backup') {
		    next if $fn !~ m!/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz|vma|vma\.gz|vma\.lzo))$!;

		    $info = { volid => "$sid:backup/$1", format => $2 };
		}

		$info->{size} = -s $fn;

		push @{$res->{$sid}}, $info;
	    }

	}

	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}


sub vdisk_list {
    my ($cfg, $storeid, $vmid, $vollist) = @_;

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # prepare/activate/refresh all storages

    my $storage_list = [];
    if ($vollist) {
	foreach my $volid (@$vollist) {
	    my ($sid, undef) = parse_volume_id($volid);
	    next if !defined($ids->{$sid});
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    push @$storage_list, $sid;
	}
    } else {
	foreach my $sid (keys %$ids) {
	    next if $storeid && $storeid ne $sid;
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    push @$storage_list, $sid;
	}
    }

    my $cache = {};

    activate_storage_list($cfg, $storage_list, $cache);

    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;
	next if !storage_check_enabled($cfg, $sid, undef, 1);

	my $scfg = $ids->{$sid};
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$res->{$sid} = $plugin->list_images($sid, $scfg, $vmid, $vollist, $cache);
	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}

sub volume_list {
    my ($cfg, $storeid, $vmid, $content) = @_;

    my @ctypes = qw(images vztmpl iso backup);

    my $cts = $content ? [ $content ] : [ @ctypes ];

    my $scfg = PVE::Storage::storage_config($cfg, $storeid);

    my $res = [];
    foreach my $ct (@$cts) {
	my $data;
	if ($ct eq 'images') {
	    $data = vdisk_list($cfg, $storeid, $vmid);
	} elsif ($ct eq 'iso' && !defined($vmid)) {
	    $data = template_list($cfg, $storeid, 'iso');
	} elsif ($ct eq 'vztmpl'&& !defined($vmid)) {
	    $data = template_list ($cfg, $storeid, 'vztmpl');
	} elsif ($ct eq 'backup') {
	    $data = template_list ($cfg, $storeid, 'backup');
	    foreach my $item (@{$data->{$storeid}}) {
		if (defined($vmid)) {
		    @{$data->{$storeid}} = grep { $_->{volid} =~ m/\S+-$vmid-\S+/ } @{$data->{$storeid}};
		}
	    }
	}

	next if !$data || !$data->{$storeid};

	foreach my $item (@{$data->{$storeid}}) {
	    $item->{content} = $ct;
	    push @$res, $item;
	}
    }

    return $res;
}

sub uevent_seqnum {

    my $filename = "/sys/kernel/uevent_seqnum";

    my $seqnum = 0;
    if (my $fh = IO::File->new($filename, "r")) {
	my $line = <$fh>;
	if ($line =~ m/^(\d+)$/) {
	    $seqnum = int($1);
	}
	close ($fh);
    }
    return $seqnum;
}

sub activate_storage {
    my ($cfg, $storeid, $cache) = @_;

    $cache = {} if !$cache;

    my $scfg = storage_check_enabled($cfg, $storeid);

    return if $cache->{activated}->{$storeid};

    $cache->{uevent_seqnum} = uevent_seqnum() if !$cache->{uevent_seqnum};

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    if ($scfg->{base}) {
	my ($baseid, undef) = parse_volume_id ($scfg->{base});
	activate_storage($cfg, $baseid, $cache);
    }

    if (!$plugin->check_connection($storeid, $scfg)) {
	die "storage '$storeid' is not online\n";
    }

    $plugin->activate_storage($storeid, $scfg, $cache);

    my $newseq = uevent_seqnum ();

    # only call udevsettle if there are events
    if ($newseq > $cache->{uevent_seqnum}) {
	my $timeout = 30;
	system ("$UDEVADM settle --timeout=$timeout"); # ignore errors
	$cache->{uevent_seqnum} = $newseq;
    }

    $cache->{activated}->{$storeid} = 1;
}

sub activate_storage_list {
    my ($cfg, $storeid_list, $cache) = @_;

    $cache = {} if !$cache;

    foreach my $storeid (@$storeid_list) {
	activate_storage($cfg, $storeid, $cache);
    }
}

sub deactivate_storage {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config ($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $cache = {};
    $plugin->deactivate_storage($storeid, $scfg, $cache);
}

sub activate_volumes {
    my ($cfg, $vollist, $snapname) = @_;

    return if !($vollist && scalar(@$vollist));

    my $storagehash = {};
    foreach my $volid (@$vollist) {
	my ($storeid, undef) = parse_volume_id($volid);
	$storagehash->{$storeid} = 1;
    }

    my $cache = {};

    activate_storage_list($cfg, [keys %$storagehash], $cache);

    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id($volid);
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$plugin->activate_volume($storeid, $scfg, $volname, $snapname, $cache);
    }
}

sub deactivate_volumes {
    my ($cfg, $vollist, $snapname) = @_;

    return if !($vollist && scalar(@$vollist));

    my $cache = {};

    my @errlist = ();
    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id($volid);

	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

	eval {
	    $plugin->deactivate_volume($storeid, $scfg, $volname, $snapname, $cache);
	};
	if (my $err = $@) {
	    warn $err;
	    push @errlist, $volid;
	}
    }

    die "volume deactivation failed: " . join(' ', @errlist)
	if scalar(@errlist);
}

sub storage_info {
    my ($cfg, $content, $includeformat) = @_;

    my $ids = $cfg->{ids};

    my $info = {};

    my @ctypes = PVE::Tools::split_list($content);

    my $slist = [];
    foreach my $storeid (keys %$ids) {
	my $storage_enabled = defined(storage_check_enabled($cfg, $storeid, undef, 1));

	if (defined($content)) {
	    my $want_ctype = 0;
	    foreach my $ctype (@ctypes) {
		if ($ids->{$storeid}->{content}->{$ctype}) {
		    $want_ctype = 1;
		    last;
		}
	    }
	    next if !$want_ctype || !$storage_enabled;
	}

	my $type = $ids->{$storeid}->{type};

	$info->{$storeid} = {
	    type => $type,
	    total => 0,
	    avail => 0,
	    used => 0,
	    shared => $ids->{$storeid}->{shared} ? 1 : 0,
	    content => PVE::Storage::Plugin::content_hash_to_string($ids->{$storeid}->{content}),
	    active => 0,
	    enabled => $storage_enabled ? 1 : 0,
	};

	push @$slist, $storeid;
    }

    my $cache = {};

    foreach my $storeid (keys %$ids) {
	my $scfg = $ids->{$storeid};

	next if !$info->{$storeid};
	next if !$info->{$storeid}->{enabled};

	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	if ($includeformat) {
	    my $pd = $plugin->plugindata();
	    $info->{$storeid}->{format} = $pd->{format}
		if $pd->{format};
	    $info->{$storeid}->{select_existing} = $pd->{select_existing}
		if $pd->{select_existing};
	}

	eval { activate_storage($cfg, $storeid, $cache); };
	if (my $err = $@) {
	    warn $err;
	    next;
	}

	my ($total, $avail, $used, $active) = eval { $plugin->status($storeid, $scfg, $cache); };
	warn $@ if $@;
	next if !$active;
	$info->{$storeid}->{total} = int($total);
	$info->{$storeid}->{avail} = int($avail);
	$info->{$storeid}->{used} = int($used);
	$info->{$storeid}->{active} = $active;
    }

    return $info;
}

sub resolv_server {
    my ($server) = @_;

    my ($packed_ip, $family);
    eval {
	my @res = PVE::Tools::getaddrinfo_all($server);
	$family = $res[0]->{family};
	$packed_ip = (PVE::Tools::unpack_sockaddr_in46($res[0]->{addr}))[2];
    };
    if (defined $packed_ip) {
	return Socket::inet_ntop($family, $packed_ip);
    }
    return undef;
}

sub scan_nfs {
    my ($server_in) = @_;

    my $server;
    if (!($server = resolv_server ($server_in))) {
	die "unable to resolve address for server '${server_in}'\n";
    }

    my $cmd = ['/sbin/showmount',  '--no-headers', '--exports', $server];

    my $res = {};
    run_command($cmd, outfunc => sub {
	my $line = shift;

	# note: howto handle white spaces in export path??
	if ($line =~ m!^(/\S+)\s+(.+)$!) {
	    $res->{$1} = $2;
	}
    });

    return $res;
}

sub scan_cifs {
    my ($server_in, $user, $password, $domain) = @_;

    my $server;
    if (!($server = resolv_server ($server_in))) {
	die "unable to resolve address for server '${server_in}'\n";
    }

    # we support only Windows grater than 2012 cifsscan so use smb3
    my $cmd = ['/usr/bin/smbclient', '-m', 'smb3', '-d', '0', '-L', $server];
    if (defined($user)) {
	die "password is required" if !defined($password);
	push @$cmd, '-U', "$user\%$password";
	push @$cmd, '-W', $domain if defined($domain);
    } else {
	push @$cmd, '-N';
    }

    my $res = {};
    run_command($cmd,
		outfunc => sub {
		    my $line = shift;
		    if ($line =~ m/(\S+)\s*Disk\s*(\S*)/) {
			$res->{$1} = $2;
		    } elsif ($line =~ m/(NT_STATUS_(\S*))/) {
			$res->{$1} = '';
		    }
		},
		errfunc => sub {},
		noerr => 1
    );

    return $res;
}

sub scan_zfs {

    my $cmd = ['zfs',  'list', '-t', 'filesystem', '-H', '-o', 'name,avail,used'];

    my $res = [];
    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~m/^(\S+)\s+(\S+)\s+(\S+)$/) {
	    my ($pool, $size_str, $used_str) = ($1, $2, $3);
	    my $size = PVE::Storage::ZFSPoolPlugin::zfs_parse_size($size_str);
	    my $used = PVE::Storage::ZFSPoolPlugin::zfs_parse_size($used_str);
	    # ignore subvolumes generated by our ZFSPoolPlugin
	    return if $pool =~ m!/subvol-\d+-[^/]+$!;
	    return if $pool =~ m!/basevol-\d+-[^/]+$!;
	    push @$res, { pool => $pool, size => $size, free => $size-$used };
	}
    });

    return $res;
}

sub resolv_portal {
    my ($portal, $noerr) = @_;

    my ($server, $port) = PVE::Tools::parse_host_and_port($portal);
    if ($server) {
	if (my $ip = resolv_server($server)) {
	    $server = $ip;
	    $server = "[$server]" if $server =~ /^$IPV6RE$/;
	    return $port ? "$server:$port" : $server;
	}
    }
    return undef if $noerr;

    raise_param_exc({ portal => "unable to resolve portal address '$portal'" });
}

# idea is from usbutils package (/usr/bin/usb-devices) script
sub __scan_usb_device {
    my ($res, $devpath, $parent, $level) = @_;

    return if ! -d $devpath;
    return if $level && $devpath !~ m/^.*[-.](\d+)$/;
    my $port = $level ? int($1 - 1) : 0;

    my $busnum = int(file_read_firstline("$devpath/busnum"));
    my $devnum = int(file_read_firstline("$devpath/devnum"));

    my $d = {
	port => $port,
	level => $level,
	busnum => $busnum,
	devnum => $devnum,
	speed => file_read_firstline("$devpath/speed"),
	class => hex(file_read_firstline("$devpath/bDeviceClass")),
	vendid => file_read_firstline("$devpath/idVendor"),
	prodid => file_read_firstline("$devpath/idProduct"),
    };

    if ($level) {
	my $usbpath = $devpath;
	$usbpath =~ s|^.*/\d+\-||;
	$d->{usbpath} = $usbpath;
    }

    my $product = file_read_firstline("$devpath/product");
    $d->{product} = $product if $product;

    my $manu = file_read_firstline("$devpath/manufacturer");
    $d->{manufacturer} = $manu if $manu;

    my $serial => file_read_firstline("$devpath/serial");
    $d->{serial} = $serial if $serial;

    push @$res, $d;

    foreach my $subdev (<$devpath/$busnum-*>) {
	next if $subdev !~ m|/$busnum-[0-9]+(\.[0-9]+)*$|;
	__scan_usb_device($res, $subdev, $devnum, $level + 1);
    }

};

sub scan_usb {

    my $devlist = [];

    foreach my $device (</sys/bus/usb/devices/usb*>) {
	__scan_usb_device($devlist, $device, 0, 0);
    }

    return $devlist;
}

sub scan_iscsi {
    my ($portal_in) = @_;

    my $portal;
    if (!($portal = resolv_portal($portal_in))) {
	die "unable to parse/resolve portal address '${portal_in}'\n";
    }

    return PVE::Storage::ISCSIPlugin::iscsi_discovery($portal);
}

sub storage_default_format {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config ($cfg, $storeid);

    return PVE::Storage::Plugin::default_format($scfg);
}

sub vgroup_is_used {
    my ($cfg, $vgname) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{type} eq 'lvm' && $scfg->{vgname} eq $vgname) {
	    return 1;
	}
    }

    return undef;
}

sub target_is_used {
    my ($cfg, $target) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{type} eq 'iscsi' && $scfg->{target} eq $target) {
	    return 1;
	}
    }

    return undef;
}

sub volume_is_used {
    my ($cfg, $volid) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{base} && $scfg->{base} eq $volid) {
	    return 1;
	}
    }

    return undef;
}

sub storage_is_used {
    my ($cfg, $storeid) = @_;

    foreach my $sid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $sid);
	next if !$scfg->{base};
	my ($st) = parse_volume_id($scfg->{base});
	return 1 if $st && $st eq $storeid;
    }

    return undef;
}

sub foreach_volid {
    my ($list, $func) = @_;

    return if !$list;

    foreach my $sid (keys %$list) {
       foreach my $info (@{$list->{$sid}}) {
           my $volid = $info->{volid};
	   my ($sid1, $volname) = parse_volume_id($volid, 1);
	   if ($sid1 && $sid1 eq $sid) {
	       &$func ($volid, $sid, $info);
	   } else {
	       warn "detected strange volid '$volid' in volume list for '$sid'\n";
	   }
       }
    }
}

sub extract_vzdump_config_tar {
    my ($archive, $conf_re) = @_;

    die "ERROR: file '$archive' does not exist\n" if ! -f $archive;

    my $pid = open(my $fh, '-|', 'tar', 'tf', $archive) ||
       die "unable to open file '$archive'\n";

    my $file;
    while (defined($file = <$fh>)) {
	if ($file =~ $conf_re) {
	    $file = $1; # untaint
	    last;
	}
    }

    kill 15, $pid;
    waitpid $pid, 0;
    close $fh;

    die "ERROR: archive contains no configuration file\n" if !$file;
    chomp $file;

    my $raw = '';
    my $out = sub {
	my $output = shift;
	$raw .= "$output\n";
    };

    PVE::Tools::run_command(['tar', '-xpOf', $archive, $file, '--occurrence'], outfunc => $out);

    return wantarray ? ($raw, $file) : $raw;
}

sub extract_vzdump_config_vma {
    my ($archive, $comp) = @_;

    my $cmd;
    my $raw = '';
    my $out = sub {
	my $output = shift;
	$raw .= "$output\n";
    };


    if ($comp) {
	my $uncomp;
	if ($comp eq 'gz') {
	    $uncomp = ["zcat", $archive];
	} elsif ($comp eq 'lzo') {
	    $uncomp = ["lzop", "-d", "-c", $archive];
	} else {
	    die "unknown compression method '$comp'\n";
	}
	$cmd = [$uncomp, ["vma", "config", "-"]];

	# in some cases, lzop/zcat exits with 1 when its stdout pipe is
	# closed early by vma, detect this and ignore the exit code later
	my $broken_pipe;
	my $errstring;
	my $err = sub {
	    my $output = shift;
	    if ($output =~ m/lzop: Broken pipe: <stdout>/ || $output =~ m/gzip: stdout: Broken pipe/) {
		$broken_pipe = 1;
	    } elsif (!defined ($errstring) && $output !~ m/^\s*$/) {
		$errstring = "Failed to extract config from VMA archive: $output\n";
	    }
	};

	# in other cases, the pipeline will exit with exit code 141
	# because of the broken pipe, handle / ignore this as well
	my $rc;
	eval {
	    $rc = PVE::Tools::run_command($cmd, outfunc => $out, errfunc => $err, noerr => 1);
	};
	my $rerr = $@;

	# use exit code if no stderr output and not just broken pipe
	if (!$errstring && !$broken_pipe && $rc != 0 && $rc != 141) {
	    die "$rerr\n" if $rerr;
	    die "config extraction failed with exit code $rc\n";
	}
	die "$errstring\n" if $errstring;
    } else {
	# simple case without compression and weird piping behaviour
	PVE::Tools::run_command(["vma", "config", $archive], outfunc => $out);
    }

    return wantarray ? ($raw, undef) : $raw;
}

sub extract_vzdump_config {
    my ($cfg, $volid) = @_;

    my $archive = abs_filesystem_path($cfg, $volid);

    if ($volid =~ /vzdump-(lxc|openvz)-\d+-(\d{4})_(\d{2})_(\d{2})-(\d{2})_(\d{2})_(\d{2})\.(tgz|(tar(\.(gz|lzo))?))$/) {
	return extract_vzdump_config_tar($archive, qr!^(\./etc/vzdump/(pct|vps)\.conf)$!);
    } elsif ($volid =~ /vzdump-qemu-\d+-(\d{4})_(\d{2})_(\d{2})-(\d{2})_(\d{2})_(\d{2})\.(tgz|((tar|vma)(\.(gz|lzo))?))$/) {
	my $format;
	my $comp;
	if ($7 eq 'tgz') {
	    $format = 'tar';
	    $comp = 'gz';
	} else {
	    $format = $9;
	    $comp = $11 if defined($11);
	}

	if ($format eq 'tar') {
	    return extract_vzdump_config_tar($archive, qr!\(\./qemu-server\.conf\)!);
	} else {
	    return extract_vzdump_config_vma($archive, $comp);
	}
    } else {
	die "cannot determine backup guest type for backup archive '$volid'\n";
    }
}

sub volume_export {
    my ($cfg, $fh, $volid, $format, $snapshot, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    die "cannot export volume '$volid'\n" if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_export($scfg, $storeid, $fh, $volname, $format,
                                  $snapshot, $base_snapshot, $with_snapshots);
}

sub volume_import {
    my ($cfg, $fh, $volid, $format, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    die "cannot import into volume '$volid'\n" if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_import($scfg, $storeid, $fh, $volname, $format,
                                  $base_snapshot, $with_snapshots);
}

sub volume_export_formats {
    my ($cfg, $volid, $snapshot, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    return if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_export_formats($scfg, $storeid, $volname,
                                          $snapshot, $base_snapshot,
                                          $with_snapshots);
}

sub volume_import_formats {
    my ($cfg, $volid, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    return if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_import_formats($scfg, $storeid, $volname,
                                          $base_snapshot, $with_snapshots);
}

sub volume_transfer_formats {
    my ($cfg, $src_volid, $dst_volid, $snapshot, $base_snapshot, $with_snapshots) = @_;
    my @export_formats = volume_export_formats($cfg, $src_volid, $snapshot, $base_snapshot, $with_snapshots);
    my @import_formats = volume_import_formats($cfg, $dst_volid, $base_snapshot, $with_snapshots);
    my %import_hash = map { $_ => 1 } @import_formats;
    my @common = grep { $import_hash{$_} } @export_formats;
    return @common;
}

# bash completion helper

sub complete_storage {
    my ($cmdname, $pname, $cvalue) = @_;

    my $cfg = PVE::Storage::config();

    return  $cmdname eq 'add' ? [] : [ PVE::Storage::storage_ids($cfg) ];
}

sub complete_storage_enabled {
    my ($cmdname, $pname, $cvalue) = @_;

    my $res = [];

    my $cfg = PVE::Storage::config();
    foreach my $sid (keys %{$cfg->{ids}}) {
	next if !storage_check_enabled($cfg, $sid, undef, 1);
	push @$res, $sid;
    }
    return $res;
}

sub complete_content_type {
    my ($cmdname, $pname, $cvalue) = @_;

    return [qw(rootdir images vztmpl iso backup)];
}

sub complete_volume {
    my ($cmdname, $pname, $cvalue) = @_;

    my $cfg = config();

    my $storage_list = complete_storage_enabled();

    if ($cvalue =~ m/^([^:]+):/) {
	$storage_list = [ $1 ];
    } else {
	if (scalar(@$storage_list) > 1) {
	    # only list storage IDs to avoid large listings
	    my $res = [];
	    foreach my $storeid (@$storage_list) {
		# Hack: simply return 2 artificial values, so that
		# completions does not finish
		push @$res, "$storeid:volname", "$storeid:...";
	    }
	    return $res;
	}
    }

    my $res = [];
    foreach my $storeid (@$storage_list) {
	my $vollist = PVE::Storage::volume_list($cfg, $storeid);

	foreach my $item (@$vollist) {
	    push @$res, $item->{volid};
	}
    }

    return $res;
}

# Various io-heavy operations require io/bandwidth limits which can be
# configured on multiple levels: The global defaults in datacenter.cfg, and
# per-storage overrides. When we want to do a restore from storage A to storage
# B, we should take the smaller limit defined for storages A and B, and if no
# such limit was specified, use the one from datacenter.cfg.
sub get_bandwidth_limit {
    my ($operation, $storage_list, $override) = @_;

    # called for each limit (global, per-storage) with the 'default' and the
    # $operation limit and should udpate $override for every limit affecting
    # us.
    my $use_global_limits = 0;
    my $apply_limit = sub {
	my ($bwlimit) = @_;
	if (defined($bwlimit)) {
	    my $limits = PVE::JSONSchema::parse_property_string('bwlimit', $bwlimit);
	    my $limit = $limits->{$operation} // $limits->{default};
	    if (defined($limit)) {
		if (!$override || $limit < $override) {
		    $override = $limit;
		}
		return;
	    }
	}
	# If there was no applicable limit, try to apply the global ones.
	$use_global_limits = 1;
    };

    my ($rpcenv, $authuser);
    if (defined($override)) {
	$rpcenv = PVE::RPCEnvironment->get();
	$authuser = $rpcenv->get_user();
    }

    # Apply per-storage limits - if there are storages involved.
    if (@$storage_list) {
	my $config = config();

	# The Datastore.Allocate permission allows us to modify the per-storage
	# limits, therefore it also allows us to override them.
	# Since we have most likely multiple storages to check, do a quick check on
	# the general '/storage' path to see if we can skip the checks entirely:
	return $override if $rpcenv && $rpcenv->check($authuser, '/storage', ['Datastore.Allocate'], 1);

	my %done;
	foreach my $storage (@$storage_list) {
	    # Avoid duplicate checks:
	    next if $done{$storage};
	    $done{$storage} = 1;

	    # Otherwise we may still have individual /storage/$ID permissions:
	    if (!$rpcenv || !$rpcenv->check($authuser, "/storage/$storage", ['Datastore.Allocate'], 1)) {
		# And if not: apply the limits.
		my $storecfg = storage_config($config, $storage);
		$apply_limit->($storecfg->{bwlimit});
	    }
	}

	# Storage limits take precedence over the datacenter defaults, so if
	# a limit was applied:
	return $override if !$use_global_limits;
    }

    # Sys.Modify on '/' means we can change datacenter.cfg which contains the
    # global default limits.
    if (!$rpcenv || !$rpcenv->check($authuser, '/', ['Sys.Modify'], 1)) {
	# So if we cannot modify global limits, apply them to our currently
	# requested override.
	my $dc = cfs_read_file('datacenter.cfg');
	$apply_limit->($dc->{bwlimit});
    }

    return $override;
}

1;
