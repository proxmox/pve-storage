package PVE::Storage::LVMPlugin;

use strict;
use warnings;

use File::Basename;
use IO::File;

use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use PVE::Storage::Common;

use JSON;

use base qw(PVE::Storage::Plugin);

# lvm helper functions

my $ignore_no_medium_warnings = sub {
    my $line = shift;
    # ignore those, most of the time they're from (virtual) IPMI/iKVM devices
    # and just spam the log..
    if ($line !~ /open failed: No medium found/) {
        print STDERR "$line\n";
    }
};

my sub fork_cleanup_worker {
    my ($cleanup_worker) = @_;

    return if !$cleanup_worker;
    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();
    $rpcenv->fork_worker('imgdel', undef, $authuser, $cleanup_worker);
}

sub lvm_pv_info {
    my ($device) = @_;

    die "no device specified" if !$device;

    my $has_label = 0;

    my $cmd = ['/usr/bin/file', '-L', '-s', $device];
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;
            $has_label = 1 if $line =~ m/LVM2/;
        },
    );

    return undef if !$has_label;

    $cmd = [
        '/sbin/pvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'k',
        '--unbuffered',
        '--nosuffix',
        '--options',
        'pv_name,pv_size,vg_name,pv_uuid',
        $device,
    ];

    my $pvinfo;
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;

            $line = trim($line);

            my ($pvname, $size, $vgname, $uuid) = split(':', $line);

            die "found multiple pvs entries for device '$device'\n"
                if $pvinfo;

            $pvinfo = {
                pvname => $pvname,
                size => int($size),
                vgname => $vgname,
                uuid => $uuid,
            };
        },
    );

    return $pvinfo;
}

sub clear_first_sector {
    my ($dev) = shift;

    if (my $fh = IO::File->new($dev, "w")) {
        my $buf = 0 x 512;
        syswrite $fh, $buf;
        $fh->close();
    }
}

sub lvm_create_volume_group {
    my ($device, $vgname, $shared) = @_;

    my $res = lvm_pv_info($device);

    if ($res->{vgname}) {
        return if $res->{vgname} eq $vgname; # already created
        die "device '$device' is already used by volume group '$res->{vgname}'\n";
    }

    clear_first_sector($device); # else pvcreate fails

    # we use --metadatasize 250k, which reseults in "pe_start = 512"
    # so pe_start is aligned on a 128k boundary (advantage for SSDs)
    my $cmd = ['/sbin/pvcreate', '--metadatasize', '250k', $device];

    run_command($cmd, errmsg => "pvcreate '$device' error");

    $cmd = ['/sbin/vgcreate', $vgname, $device];
    # push @$cmd, '-c', 'y' if $shared; # we do not use this yet

    run_command(
        $cmd,
        errmsg => "vgcreate $vgname $device error",
        errfunc => $ignore_no_medium_warnings,
        outfunc => $ignore_no_medium_warnings,
    );
}

sub lvm_destroy_volume_group {
    my ($vgname) = @_;

    run_command(
        ['vgremove', '-y', $vgname],
        errmsg => "unable to remove volume group $vgname",
        errfunc => $ignore_no_medium_warnings,
        outfunc => $ignore_no_medium_warnings,
    );
}

sub lvm_vgs {
    my ($includepvs) = @_;

    my $cmd = [
        '/sbin/vgs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--options',
    ];

    my $cols = [qw(vg_name vg_size vg_free lv_count)];

    if ($includepvs) {
        push @$cols, qw(pv_name pv_size pv_free);
    }

    push @$cmd, join(',', @$cols);

    my $vgs = {};
    eval {
        run_command(
            $cmd,
            outfunc => sub {
                my $line = shift;
                $line = trim($line);

                my ($name, $size, $free, $lvcount, $pvname, $pvsize, $pvfree) =
                    split(':', $line);

                $vgs->{$name} //= {
                    size => int($size),
                    free => int($free),
                    lvcount => int($lvcount),
                };

                if (defined($pvname) && defined($pvsize) && defined($pvfree)) {
                    push @{ $vgs->{$name}->{pvs} },
                        {
                            name => $pvname,
                            size => int($pvsize),
                            free => int($pvfree),
                        };
                }
            },
            errfunc => $ignore_no_medium_warnings,
        );
    };
    my $err = $@;

    # just warn (vgs return error code 5 if clvmd does not run)
    # but output is still OK (list without clustered VGs)
    warn $err if $err;

    return $vgs;
}

sub lvm_list_volumes {
    my ($vgname) = @_;

    my $option_list =
        'vg_name,lv_name,lv_size,lv_attr,pool_lv,data_percent,metadata_percent,snap_percent,uuid,tags,metadata_size,time';

    my $cmd = [
        '/sbin/lvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--config',
        'report/time_format="%s"',
        '--options',
        $option_list,
    ];

    push @$cmd, $vgname if $vgname;

    my $lvs = {};
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;

            $line = trim($line);

            my (
                $vg_name,
                $lv_name,
                $lv_size,
                $lv_attr,
                $pool_lv,
                $data_percent,
                $meta_percent,
                $snap_percent,
                $uuid,
                $tags,
                $meta_size,
                $ctime,
            ) = split(':', $line);
            return if !$vg_name;
            return if !$lv_name;

            my $lv_type = substr($lv_attr, 0, 1);

            my $d = {
                lv_size => int($lv_size),
                lv_state => substr($lv_attr, 4, 1),
                lv_type => $lv_type,
            };
            $d->{pool_lv} = $pool_lv if $pool_lv;
            $d->{tags} = $tags if $tags;
            $d->{ctime} = $ctime;

            if ($lv_type eq 't') {
                $data_percent ||= 0;
                $meta_percent ||= 0;
                $snap_percent ||= 0;
                $d->{metadata_size} = int($meta_size);
                $d->{metadata_used} = int(($meta_percent * $meta_size) / 100);
                $d->{used} = int(($data_percent * $lv_size) / 100);
            }
            $lvs->{$vg_name}->{$lv_name} = $d;
        },
        errfunc => $ignore_no_medium_warnings,
    );

    return $lvs;
}

my sub free_lvm_volumes {
    my ($class, $scfg, $storeid, $volnames) = @_;

    my $vg = $scfg->{vgname};

    # we need to zero out LVM data for security reasons
    # and to allow thin provisioning
    my $zero_out_worker = sub {
        # wipe throughput up to 10MB/s by default; may be overwritten with saferemove_throughput
        my $throughput = '-10485760';
        if ($scfg->{saferemove_throughput}) {
            $throughput = $scfg->{saferemove_throughput};
        }
        for my $name (@$volnames) {
            print "zero-out data on image $name (/dev/$vg/del-$name)\n";

            my $cmd = [
                '/usr/bin/cstream',
                '-i',
                '/dev/zero',
                '-o',
                "/dev/$vg/del-$name",
                '-T',
                '10',
                '-v',
                '1',
                '-b',
                '1048576',
                '-t',
                "$throughput",
            ];
            eval {
                run_command(
                    $cmd,
                    errmsg => "zero out finished (note: 'No space left on device' is ok here)",
                );
            };
            warn $@ if $@;

            $class->cluster_lock_storage(
                $storeid,
                $scfg->{shared},
                undef,
                sub {
                    my $cmd = ['/sbin/lvremove', '-f', "$vg/del-$name"];
                    run_command($cmd, errmsg => "lvremove '$vg/del-$name' error");
                },
            );
            print "successfully removed volume $name ($vg/del-$name)\n";
        }
    };

    if ($scfg->{saferemove}) {
        for my $name (@$volnames) {
            # avoid long running task, so we only rename here
            my $cmd = ['/sbin/lvrename', $vg, $name, "del-$name"];
            run_command($cmd, errmsg => "lvrename '$vg/$name' error");
        }
        return $zero_out_worker;
    } else {
        for my $name (@$volnames) {
            my $cmd = ['/sbin/lvremove', '-f', "$vg/$name"];
            run_command($cmd, errmsg => "lvremove '$vg/$name' error");
        }
    }
}

# Configuration

sub type {
    return 'lvm';
}

sub plugindata {
    return {
        content => [{ images => 1, rootdir => 1 }, { images => 1 }],
        format => [{ raw => 1, qcow2 => 1 }, 'raw'],
        'sensitive-properties' => {},
    };
}

sub properties {
    return {
        vgname => {
            description => "Volume group name.",
            type => 'string',
            format => 'pve-storage-vgname',
        },
        base => {
            description => "Base volume. This volume is automatically activated.",
            type => 'string',
            format => 'pve-volume-id',
        },
        saferemove => {
            description => "Zero-out data when removing LVs.",
            type => 'boolean',
        },
        saferemove_throughput => {
            description => "Wipe throughput (cstream -t parameter value).",
            type => 'string',
        },
        tagged_only => {
            description => "Only use logical volumes tagged with 'pve-vm-ID'.",
            type => 'boolean',
        },
    };
}

sub options {
    return {
        vgname => { fixed => 1 },
        nodes => { optional => 1 },
        shared => { optional => 1 },
        disable => { optional => 1 },
        saferemove => { optional => 1 },
        saferemove_throughput => { optional => 1 },
        content => { optional => 1 },
        base => { fixed => 1, optional => 1 },
        tagged_only => { optional => 1 },
        bwlimit => { optional => 1 },
    };
}

# Storage implementation

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (my $base = $scfg->{base}) {
        my ($baseid, $volname) = PVE::Storage::parse_volume_id($base);

        my $cfg = PVE::Storage::config();
        my $basecfg = PVE::Storage::storage_config($cfg, $baseid, 1);
        die "base storage ID '$baseid' does not exist\n" if !$basecfg;

        # we only support iscsi for now
        die "unsupported base type '$basecfg->{type}'"
            if $basecfg->{type} ne 'iscsi';

        my $path = PVE::Storage::path($cfg, $base);

        PVE::Storage::activate_storage($cfg, $baseid);

        lvm_create_volume_group($path, $scfg->{vgname}, $scfg->{shared});
    }

    return;
}

sub parse_volname {
    my ($class, $volname) = @_;

    PVE::Storage::Plugin::parse_lvm_name($volname);

    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
        my $name = $1;
        my $vmid = $2;
        my $format = $volname =~ m/\.qcow2$/ ? 'qcow2' : 'raw';
        return ('images', $name, $vmid, undef, undef, undef, $format);
    }

    die "unable to parse lvm volume name '$volname'\n";
}

my sub get_snap_name {
    my ($class, $volname, $snapname) = @_;

    die "missing snapname\n" if !$snapname;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);
    if ($snapname eq 'current') {
        return $name;
    } else {
        $name =~ s/\.[^.]+$//;
        return "snap_${name}_${snapname}.qcow2";
    }
}

my sub parse_snap_name {
    my ($name) = @_;

    if ($name =~ m/^snap_\S+_(.*)\.qcow2$/) {
        return $1;
    }
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    die "snapshot is working with qcow2 format only" if defined($snapname) && $format ne 'qcow2';

    my $vg = $scfg->{vgname};
    $name = get_snap_name($class, $volname, $snapname) if $snapname;

    my $path = "/dev/$vg/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub qemu_blockdev_options {
    my ($class, $scfg, $storeid, $volname, $machine_version, $options) = @_;

    my ($path) = $class->path($scfg, $volname, $storeid, $options->{'snapshot-name'});

    my $blockdev = { driver => 'host_device', filename => $path };

    return $blockdev;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in lvm storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in lvm storage\n";
}

sub find_free_diskname {
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;

    my $vg = $scfg->{vgname};

    my $lvs = lvm_list_volumes($vg);

    my $disk_list = [keys %{ $lvs->{$vg} }];

    $add_fmt_suffix = $fmt eq 'qcow2' ? 1 : undef;

    return PVE::Storage::Plugin::get_next_vm_diskname(
        $disk_list, $storeid, $vmid, $fmt, $scfg, $add_fmt_suffix,
    );
}

sub lvcreate {
    my ($vg, $name, $size, $tags) = @_;

    if ($size =~ m/\d$/) { # no unit is given
        $size .= "k"; # default to kilobytes
    }

    my $cmd = [
        '/sbin/lvcreate',
        '-aly',
        '-Wy',
        '--yes',
        '--size',
        $size,
        '--name',
        $name,
        '--setautoactivation',
        'n',
    ];
    for my $tag (@$tags) {
        push @$cmd, '--addtag', $tag;
    }
    push @$cmd, $vg;

    run_command($cmd, errmsg => "lvcreate '$vg/$name' error");
}

sub lvrename {
    my ($scfg, $oldname, $newname) = @_;

    my $vg = $scfg->{vgname};
    my $lvs = lvm_list_volumes($vg);
    die "target volume '${newname}' already exists\n"
        if ($lvs->{$vg}->{$newname});

    run_command(
        ['/sbin/lvrename', $vg, $oldname, $newname],
        errmsg => "lvrename '${vg}/${oldname}' to '${newname}' error",
    );
}

my sub lvm_qcow2_format {
    my ($class, $storeid, $scfg, $name, $fmt, $backing_snap, $size) = @_;

    $class->activate_volume($storeid, $scfg, $name);
    my $path = $class->path($scfg, $name, $storeid);

    my $options = {
        preallocation => PVE::Storage::Plugin::preallocation_cmd_opt($scfg, $fmt),
    };
    if ($backing_snap) {
        my $backing_volname = get_snap_name($class, $name, $backing_snap);
        PVE::Storage::Common::qemu_img_create_qcow2_backed($path, $backing_volname, $fmt, $options);
    } else {
        PVE::Storage::Common::qemu_img_create($fmt, $size, $path, $options);
    }
}

my sub calculate_lvm_size {
    my ($size, $fmt, $backing_snap) = @_;
    #input size = qcow2 image size in kb

    return $size if $fmt ne 'qcow2';

    my $options = $backing_snap ? ['extended_l2=on', 'cluster_size=128k'] : [];

    my $json = PVE::Storage::Common::qemu_img_measure($size, $fmt, 5, $options);
    die "failed to query file information with qemu-img measure\n" if !$json;
    my $info = eval { decode_json($json) };
    if ($@) {
        die "Invalid JSON: $@\n";
    }

    die "Missing fully-allocated value from json" if !$info->{'fully-allocated'};

    return $info->{'fully-allocated'} / 1024;
}

my sub alloc_lvm_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size, $backing_snap) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw' && $fmt ne 'qcow2';

    $class->parse_volname($name);

    my $vgs = lvm_vgs();

    my $vg = $scfg->{vgname};

    die "no such volume group '$vg'\n" if !defined($vgs->{$vg});

    my $free = int($vgs->{$vg}->{free});
    my $lvmsize = calculate_lvm_size($size, $fmt, $backing_snap);

    die "not enough free space ($free < $size)\n" if $free < $size;

    my $tags = ["pve-vm-$vmid"];
    #tags all snapshots volumes with the main volume tag for easier activation of the whole group
    push @$tags, "\@pve-$name" if $fmt eq 'qcow2';
    lvcreate($vg, $name, $lvmsize, $tags);

    return if $fmt ne 'qcow2';

    #format the lvm volume with qcow2 format
    eval { lvm_qcow2_format($class, $storeid, $scfg, $name, $fmt, $backing_snap, $size) };
    if ($@) {
        my $err = $@;
        #no need to safe cleanup as the volume is still empty
        eval {
            my $cmd = ['/sbin/lvremove', '-f', "$vg/$name"];
            run_command($cmd, errmsg => "lvremove '$vg/$name' error");
        };
        die $err;
    }

}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    $name = $class->find_free_diskname($storeid, $scfg, $vmid, $fmt)
        if !$name;

    alloc_lvm_image($class, $storeid, $scfg, $vmid, $fmt, $name, $size);

    return $name;
}

my sub alloc_snap_image {
    my ($class, $storeid, $scfg, $volname, $backing_snap) = @_;

    my ($vmid, $format) = ($class->parse_volname($volname))[2, 6];
    my $path = $class->path($scfg, $volname, $storeid, $backing_snap);

    #we need to use same size than the backing image qcow2 virtual-size
    my $size = PVE::Storage::Plugin::file_size_info($path, 5, $format);
    die "file_size_info on '$volname' failed\n" if !defined($size);

    $size = $size / 1024; #we use kb in lvcreate

    alloc_lvm_image($class, $storeid, $scfg, $vmid, $format, $volname, $size, $backing_snap);
}

my sub free_snap_image {
    my ($class, $storeid, $scfg, $volname, $snap) = @_;

    #activate only the snapshot volume
    my $path = $class->path($scfg, $volname, $storeid, $snap);
    my $cmd = ['/sbin/lvchange', '-aly', $path];
    run_command($cmd, errmsg => "can't activate LV '$path' to zero-out its data");
    $cmd = ['/sbin/lvchange', '--refresh', $path];
    run_command($cmd, errmsg => "can't refresh LV '$path' to zero-out its data");

    my $snap_volname = get_snap_name($class, $volname, $snap);
    return free_lvm_volumes($class, $scfg, $storeid, [$snap_volname]);
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    my $name = ($class->parse_volname($volname))[1];

    my $volnames = [$volname];

    if ($format eq 'qcow2') {
        #activate volumes && snapshot volumes
        my $path = $class->path($scfg, $volname, $storeid);
        $path = "\@pve-$name" if $format && $format eq 'qcow2';
        my $cmd = ['/sbin/lvchange', '-aly', $path];
        run_command($cmd, errmsg => "can't activate LV '$path' to zero-out its data");
        $cmd = ['/sbin/lvchange', '--refresh', $path];
        run_command($cmd, errmsg => "can't refresh LV '$path' to zero-out its data");

        my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
        for my $snapid (
            sort { $snapshots->{$a}->{order} <=> $snapshots->{$b}->{order} }
            keys %$snapshots
        ) {
            my $snap = $snapshots->{$snapid};
            next if $snapid eq 'current';
            next if !$snap->{volid};
            my ($snap_storeid, $snap_volname) = PVE::Storage::parse_volume_id($snap->{volid});
            push @$volnames, $snap_volname;
        }
    }

    return free_lvm_volumes($class, $scfg, $storeid, $volnames);
}

my $check_tags = sub {
    my ($tags) = @_;

    return defined($tags) && $tags =~ /(^|,)pve-vm-\d+(,|$)/;
};

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{lvs} = lvm_list_volumes() if !$cache->{lvs};

    my $res = [];

    if (my $dat = $cache->{lvs}->{$vgname}) {

        foreach my $volname (keys %$dat) {

            next if $volname !~ m/^vm-(\d+)-/;
            my $owner = $1;

            my $info = $dat->{$volname};

            next if $scfg->{tagged_only} && !&$check_tags($info->{tags});

            # Allow mirrored and RAID LVs
            next if $info->{lv_type} !~ m/^[-mMrR]$/;

            my $volid = "$storeid:$volname";

            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                next if defined($vmid) && ($owner ne $vmid);
            }

            push @$res,
                {
                    volid => $volid,
                    format => 'raw',
                    size => $info->{lv_size},
                    vmid => $owner,
                    ctime => $info->{ctime},
                };
        }
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    my $vgname = $scfg->{vgname};

    if (my $info = $cache->{vgs}->{$vgname}) {
        return ($info->{size}, $info->{free}, $info->{size} - $info->{free}, 1);
    }

    return undef;
}

sub volume_snapshot_info {
    my ($class, $scfg, $storeid, $volname) = @_;

    my $get_snapname_from_path = sub {
        my ($volname, $path) = @_;

        my $name = basename($path);
        if (my $snapname = parse_snap_name($name)) {
            return $snapname;
        } elsif ($name eq $volname) {
            return 'current';
        }
        return undef;
    };

    my $path = $class->filesystem_path($scfg, $volname);
    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    my $json = PVE::Storage::Common::qemu_img_info($path, undef, 10, 1);
    die "failed to query file information with qemu-img\n" if !$json;
    my $json_decode = eval { decode_json($json) };
    if ($@) {
        die "Can't decode qemu snapshot list. Invalid JSON: $@\n";
    }
    my $info = {};
    my $order = 0;
    return $info if ref($json_decode) ne 'ARRAY';

    #no snapshot or external  snapshots is an arrayref
    my $snapshots = $json_decode;
    for my $snap (@$snapshots) {
        my $snapfile = $snap->{filename};
        my $snapname = $get_snapname_from_path->($volname, $snapfile);
        #not a proxmox snapshot
        next if !$snapname;

        my $snapvolname = get_snap_name($class, $volname, $snapname);

        $info->{$snapname}->{order} = $order;
        $info->{$snapname}->{file} = $snapfile;
        $info->{$snapname}->{volname} = "$snapvolname";
        $info->{$snapname}->{volid} = "$storeid:$snapvolname";

        my $parentfile = $snap->{'backing-filename'};
        if ($parentfile) {
            my $parentname = $get_snapname_from_path->($volname, $parentfile);
            $info->{$snapname}->{parent} = $parentname;
            $info->{$parentname}->{child} = $snapname;
        }
        $order++;
    }
    return $info;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    # In LVM2, vgscans take place automatically;
    # this is just to be sure
    if (
        $cache->{vgs}
        && !$cache->{vgscaned}
        && !$cache->{vgs}->{ $scfg->{vgname} }
    ) {
        $cache->{vgscaned} = 1;
        my $cmd = ['/sbin/vgscan', '--ignorelockingfailure', '--mknodes'];
        eval {
            run_command($cmd, outfunc => sub { });
        };
        warn $@ if $@;
    }

    # we do not acticate any volumes here ('vgchange -aly')
    # instead, volumes are activate individually later
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $cmd = ['/sbin/vgchange', '-aln', $scfg->{vgname}];
    run_command($cmd, errmsg => "can't deactivate VG '$scfg->{vgname}'");
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
    #fix me lvmchange is not provided on
    my $path = $class->path($scfg, $volname, $storeid, $snapname);

    my $lvm_activate_mode = 'ey';

    #activate volume && all snapshots volumes by tag
    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    $path = "\@pve-$name" if $format eq 'qcow2';

    my $cmd = ['/sbin/lvchange', "-a$lvm_activate_mode", $path];
    run_command($cmd, errmsg => "can't activate LV '$path'");
    $cmd = ['/sbin/lvchange', '--refresh', $path];
    run_command($cmd, errmsg => "can't refresh LV '$path' for activation");
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $path = $class->path($scfg, $volname, $storeid, $snapname);
    return if !-b $path;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);
    $path = "\@pve-$name" if $format eq 'qcow2';

    my $cmd = ['/sbin/lvchange', '-aln', $path];
    run_command($cmd, errmsg => "can't deactivate LV '$path'");
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    my $lvmsize = calculate_lvm_size($size / 1024, $format);
    $lvmsize = "${lvmsize}k";

    my $path = $class->path($scfg, $volname);
    my $cmd = ['/sbin/lvextend', '-L', $lvmsize, $path];

    $class->cluster_lock_storage(
        $storeid,
        $scfg->{shared},
        undef,
        sub {
            run_command($cmd, errmsg => "error resizing volume '$path'");
        },
    );

    if (!$running && $format eq 'qcow2') {
        my $preallocation = PVE::Storage::Plugin::preallocation_cmd_opt($scfg, $format);
        PVE::Storage::Common::qemu_img_resize($path, $format, $size, $preallocation, 10);
    }

    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my $path = $class->filesystem_path($scfg, $volname);

    my $cmd = [
        '/sbin/lvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--options',
        'lv_size',
        $path,
    ];

    my $size;
    run_command(
        $cmd,
        timeout => $timeout,
        errmsg => "can't get size of '$path'",
        outfunc => sub {
            $size = int(shift);
        },
    );
    return wantarray ? ($size, 'raw', 0, undef) : $size;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($vmid, $format) = ($class->parse_volname($volname))[2, 6];

    die "can't snapshot '$format' volume\n" if $format ne 'qcow2';

    if ($running) {
        #rename with blockdev-reopen is done at qemu level when running
        eval { alloc_snap_image($class, $storeid, $scfg, $volname, $snap) };
        if ($@) {
            die "can't allocate new volume $volname: $@\n";
        }
        return;
    }

    $class->activate_volume($storeid, $scfg, $volname);

    #rename current volume to snap volume
    eval { $class->rename_snapshot($scfg, $storeid, $volname, 'current', $snap) };
    die "error rename $volname to $snap\n" if $@;

    eval { alloc_snap_image($class, $storeid, $scfg, $volname, $snap) };
    if ($@) {
        my $err = $@;
        eval { $class->rename_snapshot($scfg, $storeid, $volname, $snap, 'current') };
        die $err;
    }
}

# Asserts that a rollback to $snap on $volname is possible.
# If certain snapshots are preventing the rollback and $blockers is an array
# reference, the snapshot names can be pushed onto $blockers prior to dying.
sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap, $blockers) = @_;

    my $format = ($class->parse_volname($volname))[6];
    die "can't rollback snapshot for '$format' volume\n" if $format ne 'qcow2';

    $class->activate_volume($storeid, $scfg, $volname);

    my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
    my $found;
    $blockers //= []; # not guaranteed to be set by caller
    for my $snapid (
        sort { $snapshots->{$b}->{order} <=> $snapshots->{$a}->{order} }
        keys %$snapshots
    ) {
        next if $snapid eq 'current';

        if ($snapid eq $snap) {
            $found = 1;
        } elsif ($found) {
            push $blockers->@*, $snapid;
        }
    }

    die "can't rollback, snapshot '$snap' does not exist on '$volname'\n"
        if !$found;

    die "can't rollback, '$snap' is not most recent snapshot on '$volname'\n"
        if scalar($blockers->@*) > 0;

    return 1;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $format = ($class->parse_volname($volname))[6];

    die "can't rollback snapshot for '$format' volume\n" if $format ne 'qcow2';

    my $cleanup_worker = eval { free_snap_image($class, $storeid, $scfg, $volname, 'current'); };
    die "error deleting snapshot $snap $@\n" if $@;

    eval { alloc_snap_image($class, $storeid, $scfg, $volname, $snap) };

    fork_cleanup_worker($cleanup_worker);

    if ($@) {
        die "can't allocate new volume $volname: $@\n";
    }

    return undef;
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    die "can't delete snapshot for '$format' volume\n" if $format ne 'qcow2';

    if ($running) {
        my $cleanup_worker = eval { free_snap_image($class, $storeid, $scfg, $volname, $snap); };
        die "error deleting snapshot $snap $@\n" if $@;
        fork_cleanup_worker($cleanup_worker);
        return;
    }

    my $cmd = "";
    my $path = $class->filesystem_path($scfg, $volname);

    my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
    my $snappath = $snapshots->{$snap}->{file};
    my $snapvolname = $snapshots->{$snap}->{volname};
    die "volume $snappath is missing" if !-e $snappath;

    my $parentsnap = $snapshots->{$snap}->{parent};

    my $childsnap = $snapshots->{$snap}->{child};
    my $childpath = $snapshots->{$childsnap}->{file};
    my $childvolname = $snapshots->{$childsnap}->{volname};

    my $err = undef;
    #if first snapshot,as it should be bigger,  we merge child, and rename the snapshot to child
    if (!$parentsnap) {
        print "$volname: deleting snapshot '$snap' by commiting snapshot '$childsnap'\n";
        print "running 'qemu-img commit $childpath'\n";
        #can't use -d here, as it's an lvm volume
        $cmd = ['/usr/bin/qemu-img', 'commit', $childpath];
        eval { run_command($cmd) };
        if ($@) {
            warn
                "The state of $snap is now invalid. Don't try to clone or rollback it. You can only try to delete it again later\n";
            die "error commiting $childsnap to $snap; $@\n";
        }
        print "delete $childvolname\n";
        my $cleanup_worker =
            eval { free_snap_image($class, $storeid, $scfg, $volname, $childsnap) };
        if ($@) {
            die "error delete old snapshot volume $childvolname: $@\n";
        }

        print "rename $snapvolname to $childvolname\n";
        eval { lvrename($scfg, $snapvolname, $childvolname) };
        if ($@) {
            warn $@;
            $err = "error renaming snapshot: $@\n";
        }
        fork_cleanup_worker($cleanup_worker);

    } else {
        #we rebase the child image on the parent as new backing image
        my $parentpath = $snapshots->{$parentsnap}->{file};
        print
            "$volname: deleting snapshot '$snap' by rebasing '$childsnap' on top of '$parentsnap'\n";
        print "running 'qemu-img rebase -b $parentpath -F qcow -f qcow2 $childpath'\n";
        $cmd = [
            '/usr/bin/qemu-img',
            'rebase',
            '-b',
            $parentpath,
            '-F',
            'qcow2',
            '-f',
            'qcow2',
            $childpath,
        ];
        eval { run_command($cmd) };
        if ($@) {
            #in case of abort, the state of the snap is still clean, just a little bit bigger
            die "error rebase $childsnap from $parentsnap; $@\n";
        }
        #delete the snapshot
        my $cleanup_worker = eval { free_snap_image($class, $storeid, $scfg, $volname, $snap); };
        if ($@) {
            die "error deleting old snapshot volume $snapvolname\n";
        }
        fork_cleanup_worker($cleanup_worker);
    }

    die $err if $err;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => {
            base => { qcow2 => 1, raw => 1 },
            current => { qcow2 => 1, raw => 1 },
            snap => { qcow2 => 1 },
        },
        'rename' => {
            current => { qcow2 => 1, raw => 1 },
        },
        snapshot => {
            current => { qcow2 => 1 },
            snap => { qcow2 => 1 },
        },
        #       fixme: add later ? (we need to handle basepath, volume activation,...)
        #       template => {
        #           current => { raw => 1, qcow2 => 1},
        #       },
        #       clone => {
        #           base => { qcow2 => 1 },
        #       },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    } else {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if defined($features->{$feature}->{$key}->{$format});

    return undef;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    return () if defined($snapshot); # lvm-thin only
    return volume_import_formats(
        $class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots,
    );
}

sub volume_export {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots)
        = @_;
    die "volume export format $format not available for $class\n"
        if $format ne 'raw+size';
    die "cannot export volumes together with their snapshots in $class\n"
        if $with_snapshots;
    die "cannot export a snapshot in $class\n" if defined($snapshot);
    die "cannot export an incremental stream in $class\n" if defined($base_snapshot);
    my $file = $class->path($scfg, $volname, $storeid);
    my $size;
    # should be faster than querying LVM, also checks for the device file's availability
    run_command(
        ['/sbin/blockdev', '--getsize64', $file],
        outfunc => sub {
            my ($line) = @_;
            die "unexpected output from /sbin/blockdev: $line\n" if $line !~ /^(\d+)$/;
            $size = int($1);
        },
    );
    PVE::Storage::Plugin::write_common_header($fh, $size);
    run_command(['dd', "if=$file", "bs=64k", "status=progress"], output => '>&' . fileno($fh));
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    return () if $with_snapshots; # not supported
    return () if defined($base_snapshot); # not supported
    return ('raw+size');
}

sub volume_import {
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
        $allow_rename,
    ) = @_;
    die "volume import format $format not available for $class\n"
        if $format ne 'raw+size';
    die "cannot import volumes together with their snapshots in $class\n"
        if $with_snapshots;
    die "cannot import an incremental stream in $class\n" if defined($base_snapshot);

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $file_format) =
        $class->parse_volname($volname);
    die "cannot import format $format into a file of format $file_format\n"
        if $file_format ne 'raw';

    my $vg = $scfg->{vgname};
    my $lvs = lvm_list_volumes($vg);
    if ($lvs->{$vg}->{$volname}) {
        die "volume $vg/$volname already exists\n" if !$allow_rename;
        warn "volume $vg/$volname already exists - importing with a different name\n";
        $name = undef;
    }

    my ($size) = PVE::Storage::Plugin::read_common_header($fh);
    $size = PVE::Storage::Common::align_size_up($size, 1024) / 1024;

    eval {
        my $allocname = $class->alloc_image($storeid, $scfg, $vmid, 'raw', $name, $size);
        my $oldname = $volname;
        $volname = $allocname;
        if (defined($name) && $allocname ne $oldname) {
            die "internal error: unexpected allocated name: '$allocname' != '$oldname'\n";
        }
        my $file = $class->path($scfg, $volname, $storeid)
            or die "internal error: failed to get path to newly allocated volume $volname\n";

        $class->volume_import_write($fh, $file);
    };
    if (my $err = $@) {
        my $cleanup_worker = eval { $class->free_image($storeid, $scfg, $volname, 0) };
        warn $@ if $@;
        fork_cleanup_worker($cleanup_worker);
        die $err;
    }

    return "$storeid:$volname";
}

sub volume_import_write {
    my ($class, $input_fh, $output_file) = @_;
    run_command(['dd', "of=$output_file", 'bs=64k'], input => '<&' . fileno($input_fh));
}

sub rename_volume {
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;

    my (
        undef, $source_image, $source_vmid, $base_name, $base_vmid, undef, $format,
    ) = $class->parse_volname($source_volname);

    if ($format eq 'qcow2') {
        my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $source_volname);
        die "we can't rename volume if external snapshot exists" if $snapshots->{current}->{parent};
    }

    $target_volname = $class->find_free_diskname($storeid, $scfg, $target_vmid, $format)
        if !$target_volname;

    my $vg = $scfg->{vgname};
    my $lvs = lvm_list_volumes($vg);
    die "target volume '${target_volname}' already exists\n"
        if ($lvs->{$vg}->{$target_volname});

    lvrename($scfg, $source_volname, $target_volname);
    return "${storeid}:${target_volname}";
}

sub rename_snapshot {
    my ($class, $scfg, $storeid, $volname, $source_snap, $target_snap) = @_;

    my $source_snap_volname = get_snap_name($class, $volname, $source_snap);
    my $target_snap_volname = get_snap_name($class, $volname, $target_snap);

    lvrename($scfg, $source_snap_volname, $target_snap_volname);
}

sub volume_support_qemu_snapshot {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $format = ($class->parse_volname($volname))[6];
    return 'external' if $format eq 'qcow2';
}

1;
