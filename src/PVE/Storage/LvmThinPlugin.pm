package PVE::Storage::LvmThinPlugin;

use strict;
use warnings;

use IO::File;

use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::Storage::LVMPlugin;
use PVE::JSONSchema qw(get_standard_option);

# see: man lvmthin
# lvcreate -n ThinDataLV -L LargeSize VG
# lvconvert --type thin-pool VG/ThinDataLV
# lvcreate -n pvepool -L 20G pve
# lvconvert --type thin-pool pve/pvepool

# NOTE: volumes which were created as linked clones of another base volume
# are currently not tracking this relationship in their volume IDs. this is
# generally not a problem, as LVM thin allows deletion of such base volumes
# without affecting the linked clones. this leads to increased disk usage
# when migrating LVM-thin volumes, which is normally prevented for linked clones.

use base qw(PVE::Storage::LVMPlugin);

sub type {
    return 'lvmthin';
}

sub plugindata {
    return {
	content => [ {images => 1, rootdir => 1}, { images => 1, rootdir => 1}],
    };
}

sub properties {
    return {
	thinpool => {
	    description => "LVM thin pool LV name.",
	    type => 'string', format => 'pve-storage-vgname',
	},
    };
}

sub options {
    return {
	thinpool => { fixed => 1 },
	vgname => { fixed => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
	content => { optional => 1 },
	bwlimit => { optional => 1 },
    };
}

# NOTE: the fourth and fifth element of the returned array are always
# undef, even if the volume is a linked clone of another volume. see note
# at beginning of file.
sub parse_volname {
    my ($class, $volname) = @_;

    PVE::Storage::Plugin::parse_lvm_name($volname);

    if ($volname =~ m/^((vm|base)-(\d+)-\S+)$/) {
	return ('images', $1, $3, undef, undef, $2 eq 'base', 'raw');
    }

    die "unable to parse lvm volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $vg = $scfg->{vgname};

    my $path = defined($snapname) ? "/dev/$vg/snap_${name}_$snapname": "/dev/$vg/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    my $vgs = PVE::Storage::LVMPlugin::lvm_vgs();

    my $vg = $scfg->{vgname};

    die "no such volume group '$vg'\n" if !defined ($vgs->{$vg});

    $name = $class->find_free_diskname($storeid, $scfg, $vmid)
	if !$name;

    my $cmd = ['/sbin/lvcreate', '-aly', '-V', "${size}k", '--name', $name,
	       '--thinpool', "$vg/$scfg->{thinpool}" ];

    run_command($cmd, errmsg => "lvcreate '$vg/$name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $vg = $scfg->{vgname};

    my $lvs = PVE::Storage::LVMPlugin::lvm_list_volumes($vg);

    if (my $dat = $lvs->{$scfg->{vgname}}) {

	# remove all volume snapshots first
	foreach my $lv (keys %$dat) {
	    next if $lv !~ m/^snap_${volname}_${PVE::JSONSchema::CONFIGID_RE}$/;
	    my $cmd = ['/sbin/lvremove', '-f', "$vg/$lv"];
	    run_command($cmd, errmsg => "lvremove snapshot '$vg/$lv' error");
	}

	# finally remove original (if exists)
	if ($dat->{$volname}) {
	    my $cmd = ['/sbin/lvremove', '-f', "$vg/$volname"];
	    run_command($cmd, errmsg => "lvremove '$vg/$volname' error");
	}
    }

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{lvs} = PVE::Storage::LVMPlugin::lvm_list_volumes() if !$cache->{lvs};

    my $res = [];

    if (my $dat = $cache->{lvs}->{$vgname}) {

	foreach my $volname (keys %$dat) {

	    next if $volname !~ m/^(vm|base)-(\d+)-/;
	    my $owner = $2;

	    my $info = $dat->{$volname};

	    next if $info->{lv_type} ne 'V';

	    next if $info->{pool_lv} ne $scfg->{thinpool};

	    my $volid = "$storeid:$volname";

	    if ($vollist) {
		my $found = grep { $_ eq $volid } @$vollist;
		next if !$found;
	    } else {
		next if defined($vmid) && ($owner ne $vmid);
	    }

	    push @$res, {
		volid => $volid, format => 'raw', size => $info->{lv_size}, vmid => $owner,
		ctime => $info->{ctime},
	    };
	}
    }

    return $res;
}

sub list_thinpools {
    my ($vg) = @_;

    my $lvs = PVE::Storage::LVMPlugin::lvm_list_volumes($vg);
    my $thinpools = [];

    foreach my $vg (keys %$lvs) {
	foreach my $lvname (keys %{$lvs->{$vg}}) {
	    next if $lvs->{$vg}->{$lvname}->{lv_type} ne 't';
	    my $lv = $lvs->{$vg}->{$lvname};
	    $lv->{lv} = $lvname;
	    $lv->{vg} = $vg;
	    push @$thinpools, $lv;
	}
    }

    return $thinpools;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $lvs = $cache->{lvs} ||= PVE::Storage::LVMPlugin::lvm_list_volumes();

    return if !$lvs->{$scfg->{vgname}};

    my $info = $lvs->{$scfg->{vgname}}->{$scfg->{thinpool}};

    return if !$info || $info->{lv_type} ne 't' || !$info->{lv_size};

    return (
	$info->{lv_size},
	$info->{lv_size} - $info->{used},
	$info->{used},
	$info->{lv_state} eq 'a' ? 1 : 0,
    );
}

my $activate_lv = sub {
    my ($vg, $lv, $cache) = @_;

    my $lvs = $cache->{lvs} ||= PVE::Storage::LVMPlugin::lvm_list_volumes();

    die "no such logical volume $vg/$lv\n" if !$lvs->{$vg} || !$lvs->{$vg}->{$lv};

    return if $lvs->{$vg}->{$lv}->{lv_state} eq 'a';

    run_command(['lvchange', '-ay', '-K', "$vg/$lv"], errmsg => "activating LV '$vg/$lv' failed");

    $lvs->{$vg}->{$lv}->{lv_state} = 'a'; # update cache

    return;
};

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $class->SUPER::activate_storage($storeid, $scfg, $cache);

    $activate_lv->($scfg->{vgname}, $scfg->{thinpool}, $cache);
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $vg = $scfg->{vgname};
    my $lv = $snapname ? "snap_${volname}_$snapname" : $volname;

    $activate_lv->($vg, $lv, $cache);
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    return if !$snapname && $volname !~ /^base-/; # other volumes are kept active

    my $vg = $scfg->{vgname};
    my $lv = $snapname ? "snap_${volname}_$snapname" : $volname;

    run_command(['lvchange', '-an', "$vg/$lv"], errmsg => "deactivate_volume '$vg/$lv' error");

    $cache->{lvs}->{$vg}->{$lv}->{lv_state} = '-' # update cache
	if $cache->{lvs} && $cache->{lvs}->{$vg} && $cache->{lvs}->{$vg}->{$lv};

    return;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    my $vg = $scfg->{vgname};

    my $lv;

    if ($snap) {
	$lv = "$vg/snap_${volname}_$snap";
    } else {
	my ($vtype, undef, undef, undef, undef, $isBase, $format) =
	    $class->parse_volname($volname);

	die "clone_image only works on base images\n" if !$isBase;

	$lv = "$vg/$volname";
    }

    my $name = $class->find_free_diskname($storeid, $scfg, $vmid);

    my $cmd = ['/sbin/lvcreate', '-n', $name, '-prw', '-kn', '-s', $lv];
    run_command($cmd, errmsg => "clone image '$lv' error");

    return $name;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    die "create_base not possible with base image\n" if $isBase;

    my $vg = $scfg->{vgname};
    my $lvs = PVE::Storage::LVMPlugin::lvm_list_volumes($vg);

    if (my $dat = $lvs->{$vg}) {
	# to avoid confusion, reject if we find volume snapshots
	foreach my $lv (keys %$dat) {
	    die "unable to create base volume - found snaphost '$lv'\n"
		if $lv =~ m/^snap_${volname}_(\w+)$/;
	}
    }

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $cmd = ['/sbin/lvrename', $vg, $volname, $newname];
    run_command($cmd, errmsg => "lvrename '$vg/$volname' => '$vg/$newname' error");

    # set inactive, read-only and activationskip flags
    $cmd = ['/sbin/lvchange', '-an', '-pr', '-ky', "$vg/$newname"];
    eval { run_command($cmd); };
    warn $@ if $@;

    my $newvolname = $newname;

    return $newvolname;
}

# sub volume_resize {} reuse code from parent class

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $vg = $scfg->{vgname};
    my $snapvol = "snap_${volname}_$snap";

    my $cmd = ['/sbin/lvcreate', '-n', $snapvol, '-pr', '-s', "$vg/$volname"];
    run_command($cmd, errmsg => "lvcreate snapshot '$vg/$snapvol' error");

}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $vg = $scfg->{vgname};
    my $snapvol = "snap_${volname}_$snap";

    my $cmd = ['/sbin/lvremove', '-f', "$vg/$volname"];
    run_command($cmd, errmsg => "lvremove '$vg/$volname' error");

    $cmd = ['/sbin/lvcreate', '-kn', '-n', $volname, '-s', "$vg/$snapvol"];
    run_command($cmd, errmsg => "lvm rollback '$vg/$snapvol' error");
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $vg = $scfg->{vgname};
    my $snapvol = "snap_${volname}_$snap";

    my $cmd = ['/sbin/lvremove', '-f', "$vg/$snapvol"];
    run_command($cmd, errmsg => "lvremove snapshot '$vg/$snapvol' error");
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => 1 },
	clone => { base => 1, snap => 1},
	template => { current => 1},
	copy => { base => 1, current => 1, snap => 1},
	sparseinit => { base => 1, current => 1},
	rename => {current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
	$key = 'snap';
    }else{
	$key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

sub volume_import {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots, $allow_rename) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $file_format) =
	$class->parse_volname($volname);

    if (!$isBase) {
	return $class->SUPER::volume_import(
	    $scfg,
	    $storeid,
	    $fh,
	    $volname,
	    $format,
	    $snapshot,
	    $base_snapshot,
	    $with_snapshots,
	    $allow_rename
	);
    } else {
	my $tempname;
	my $vg = $scfg->{vgname};
	my $lvs = PVE::Storage::LVMPlugin::lvm_list_volumes($vg);
	if ($lvs->{$vg}->{$volname}) {
	    die "volume $vg/$volname already exists\n" if !$allow_rename;
	    warn "volume $vg/$volname already exists - importing with a different name\n";

	    $tempname = $class->find_free_diskname($storeid, $scfg, $vmid);
	} else {
	    $tempname = $volname;
	    $tempname =~ s/base/vm/;
	}

	($storeid,my $newname) = PVE::Storage::parse_volume_id($class->SUPER::volume_import(
	    $scfg,
	    $storeid,
	    $fh,
	    $tempname,
	    $format,
	    $snapshot,
	    $base_snapshot,
	    $with_snapshots,
	    $allow_rename
	));

	$volname = $class->create_base($storeid, $scfg, $newname);
    }

    return "$storeid:$volname";
}

# used in LVMPlugin->volume_import
sub volume_import_write {
    my ($class, $input_fh, $output_file) = @_;
    run_command(['dd', "of=$output_file", 'conv=sparse', 'bs=64k'],
	input => '<&'.fileno($input_fh));
}

1;
