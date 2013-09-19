package PVE::Storage::LVMPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# lvm helper functions

sub lvm_pv_info {
    my ($device) = @_;

    die "no device specified" if !$device;

    my $has_label = 0;

    my $cmd = ['/usr/bin/file', '-L', '-s', $device];
    run_command($cmd, outfunc => sub {
	my $line = shift;
	$has_label = 1 if $line =~ m/LVM2/;
    });

    return undef if !$has_label;

    $cmd = ['/sbin/pvs', '--separator', ':', '--noheadings', '--units', 'k',
	    '--unbuffered', '--nosuffix', '--options',
	    'pv_name,pv_size,vg_name,pv_uuid', $device];

    my $pvinfo;
    run_command($cmd, outfunc => sub {
	my $line = shift;

	$line = trim($line);

	my ($pvname, $size, $vgname, $uuid) = split(':', $line);

	die "found multiple pvs entries for device '$device'\n" 
	    if $pvinfo;

	$pvinfo = {
	    pvname => $pvname,
	    size => $size,
	    vgname => $vgname,
	    uuid => $uuid,
	};
    });

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

    run_command($cmd, errmsg => "vgcreate $vgname $device error");
}

sub lvm_vgs {

    my $cmd = ['/sbin/vgs', '--separator', ':', '--noheadings', '--units', 'b',
	       '--unbuffered', '--nosuffix', '--options',
	       'vg_name,vg_size,vg_free'];

    my $vgs = {};
    eval {
	run_command($cmd, outfunc => sub {
	    my $line = shift;

	    $line = trim($line);

	    my ($name, $size, $free) = split (':', $line);

	    $vgs->{$name} = { size => int ($size), free => int ($free) };
        });
    };
    my $err = $@;

    # just warn (vgs return error code 5 if clvmd does not run)
    # but output is still OK (list without clustered VGs)
    warn $err if $err;

    return $vgs;
}

sub lvm_lvs {
    my ($vgname) = @_;

    my $cmd = ['/sbin/lvs', '--separator', ':', '--noheadings', '--units', 'b',
	       '--unbuffered', '--nosuffix', '--options',
	       'vg_name,lv_name,lv_size,uuid,tags'];

    push @$cmd, $vgname if $vgname;

    my $lvs = {};
    run_command($cmd, outfunc => sub {
	my $line = shift;

	$line = trim($line);

	my ($vg, $name, $size, $uuid, $tags) = split(':', $line);

	return if $name !~ m/^vm-(\d+)-/;
	my $nid = $1;

	my $owner;
	foreach my $tag (split (/,/, $tags)) {
	    if ($tag =~ m/^pve-vm-(\d+)$/) {
		$owner = $1;
		last;
	    }
	}
	
	if ($owner) {
	    if ($owner ne $nid) {
		warn "owner mismatch name = $name, owner = $owner\n";
	    }
   
	    $lvs->{$vg}->{$name} = { format => 'raw', size => $size, 
				     uuid => $uuid,  tags => $tags, 
				     vmid => $owner };
	}
    });

    return $lvs;
}

# Configuration 

PVE::JSONSchema::register_format('pve-storage-vgname', \&parse_lvm_name);
sub parse_lvm_name {
    my ($name, $noerr) = @_;

    if ($name !~ m/^[a-z][a-z0-9\-\_\.]*[a-z0-9]$/i) {
	return undef if $noerr;
	die "lvm name '$name' contains illegal characters\n";
    }

    return $name;
}

sub type {
    return 'lvm';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}

sub properties {
    return {
	vgname => {
	    description => "Volume group name.",
	    type => 'string', format => 'pve-storage-vgname',
	},
	base => {
	    description => "Base volume. This volume is automatically activated.",
	    type => 'string', format => 'pve-volume-id',
	},
	saferemove => {
	    description => "Zero-out data when removing LVs.",
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
	content => { optional => 1 },
        base => { fixed => 1, optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    parse_lvm_name($volname);

    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
	return ('images', $1, $2);
    }

    die "unable to parse lvm volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $vg = $scfg->{vgname};
    
    my $path = "/dev/$vg/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in lvm storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid) = @_;

    die "can't clone images in lvm storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n" 
	if  $name && $name !~ m/^vm-$vmid-/;

    my $vgs = lvm_vgs();

    my $vg = $scfg->{vgname};

    die "no such volume group '$vg'\n" if !defined ($vgs->{$vg});

    my $free = int($vgs->{$vg}->{free});

    die "not enough free space ($free < $size)\n" if $free < $size;

    if (!$name) {
	my $lvs = lvm_lvs($vg);

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($lvs->{$vg}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my $cmd = ['/sbin/lvcreate', '-aly', '--addtag', "pve-vm-$vmid", '--size', "${size}k", '--name', $name, $vg];

    run_command($cmd, errmsg => "lvcreate '$vg/pve-vm-$vmid' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $vg = $scfg->{vgname};
    
    # we need to zero out LVM data for security reasons
    # and to allow thin provisioning

    my $zero_out_worker = sub {
	print "zero-out data on image $volname\n";
	my $cmd = ['dd', "if=/dev/zero", "of=/dev/$vg/del-$volname", "bs=1M"];
	eval { run_command($cmd, errmsg => "zero out failed"); };
	warn $@ if $@;

	$class->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	    my $cmd = ['/sbin/lvremove', '-f', "$vg/del-$volname"];
	    run_command($cmd, errmsg => "lvremove '$vg/del-$volname' error");
	});
	print "successfully removed volume $volname\n";
    };

    if ($scfg->{saferemove}) {
	# avoid long running task, so we only rename here
	my $cmd = ['/sbin/lvrename', $vg, $volname, "del-$volname"];
	run_command($cmd, errmsg => "lvrename '$vg/$volname' error");
	return $zero_out_worker;
    } else {
	my $tmpvg = $scfg->{vgname};
	my $cmd = ['/sbin/lvremove', '-f', "$tmpvg/$volname"];
	run_command($cmd, errmsg => "lvremove '$tmpvg/$volname' error");
    }

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{lvs} = lvm_lvs() if !$cache->{lvs};

    my $res = [];
    
    if (my $dat = $cache->{lvs}->{$vgname}) {

	foreach my $volname (keys %$dat) {

	    my $owner = $dat->{$volname}->{vmid};

	    my $volid = "$storeid:$volname";

	    if ($vollist) {
		my $found = grep { $_ eq $volid } @$vollist;
		next if !$found;
	    } else {
		next if defined ($vmid) && ($owner ne $vmid);
	    }

	    my $info = $dat->{$volname};
	    $info->{volid} = $volid;

	    push @$res, $info;
	}
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    my $vgname = $scfg->{vgname};

    my $total = 0;
    my $free = 0;
    my $used = 0;

    if (my $info = $cache->{vgs}->{$vgname}) {
	return ($info->{size}, $info->{free}, $total - $free, 1);
    }

    return undef;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    # In LVM2, vgscans take place automatically;
    # this is just to be sure
    if ($cache->{vgs} && !$cache->{vgscaned} && 
	!$cache->{vgs}->{$scfg->{vgname}}) {
	$cache->{vgscaned} = 1;
	my $cmd = ['/sbin/vgscan', '--ignorelockingfailure', '--mknodes'];
	eval { run_command($cmd, outfunc => sub {}); };
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
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;

    my $path = $class->path($scfg, $volname);

    my $lvm_activate_mode = $exclusive ? 'ey' : 'ly';

    my $cmd = ['/sbin/lvchange', "-a$lvm_activate_mode", $path];
    run_command($cmd, errmsg => "can't activate LV '$path'");
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $cache) = @_;

    my $path = $class->path($scfg, $volname);
    return if ! -b $path;

    my $cmd = ['/sbin/lvchange', '-aln', $path];
    run_command($cmd, errmsg => "can't deactivate LV '$path'");
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    $size = ($size/1024/1024) . "M";

    my $path = $class->path($scfg, $volname);
    my $cmd = ['/sbin/lvextend', '-L', $size, $path];
    run_command($cmd, errmsg => "error resizing volume '$path'");

    return 1;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "lvm snapshot is not implemented";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "lvm snapshot rollback is not implemented";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "lvm snapshot delete is not implemented";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	copy => { base => 1, current => 1},
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

1;
