package PVE::Storage::RBDPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

sub rbd_unittobytes {
  {
       "M"  => 1024*1024,
       "G"  => 1024*1024*1024,
       "T"  => 1024*1024*1024*1024,
  }
}

my $add_pool_to_disk = sub {
    my ($scfg, $disk) = @_;

    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    return "$pool/$disk";
};

my $rbd_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $keyring = "/etc/pve/priv/ceph/${storeid}.keyring";
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    my $username =  $scfg->{username} ? $scfg->{username} : 'admin';

    my $cmd = ['/usr/bin/rbd', '-p', $pool, '-m', $monhost]; 

    if(-e $keyring){
	push @$cmd, '-n', "client.$username";
	push @$cmd, '--keyring', $keyring;
	push @$cmd, '--auth_supported', 'cephx';
    }else{
	push @$cmd, '--auth_supported', 'none';
    }

    push @$cmd, $op;

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

my $rados_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $keyring = "/etc/pve/priv/ceph/${storeid}.keyring";
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    my $username =  $scfg->{username} ? $scfg->{username} : 'admin';

    my $cmd = ['/usr/bin/rados', '-p', $pool, '-m', $monhost];

    if(-e $keyring){
	push @$cmd, '-n', "client.$username";
	push @$cmd, '--keyring', $keyring;
	push @$cmd, '--auth_supported', 'cephx';
    }else{
	push @$cmd, '--auth_supported', 'none';
    }

    push @$cmd, $op;

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

sub run_rbd_command {
    my ($cmd, %args) = @_;

    my $lasterr;
    my $errmsg = $args{errmsg} . ": " || "";
    if (!exists($args{errfunc})) {
	# ' error: 2014-02-06 11:51:59.839135 7f09f94d0760 -1 librbd: snap_unprotect: can't unprotect;
	# at least 1 child(ren) in pool cephstor1
	$args{errfunc} = sub {
	    my $line = shift;
	    if ($line =~ m/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]+ [\-\d]+ librbd: (.*)$/) {
		$lasterr = "$1\n";
	    } else {
		$lasterr = $line;
	    }
	    print STDERR $lasterr;
	    *STDERR->flush();
	};
    }
    
    eval { run_command($cmd, %args); };
    if (my $err = $@) {
	die $errmsg . $lasterr if length($lasterr);
	die $err;
    }

    return undef;
}

sub rbd_ls {
    my ($scfg, $storeid) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'ls', '-l');
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $list = {};

    my $parser = sub {
	my $line = shift;

	if ($line =~  m/^((vm|base)-(\d+)-disk-\d+)\s+(\d+)(M|G|T)\s((\S+)\/((vm|base)-\d+-\S+@\S+))?/) {
	    my ($image, $owner, $size, $unit, $parent) = ($1, $3, $4, $5, $8);

	    $list->{$pool}->{$image} = {
		name => $image,
		size => $size*rbd_unittobytes()->{$unit},
		parent => $parent,
		vmid => $owner
	    };
	}
    };

    eval {
	run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);
    };
    my $err = $@;

    die $err if $err && $err !~ m/doesn't contain rbd images/ ;
  
    return $list;
}

sub rbd_volume_info {
    my ($scfg, $storeid, $volname, $snap) = @_;

    my $cmd = undef;

    if($snap){
       $cmd = &$rbd_cmd($scfg, $storeid, 'info', $volname, '--snap', $snap);
    }else{
       $cmd = &$rbd_cmd($scfg, $storeid, 'info', $volname);
    }

    my $size = undef;
    my $parent = undef;
    my $format = undef;
    my $protected = undef;

    my $parser = sub {
	my $line = shift;

	if ($line =~ m/size (\d+) (M|G|T)B in (\d+) objects/) {
	    $size = $1 * rbd_unittobytes()->{$2} if ($1);
	} elsif ($line =~ m/parent:\s(\S+)\/(\S+)/) {
	    $parent = $2;
	} elsif ($line =~ m/format:\s(\d+)/) {
	    $format = $1;
	} elsif ($line =~ m/protected:\s(\S+)/) {
	    $protected = 1 if $1 eq "True";
	}

    };

    run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);

    return ($size, $parent, $format, $protected);
}

# Configuration

PVE::JSONSchema::register_format('pve-storage-monhost', \&parse_monhost);
sub parse_monhost {
    my ($name, $noerr) = @_;

    if ($name !~ m/^[a-z][a-z0-9\-\_\.]*[a-z0-9]$/i) {
	return undef if $noerr;
	die "lvm name '$name' contains illegal characters\n";
    }

    return $name;
}

sub type {
    return 'rbd';
}

sub plugindata {
    return {
	content => [ {images => 1, rootdir => 1}, { images => 1 }],
    };
}

sub properties {
    return {
	monhost => {
	    description => "Monitors daemon ips.",
	    type => 'string',
	},
	pool => {
	    description => "Pool.",
	    type => 'string',
	},
	username => {
	    description => "RBD Id.",
	    type => 'string',
	},
	authsupported => {
	    description => "Authsupported.",
	    type => 'string',
	},
	krbd => {
	    description => "Access rbd through krbd kernel module.",
	    type => 'boolean',
	},
    };
}

sub options {
    return {
	nodes => { optional => 1 },
	disable => { optional => 1 },
	monhost => { fixed => 1 },
	pool => { optional => 1 },
	username => { optional => 1 },
	content => { optional => 1 },
	krbd => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^((base-(\d+)-\S+)\/)?((base)?(vm)?-(\d+)-\S+)$/) {
	return ('images', $4, $7, $2, $3, $5, 'raw');
    }

    die "unable to parse rbd volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;

    my $monhost = $scfg->{monhost};
    $monhost =~ s/:/\\:/g;

    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    my $username =  $scfg->{username} ? $scfg->{username} : 'admin';

    my $path = "rbd:$pool/$name:mon_host=$monhost";
    my $keyring = "/etc/pve/priv/ceph/${storeid}.keyring";

    if(-e $keyring ){
        $path .= ":id=$username:auth_supported=cephx:keyring=$keyring";
    }else{
	$path .= ":auth_supported=none";
    }

    $path = "/dev/rbd/$pool/$name" if $scfg->{krbd};

    return ($path, $vmid, $vtype);
}

my $find_free_diskname = sub {
    my ($storeid, $scfg, $vmid) = @_;

    my $rbd = rbd_ls($scfg, $storeid);
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    my $disk_ids = {};
    my $dat = $rbd->{$pool};

    foreach my $image (keys %$dat) {
	my $volname = $dat->{$image}->{name};
	if ($volname =~ m/(vm|base)-$vmid-disk-(\d+)/){
	    $disk_ids->{$2} = 1;
	}
    }
    #fix: can we search in $rbd hash key with a regex to find (vm|base) ?
    for (my $i = 1; $i < 100; $i++) {
        if (!$disk_ids->{$i}) {
            return "vm-$vmid-disk-$i";
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n";
};

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $snap = '__base__';

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    die "create_base not possible with base image\n" if $isBase;

    my ($size, $parent, $format, undef) = rbd_volume_info($scfg, $storeid, $name);
    die "rbd volume info on '$name' failed\n" if !($size);

    die "rbd image must be at format V2" if $format ne "2";

    die "volname '$volname' contains wrong information about parent $parent $basename\n"
        if $basename && (!$parent || $parent ne $basename."@".$snap);

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basename/$newname" : "$newname";

    my $cmd = &$rbd_cmd($scfg, $storeid, 'rename', &$add_pool_to_disk($scfg, $name), &$add_pool_to_disk($scfg, $newname));
    run_rbd_command($cmd, errmsg => "rbd rename '$name' error");

    my $running  = undef; #fixme : is create_base always offline ?

    $class->volume_snapshot($scfg, $storeid, $newname, $snap, $running);

    my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $newname, $snap);

    if (!$protected){
	my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'protect', $newname, '--snap', $snap);
	run_rbd_command($cmd, errmsg => "rbd protect $newname snap '$snap' error");
    }

    return $newvolname;

}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snapname) = @_;

    my $snap = '__base__';
    $snap = $snapname if length $snapname;

    my ($vtype, $basename, $basevmid, undef, undef, $isBase) =
        $class->parse_volname($volname);

    die "$volname is not a base image and snapname is not provided\n" 
	if !$isBase && !length($snapname);

    my $name = &$find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename snapname $snap to $name\n";

    if (length($snapname)) {
	my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $volname, $snapname);

	if (!$protected) {
	    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'protect', $volname, '--snap', $snapname);
	    run_rbd_command($cmd, errmsg => "rbd protect $volname snap $snapname error");
	}
    }

    my $newvol = "$basename/$name";
    $newvol = $name if length($snapname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'clone', &$add_pool_to_disk($scfg, $basename), 
			'--snap', $snap, &$add_pool_to_disk($scfg, $name));

    run_rbd_command($cmd, errmsg => "rbd clone '$basename' error");

    return $newvol;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    $name = &$find_free_diskname($storeid, $scfg, $vmid) if !$name;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'create', '--image-format' , 2, '--size', int(($size+1023)/1024), $name);
    run_rbd_command($cmd, errmsg => "rbd create $name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid, undef, undef, undef) =
	$class->parse_volname($volname);

    if ($isBase) {
	my $snap = '__base__';
	my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $name, $snap);
	if ($protected){
	    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'unprotect', $name, '--snap', $snap);
	    run_rbd_command($cmd, errmsg => "rbd unprotect $name snap '$snap' error");
	}
    }

    $class->deactivate_volume($storeid, $scfg, $volname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'purge',  $name);
    run_rbd_command($cmd, errmsg => "rbd snap purge '$volname' error");

    $cmd = &$rbd_cmd($scfg, $storeid, 'rm', $name);
    run_rbd_command($cmd, errmsg => "rbd rm '$volname' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{rbd} = rbd_ls($scfg, $storeid) if !$cache->{rbd};
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $res = [];

    if (my $dat = $cache->{rbd}->{$pool}) {
        foreach my $image (keys %$dat) {

            my $volname = $dat->{$image}->{name};

            my $volid = "$storeid:$volname";

            my $owner = $dat->{$volname}->{vmid};
            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                next if defined ($vmid) && ($owner ne $vmid);
            }

            my $info = $dat->{$volname};
            $info->{volid} = $volid;
	    $info->{format} = 'raw';

            push @$res, $info;
        }
    }
    
    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $cmd = &$rados_cmd($scfg, $storeid, 'df');

    my $stats = {};

    my $parser = sub {
	my $line = shift;
	if ($line =~ m/^\s+total\s(\S+)\s+(\d+)/) {
	    $stats->{$1} = $2;
	}
    };

    eval {
	run_rbd_command($cmd, errmsg => "rados error", errfunc => sub {}, outfunc => $parser);
    };

    my $total = $stats->{space} ? $stats->{space}*1024 : 0;
    my $free = $stats->{avail} ? $stats->{avail}*1024 : 0;
    my $used = $stats->{used} ? $stats->{used}*1024: 0;
    my $active = 1;

    return ($total, $free, $used, $active);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    return 1 if !$scfg->{krbd};

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $path = "/dev/rbd/$pool/$name";
    $path .= '@'.$snapname if $snapname;
    return if -b $path;

    $name .= '@'.$snapname if $snapname;
    my $cmd = &$rbd_cmd($scfg, $storeid, 'map', $name);
    run_rbd_command($cmd, errmsg => "can't mount rbd volume $name");

    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    return 1 if !$scfg->{krbd};

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $path = "/dev/rbd/$pool/$name";
    $path .= '@'.$snapname if $snapname;
    return if ! -b $path;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'unmap', $path);
    run_rbd_command($cmd, errmsg => "can't unmount rbd volume $name");

    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my ($size, undef) = rbd_volume_info($scfg, $storeid, $name);
    return $size;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    return 1 if $running;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'resize', '--size', ($size/1024/1024), $name);
    run_rbd_command($cmd, errmsg => "rbd resize '$volname' error");
    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'create', '--snap', $snap, $name);
    run_rbd_command($cmd, errmsg => "rbd snapshot '$volname' error");
    return undef;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'rollback', '--snap', $snap, $name);
    run_rbd_command($cmd, errmsg => "rbd snapshot $volname to '$snap' error");
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running;

    $class->deactivate_volume($storeid, $scfg, $volname, $snap, {});

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $name, $snap);
    if ($protected){
	my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'unprotect', $name, '--snap', $snap);
	run_rbd_command($cmd, errmsg => "rbd unprotect $name snap '$snap' error");
    }

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'rm', '--snap', $snap, $name);

    run_rbd_command($cmd, errmsg => "rbd snapshot '$volname' error");

    return undef;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

   my $features = {
	snapshot => { current => 1, snap => 1},
	clone => { base => 1, snap => 1},
	template => { current => 1},
	copy => { base => 1, current => 1, snap => 1},
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
