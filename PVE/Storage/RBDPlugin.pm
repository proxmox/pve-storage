package PVE::Storage::RBDPlugin;

use strict;
use warnings;
use IO::File;
use Net::IP;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);
use PVE::RADOS;

use base qw(PVE::Storage::Plugin);

my $rbd_unittobytes = {
    "k"  => 1024,
    "M"  => 1024*1024,
    "G"  => 1024*1024*1024,
    "T"  => 1024*1024*1024*1024,
};

my $add_pool_to_disk = sub {
    my ($scfg, $disk) = @_;

    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    return "$pool/$disk";
};

my $hostlist = sub {
    my ($list_text, $separator) = @_;

    my @monhostlist = PVE::Tools::split_list($list_text);
    return join($separator, map {
	my ($host, $port) = PVE::Tools::parse_host_and_port($_);
	$port = defined($port) ? ":$port" : '';
	$host = "[$host]" if Net::IP::ip_is_ipv6($host);
	"${host}${port}"
    } @monhostlist);
};

my $ceph_connect_option = sub {
    my ($scfg, $storeid, %options) = @_;

    my $cmd_option = {};
    my $ceph_storeid_conf = "/etc/pve/priv/ceph/${storeid}.conf";
    my $pveceph_config = '/etc/pve/ceph.conf';
    my $keyring = "/etc/pve/priv/ceph/${storeid}.keyring";
    my $pveceph_managed = !defined($scfg->{monhost});

    $cmd_option->{ceph_conf} = $pveceph_config if $pveceph_managed;

    if (-e $ceph_storeid_conf) {
	if ($pveceph_managed) {
	    warn "ignoring custom ceph config for storage '$storeid', 'monhost' is not set (assuming pveceph managed cluster)!\n";
	} else {
	    $cmd_option->{ceph_conf} = $ceph_storeid_conf;
	}
    }

    $cmd_option->{keyring} = $keyring if (-e $keyring);
    $cmd_option->{auth_supported} = (defined $cmd_option->{keyring}) ? 'cephx' : 'none';
    $cmd_option->{userid} =  $scfg->{username} ? $scfg->{username} : 'admin';
    $cmd_option->{mon_host} = $hostlist->($scfg->{monhost}, ',') if (defined($scfg->{monhost}));

    if (%options) {
	foreach my $k (keys %options) {
	    $cmd_option->{$k} = $options{$k};
	}
    }

    return $cmd_option;

};

my $build_cmd = sub {
    my ($binary, $scfg, $storeid, $op, @options) = @_;

    my $cmd_option = $ceph_connect_option->($scfg, $storeid);
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $cmd = [$binary, '-p', $pool];

    push @$cmd, '-c', $cmd_option->{ceph_conf} if ($cmd_option->{ceph_conf});
    push @$cmd, '-m', $cmd_option->{mon_host} if ($cmd_option->{mon_host});
    push @$cmd, '--auth_supported', $cmd_option->{auth_supported} if ($cmd_option->{auth_supported});
    push @$cmd, '-n', "client.$cmd_option->{userid}" if ($cmd_option->{userid});
    push @$cmd, '--keyring', $cmd_option->{keyring} if ($cmd_option->{keyring});

    push @$cmd, $op;

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

my $rbd_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    return $build_cmd->('/usr/bin/rbd', $scfg, $storeid, $op, @options);
};

my $rados_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    return $build_cmd->('/usr/bin/rados', $scfg, $storeid, $op, @options);
};

my $librados_connect = sub {
    my ($scfg, $storeid, $options) = @_;

    my $librados_config = $ceph_connect_option->($scfg, $storeid);

    my $rados = PVE::RADOS->new(%$librados_config);

    return $rados;
};

# needed for volumes created using ceph jewel (or higher)
my $krbd_feature_disable = sub {
    my ($scfg, $storeid, $name) = @_;

    return 1 if !$scfg->{krbd};

    my ($major, undef, undef, undef) = ceph_version();
    return 1 if $major < 10;

    my $krbd_feature_blacklist = ['deep-flatten', 'fast-diff', 'object-map', 'exclusive-lock'];
    my (undef, undef, undef, undef, $features) = rbd_volume_info($scfg, $storeid, $name);

    my $active_features = { map { $_ => 1 } PVE::Tools::split_list($features)};
    my $incompatible_features = join(',', grep { %$active_features{$_} } @$krbd_feature_blacklist);

    if ($incompatible_features) {
	my $feature_cmd = &$rbd_cmd($scfg, $storeid, 'feature', 'disable', $name, $incompatible_features);
	run_rbd_command($feature_cmd, errmsg => "could not disable krbd-incompatible image features of rbd volume $name");
    }
};

my $ceph_version_parser = sub {
    my $line = shift;
    if ($line =~ m/^ceph version ((\d+)\.(\d+)\.(\d+))(?: \([a-fA-F0-9]+\))/) {
	return ($2, $3, $4, $1);
    } else {
	warn "Could not parse Ceph version: '$line'\n";
    }
};

sub ceph_version {
    my ($cache) = @_;

    my $version_string = $cache;

    my $major;
    my $minor;
    my $bugfix;

    if (defined($version_string)) {
	($major, $minor, $bugfix, $version_string) = &$ceph_version_parser($version_string);
    } else {
	run_command('ceph --version', outfunc => sub {
	    my $line = shift;
	    ($major, $minor, $bugfix, $version_string) = &$ceph_version_parser($line);
	});
    }
    return undef if !defined($version_string);
    return wantarray ? ($major, $minor, $bugfix, $version_string) : $version_string;
}

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

	if ($line =~  m/^((vm|base)-(\d+)-\S+)\s+(\d+)(k|M|G|T)\s((\S+)\/((vm|base)-\d+-\S+@\S+))?/) {
	    my ($image, $owner, $size, $unit, $parent) = ($1, $3, $4, $5, $8);
	    return if $image =~ /@/; #skip snapshots

	    $list->{$pool}->{$image} = {
		name => $image,
		size => $size*$rbd_unittobytes->{$unit},
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
    my $features = undef;

    my $parser = sub {
	my $line = shift;

	if ($line =~ m/size (\d+) (k|M|G|T)B in (\d+) objects/) {
	    $size = $1 * $rbd_unittobytes->{$2} if ($1);
	} elsif ($line =~ m/parent:\s(\S+)\/(\S+)/) {
	    $parent = $2;
	} elsif ($line =~ m/format:\s(\d+)/) {
	    $format = $1;
	} elsif ($line =~ m/protected:\s(\S+)/) {
	    $protected = 1 if $1 eq "True";
	} elsif ($line =~ m/features:\s(.+)/) {
	    $features = $1;
	}

    };

    run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);

    return ($size, $parent, $format, $protected, $features);
}

# Configuration

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
	    description => "IP addresses of monitors (for external clusters).",
	    type => 'string', format => 'pve-storage-portal-dns-list',
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
	monhost => { optional => 1},
	pool => { optional => 1 },
	username => { optional => 1 },
	content => { optional => 1 },
	krbd => { optional => 1 },
	bwlimit => { optional => 1 },
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

    my $cmd_option = $ceph_connect_option->($scfg, $storeid);
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;

    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    return ("/dev/rbd/$pool/$name", $vmid, $vtype) if $scfg->{krbd};

    my $path = "rbd:$pool/$name";

    $path .= ":conf=$cmd_option->{ceph_conf}" if $cmd_option->{ceph_conf};
    if (defined($scfg->{monhost})) {
	my $monhost = $hostlist->($scfg->{monhost}, ';');
	$monhost =~ s/:/\\:/g;
	$path .= ":mon_host=$monhost";
	$path .= ":auth_supported=$cmd_option->{auth_supported}";
    }

    $path .= ":id=$cmd_option->{userid}:keyring=$cmd_option->{keyring}" if ($cmd_option->{keyring});

    return ($path, $vmid, $vtype);
}

my $find_free_diskname = sub {
    my ($storeid, $scfg, $vmid) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'ls');
    my $disk_ids = {};

    my $parser = sub {
	my $line = shift;

	if ($line =~  m/^(vm|base)-\Q$vmid\E+-disk-(\d+)$/) {
	    $disk_ids->{$2} = 1;
	}
    };

    eval {
	run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);
    };
    my $err = $@;

    die $err if $err && $err !~ m/doesn't contain rbd images/;

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

    &$krbd_feature_disable($scfg, $storeid, $name);

    return $newvol;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    $name = &$find_free_diskname($storeid, $scfg, $vmid) if !$name;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'create', '--image-format' , 2, '--size', int(($size+1023)/1024), $name);
    run_rbd_command($cmd, errmsg => "rbd create $name' error");

    &$krbd_feature_disable($scfg, $storeid, $name);

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

	    my $info = $dat->{$image};

	    my $volname = $info->{name};
	    my $parent = $info->{parent};
	    my $owner = $info->{vmid};

	    if ($parent && $parent =~ m/^(base-\d+-\S+)\@__base__$/) {
		$info->{volid} = "$storeid:$1/$volname";
	    } else {
		$info->{volid} = "$storeid:$volname";
	    }

	    if ($vollist) {
		my $found = grep { $_ eq $info->{volid} } @$vollist;
		next if !$found;
	    } else {
		next if defined ($vmid) && ($owner ne $vmid);
	    }

	    $info->{format} = 'raw';

	    push @$res, $info;
	}
    }
    
    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;


    my $rados = &$librados_connect($scfg, $storeid);
    my $df = $rados->mon_command({ prefix => 'df', format => 'json' });

    my ($d) = grep { $_->{name} eq $scfg->{pool} } @{$df->{pools}};

    # max_avail -> max available space for data w/o replication in the pool
    # bytes_used -> data w/o replication in the pool
    my $free = $d->{stats}->{max_avail};
    my $used = $d->{stats}->{bytes_used};
    my $total = $used + $free;
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
    run_rbd_command($cmd, errmsg => "can't unmap rbd volume $name");

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

    return 1 if $running && !$scfg->{krbd};

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = &$rbd_cmd($scfg, $storeid, 'resize', '--allow-shrink', '--size', ($size/1024/1024), $name);
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

    return 1 if $running && !$scfg->{krbd};

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
	sparseinit => { base => 1, current => 1},
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
