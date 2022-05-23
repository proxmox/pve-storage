package PVE::Storage::RBDPlugin;

use strict;
use warnings;

use Cwd qw(abs_path);
use IO::File;
use JSON;
use Net::IP;

use PVE::CephConfig;
use PVE::Cluster qw(cfs_read_file);;
use PVE::JSONSchema qw(get_standard_option);
use PVE::ProcFSTools;
use PVE::RADOS;
use PVE::Storage::Plugin;
use PVE::Tools qw(run_command trim file_read_firstline);

use base qw(PVE::Storage::Plugin);

my $get_parent_image_name = sub {
    my ($parent) = @_;
    return undef if !$parent;
    return $parent->{image} . "@" . $parent->{snapshot};
};

my $librados_connect = sub {
    my ($scfg, $storeid, $options) = @_;

    my $librados_config = PVE::CephConfig::ceph_connect_option($scfg, $storeid);

    my $rados = PVE::RADOS->new(%$librados_config);

    return $rados;
};

my sub get_rbd_path {
    my ($scfg, $volume) = @_;
    my $path = $scfg->{pool} ? $scfg->{pool} : 'rbd';
    $path .= "/$scfg->{namespace}" if defined($scfg->{namespace});
    $path .= "/$volume" if defined($volume);
    return $path;
};

my sub get_rbd_dev_path {
    my ($scfg, $storeid, $volume) = @_;

    my $cluster_id = '';
    if ($scfg->{fsid}) {
	# NOTE: the config doesn't support this currently (but it could!), hack for qemu-server tests
	$cluster_id = $scfg->{fsid};
    } elsif ($scfg->{monhost}) {
	my $rados = $librados_connect->($scfg, $storeid);
	$cluster_id = $rados->mon_command({ prefix => 'fsid', format => 'json' })->{fsid};
    } else {
	$cluster_id = cfs_read_file('ceph.conf')->{global}->{fsid};
    }

    my $uuid_pattern = "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
    if ($cluster_id =~ qr/^${uuid_pattern}$/is) {
	$cluster_id = $1; # use untained value
    } else {
	die "cluster fsid has invalid format\n";
    }

    my $rbd_path = get_rbd_path($scfg, $volume);
    my $pve_path = "/dev/rbd-pve/${cluster_id}/${rbd_path}";
    my $path = "/dev/rbd/${rbd_path}";

    if (!-e $pve_path && -e $path) {
	# possibly mapped before rbd-pve rule existed
	my $real_dev = abs_path($path);
	my ($rbd_id) = ($real_dev =~ m|/dev/rbd([0-9]+)$|);
	my $dev_cluster_id = file_read_firstline("/sys/devices/rbd/${rbd_id}/cluster_fsid");
	return $path if $cluster_id eq $dev_cluster_id;
    }
    return $pve_path;
}

my $build_cmd = sub {
    my ($binary, $scfg, $storeid, $op, @options) = @_;

    my $cmd_option = PVE::CephConfig::ceph_connect_option($scfg, $storeid);
    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';

    my $cmd = [$binary, '-p', $pool];

    if (defined(my $namespace = $scfg->{namespace})) {
	# some subcommands will fail if the --namespace parameter is present
	my $no_namespace_parameter = {
	    unmap => 1,
	};
	push @$cmd, '--namespace', "$namespace" if !$no_namespace_parameter->{$op};
    }
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

# needed for volumes created using ceph jewel (or higher)
my $krbd_feature_update = sub {
    my ($scfg, $storeid, $name) = @_;

    my (@disable, @enable);
    my ($kmajor, $kminor) = PVE::ProcFSTools::kernel_version();

    if ($kmajor > 5 || $kmajor == 5 && $kminor >= 3) {
	# 'deep-flatten' can only be disabled, not enabled after image creation
	push @enable, 'fast-diff', 'object-map';
    } else {
	push @disable, 'fast-diff', 'object-map', 'deep-flatten';
    }

    if ($kmajor >= 5) {
	push @enable, 'exclusive-lock';
    } else {
	push @disable, 'exclusive-lock';
    }

    my $active_features_list = (rbd_volume_info($scfg, $storeid, $name))[4];
    my $active_features = { map { $_ => 1 } @$active_features_list };

    my $to_disable = join(',', grep {  $active_features->{$_} } @disable);
    my $to_enable  = join(',', grep { !$active_features->{$_} } @enable );

    if ($to_disable) {
	print "disable RBD image features this kernel RBD drivers is not compatible with: $to_disable\n";
	my $cmd = $rbd_cmd->($scfg, $storeid, 'feature', 'disable', $name, $to_disable);
	run_rbd_command(
	    $cmd,
	    errmsg => "could not disable krbd-incompatible image features '$to_disable' for rbd image: $name",
	);
    }
    if ($to_enable) {
	print "enable RBD image features this kernel RBD drivers supports: $to_enable\n";
	eval {
	    my $cmd = $rbd_cmd->($scfg, $storeid, 'feature', 'enable', $name, $to_enable);
	    run_rbd_command(
		$cmd,
		errmsg => "could not enable krbd-compatible image features '$to_enable' for rbd image: $name",
	    );
	};
	warn "$@" if $@;
    }
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

    my $pool =  $scfg->{pool} ? $scfg->{pool} : 'rbd';
    $pool .= "/$scfg->{namespace}" if defined($scfg->{namespace});

    my $raw = '';
    my $parser = sub { $raw .= shift };

    my $cmd = $rbd_cmd->($scfg, $storeid, 'ls', '-l', '--format', 'json');
    eval {
	run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);
    };
    my $err = $@;

    die $err if $err && $err !~ m/doesn't contain rbd images/ ;

    my $result;
    if ($raw eq '') {
	$result = [];
    } elsif ($raw =~ m/^(\[.*\])$/s) { # untaint
	$result = JSON::decode_json($1);
    } else {
	die "got unexpected data from rbd ls: '$raw'\n";
    }

    my $list = {};

    foreach my $el (@$result) {
	next if defined($el->{snapshot});

	my $image = $el->{image};

	my ($owner) = $image =~ m/^(?:vm|base)-(\d+)-/;
	next if !defined($owner);

	$list->{$pool}->{$image} = {
	    name => $image,
	    size => $el->{size},
	    parent => $get_parent_image_name->($el->{parent}),
	    vmid => $owner
	};
    }

    return $list;
}

sub rbd_ls_snap {
    my ($scfg, $storeid, $name) = @_;

    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'ls', $name, '--format', 'json');

    my $raw = '';
    run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => sub { $raw .= shift; });

    my $list;
    if ($raw =~ m/^(\[.*\])$/s) { # untaint
	$list = eval { JSON::decode_json($1) };
	die "invalid JSON output from 'rbd snap ls $name': $@\n" if $@;
    } else {
	die "got unexpected data from 'rbd snap ls $name': '$raw'\n";
    }

    $list = [] if !defined($list);

    my $res = {};
    foreach my $el (@$list) {
	my $snap = $el->{name};
	my $protected = defined($el->{protected}) && $el->{protected} eq "true" ? 1 : undef;
	$res->{$snap} = {
	    name => $snap,
	    id => $el->{id} // undef,
	    size => $el->{size} // 0,
	    protected => $protected,
	};
    }
    return $res;
}

sub rbd_volume_info {
    my ($scfg, $storeid, $volname, $snap) = @_;

    my $cmd = undef;

    my @options = ('info', $volname, '--format', 'json');
    if ($snap) {
	push @options, '--snap', $snap;
    }

    $cmd = $rbd_cmd->($scfg, $storeid, @options);

    my $raw = '';
    my $parser = sub { $raw .= shift };

    run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);

    my $volume;
    if ($raw eq '') {
	$volume = {};
    } elsif ($raw =~ m/^(\{.*\})$/s) { # untaint
	$volume = JSON::decode_json($1);
    } else {
	die "got unexpected data from rbd info: '$raw'\n";
    }

    $volume->{parent} = $get_parent_image_name->($volume->{parent});
    $volume->{protected} = defined($volume->{protected}) && $volume->{protected} eq "true" ? 1 : undef;

    return $volume->@{qw(size parent format protected features)};
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
	'data-pool' => {
	    description => "Data Pool (for erasure coding only)",
	    type => 'string',
	},
	namespace => {
	    description => "Namespace.",
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
	    description => "Always access rbd through krbd kernel module.",
	    type => 'boolean',
	},
	keyring => {
	    description => "Client keyring contents (for external clusters).",
	    type => 'string',
	},
    };
}

sub options {
    return {
	nodes => { optional => 1 },
	disable => { optional => 1 },
	monhost => { optional => 1},
	pool => { optional => 1 },
	'data-pool' => { optional => 1 },
	namespace => { optional => 1 },
	username => { optional => 1 },
	content => { optional => 1 },
	krbd => { optional => 1 },
	keyring => { optional => 1 },
	bwlimit => { optional => 1 },
    };
}

# Storage implementation

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    my $secret = $param{keyring} if defined $param{keyring} // undef;
    PVE::CephConfig::ceph_create_keyfile($scfg->{type}, $storeid, $secret);

    return;
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (exists($param{keyring})) {
	if (defined($param{keyring})) {
	    PVE::CephConfig::ceph_create_keyfile($scfg->{type}, $storeid, $param{keyring});
	} else {
	    PVE::CephConfig::ceph_remove_keyfile($scfg->{type}, $storeid);
	}
    }

    return;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;
    PVE::CephConfig::ceph_remove_keyfile($scfg->{type}, $storeid);
    return;
}

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^((base-(\d+)-\S+)\/)?((base)?(vm)?-(\d+)-\S+)$/) {
	return ('images', $4, $7, $2, $3, $5, 'raw');
    }

    die "unable to parse rbd volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    my $cmd_option = PVE::CephConfig::ceph_connect_option($scfg, $storeid);
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;

    my $rbd_dev_path = get_rbd_dev_path($scfg, $storeid, $name);
    return ($rbd_dev_path, $vmid, $vtype) if $scfg->{krbd};

    my $rbd_path = get_rbd_path($scfg, $name);
    my $path = "rbd:${rbd_path}";

    $path .= ":conf=$cmd_option->{ceph_conf}" if $cmd_option->{ceph_conf};
    if (defined($scfg->{monhost})) {
	my $monhost = PVE::CephConfig::hostlist($scfg->{monhost}, ';');
	$monhost =~ s/:/\\:/g;
	$path .= ":mon_host=$monhost";
	$path .= ":auth_supported=$cmd_option->{auth_supported}";
    }

    $path .= ":id=$cmd_option->{userid}:keyring=$cmd_option->{keyring}" if ($cmd_option->{keyring});

    return ($path, $vmid, $vtype);
}

sub find_free_diskname {
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;

    my $cmd = $rbd_cmd->($scfg, $storeid, 'ls');

    my $disk_list = [];

    my $parser = sub {
	my $line = shift;
	if ($line =~ m/^(.*)$/) { # untaint
	    push @$disk_list, $1;
	}
    };

    eval {
	run_rbd_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);
    };
    my $err = $@;

    die $err if $err && $err !~ m/doesn't contain rbd images/;

    return PVE::Storage::Plugin::get_next_vm_diskname($disk_list, $storeid, $vmid, undef, $scfg);
}

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

    my $cmd = $rbd_cmd->(
	$scfg,
	$storeid,
	'rename',
	get_rbd_path($scfg, $name),
	get_rbd_path($scfg, $newname),
    );
    run_rbd_command($cmd, errmsg => "rbd rename '$name' error");

    eval { $class->unmap_volume($storeid, $scfg, $volname); };
    warn $@ if $@;

    my $running  = undef; #fixme : is create_base always offline ?

    $class->volume_snapshot($scfg, $storeid, $newname, $snap, $running);

    my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $newname, $snap);

    if (!$protected){
	my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'protect', $newname, '--snap', $snap);
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

    my $name = $class->find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename snapname $snap to $name\n";

    if (length($snapname)) {
	my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $volname, $snapname);

	if (!$protected) {
	    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'protect', $volname, '--snap', $snapname);
	    run_rbd_command($cmd, errmsg => "rbd protect $volname snap $snapname error");
	}
    }

    my $newvol = "$basename/$name";
    $newvol = $name if length($snapname);

    my @options = (
	get_rbd_path($scfg, $basename),
	'--snap', $snap,
    );
    push @options, ('--data-pool', $scfg->{'data-pool'}) if $scfg->{'data-pool'};

    my $cmd = $rbd_cmd->($scfg, $storeid, 'clone', @options, get_rbd_path($scfg, $name));
    run_rbd_command($cmd, errmsg => "rbd clone '$basename' error");

    return $newvol;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - should be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    $name = $class->find_free_diskname($storeid, $scfg, $vmid) if !$name;

    my @options = (
	'--image-format' , 2,
	'--size', int(($size + 1023) / 1024),
    );
    push @options, ('--data-pool', $scfg->{'data-pool'}) if $scfg->{'data-pool'};

    my $cmd = $rbd_cmd->($scfg, $storeid, 'create', @options, $name);
    run_rbd_command($cmd, errmsg => "rbd create '$name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid, undef, undef, undef) =
	$class->parse_volname($volname);


    my $snaps = rbd_ls_snap($scfg, $storeid, $name);
    foreach my $snap (keys %$snaps) {
	if ($snaps->{$snap}->{protected}) {
	    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'unprotect', $name, '--snap', $snap);
	    run_rbd_command($cmd, errmsg => "rbd unprotect $name snap '$snap' error");
	}
    }

    $class->deactivate_volume($storeid, $scfg, $volname);

    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'purge',  $name);
    run_rbd_command($cmd, errmsg => "rbd snap purge '$name' error");

    $cmd = $rbd_cmd->($scfg, $storeid, 'rm', $name);
    run_rbd_command($cmd, errmsg => "rbd rm '$name' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{rbd} = rbd_ls($scfg, $storeid) if !$cache->{rbd};

    my $dat = $cache->{rbd}->{get_rbd_path($scfg)};
    return [] if !$dat; # nothing found

    my $res = [];
    for my $image (sort keys %$dat) {
	my $info = $dat->{$image};
	my ($volname, $parent, $owner) = $info->@{'name', 'parent', 'vmid'};

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

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $rados = $librados_connect->($scfg, $storeid);
    my $df = $rados->mon_command({ prefix => 'df', format => 'json' });

    my $pool = $scfg->{'data-pool'} // $scfg->{pool} // 'rbd';

    my ($d) = grep { $_->{name} eq $pool } @{$df->{pools}};

    if (!defined($d)) {
	warn "could not get usage stats for pool '$pool'\n";
	return;
    }

    # max_avail -> max available space for data w/o replication in the pool
    # bytes_used -> data w/o replication in the pool
    my $free = $d->{stats}->{max_avail};
    my $used = $d->{stats}->{stored} // $d->{stats}->{bytes_used};
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

sub map_volume {
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;

    my ($vtype, $img_name, $vmid) = $class->parse_volname($volname);

    my $name = $img_name;
    $name .= '@'.$snapname if $snapname;

    my $kerneldev = get_rbd_dev_path($scfg, $storeid, $name);

    return $kerneldev if -b $kerneldev; # already mapped

    # features can only be enabled/disabled for image, not for snapshot!
    $krbd_feature_update->($scfg, $storeid, $img_name);

    my $cmd = $rbd_cmd->($scfg, $storeid, 'map', $name);
    run_rbd_command($cmd, errmsg => "can't map rbd volume $name");

    return $kerneldev;
}

sub unmap_volume {
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    $name .= '@'.$snapname if $snapname;

    my $kerneldev = get_rbd_dev_path($scfg, $storeid, $name);

    if (-b $kerneldev) {
	my $cmd = $rbd_cmd->($scfg, $storeid, 'unmap', $kerneldev);
	run_rbd_command($cmd, errmsg => "can't unmap rbd device $kerneldev");
    }

    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    $class->map_volume($storeid, $scfg, $volname, $snapname) if $scfg->{krbd};

    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    $class->unmap_volume($storeid, $scfg, $volname, $snapname);

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

    return 1 if $running && !$scfg->{krbd}; # FIXME???

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = $rbd_cmd->($scfg, $storeid, 'resize', '--allow-shrink', '--size', ($size/1024/1024), $name);
    run_rbd_command($cmd, errmsg => "rbd resize '$volname' error");
    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'create', '--snap', $snap, $name);
    run_rbd_command($cmd, errmsg => "rbd snapshot '$volname' error");
    return undef;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'rollback', '--snap', $snap, $name);
    run_rbd_command($cmd, errmsg => "rbd snapshot $volname to '$snap' error");
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running && !$scfg->{krbd}; # FIXME: ????

    $class->deactivate_volume($storeid, $scfg, $volname, $snap, {});

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my (undef, undef, undef, $protected) = rbd_volume_info($scfg, $storeid, $name, $snap);
    if ($protected){
	my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'unprotect', $name, '--snap', $snap);
	run_rbd_command($cmd, errmsg => "rbd unprotect $name snap '$snap' error");
    }

    my $cmd = $rbd_cmd->($scfg, $storeid, 'snap', 'rm', '--snap', $snap, $name);

    run_rbd_command($cmd, errmsg => "rbd snapshot '$volname' error");

    return undef;
}

sub volume_snapshot_needs_fsfreeze {
    return 1;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

   my $features = {
	snapshot => { current => 1, snap => 1},
	clone => { base => 1, snap => 1},
	template => { current => 1},
	copy => { base => 1, current => 1, snap => 1},
	sparseinit => { base => 1, current => 1},
	rename => {current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) = $class->parse_volname($volname);

    my $key = undef;
    if ($snapname){
	$key = 'snap';
    } else {
	$key = $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

sub rename_volume {
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;

    my (
	undef,
	$source_image,
	$source_vmid,
	$base_name,
	$base_vmid,
	undef,
	$format
    ) = $class->parse_volname($source_volname);
    $target_volname = $class->find_free_diskname($storeid, $scfg, $target_vmid, $format)
	if !$target_volname;

    eval {
	my $cmd = $rbd_cmd->($scfg, $storeid, 'info', $target_volname);
	run_rbd_command($cmd, errmsg => "exist check",  quiet => 1);
    };
    die "target volume '${target_volname}' already exists\n" if !$@;

    my $cmd = $rbd_cmd->($scfg, $storeid, 'rename', $source_image, $target_volname);

    run_rbd_command(
	$cmd,
	errmsg => "could not rename image '${source_image}' to '${target_volname}'",
    );

    eval { $class->unmap_volume($storeid, $scfg, $source_volname); };
    warn $@ if $@;

    $base_name = $base_name ? "${base_name}/" : '';

    return "${storeid}:${base_name}${target_volname}";
}

1;
