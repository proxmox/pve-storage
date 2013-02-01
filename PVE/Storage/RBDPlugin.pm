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

my $rbd_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $cmd = ['/usr/bin/rbd', '-p', $scfg->{pool}, '-m', $monhost, '-n', 
	       "client.$scfg->{username}", 
	       '--keyring', "/etc/pve/priv/ceph/${storeid}.keyring", 
	       '--auth_supported', $scfg->{authsupported}, $op];

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

my $rados_cmd = sub {
    my ($scfg, $storeid, $op, @options) = @_;

    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $cmd = ['/usr/bin/rados', '-p', $scfg->{pool}, '-m', $monhost, '-n', 
	       "client.$scfg->{username}", 
	       '--keyring', "/etc/pve/priv/ceph/${storeid}.keyring", 
	       '--auth_supported', $scfg->{authsupported}, $op];

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

sub rbd_ls {
    my ($scfg, $storeid) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'ls', '-l');

    my $list = {};

    my $parser = sub {
	my $line = shift;

	if ($line =~  m/^(vm-(\d+)-disk-\d+)\s+(\d+)(M|G|T)\s((\S+)\/(vm-\d+-\S+@\S+))?/) {
	    my ($image, $owner, $size, $unit, $parent) = ($1, $2, $3, $4, $7);

	    $list->{$scfg->{pool}}->{$image} = {
		name => $image,
		size => $size*rbd_unittobytes()->{$unit},
		parent => $parent,
		vmid => $owner
	    };
	}
    };

    eval {
	run_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);
    };
    my $err = $@;

    die $err if $err && $err !~ m/doesn't contain rbd images/ ;
  
    return $list;
}

sub rbd_volume_info {
    my ($scfg, $storeid, $volname) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'info', $volname);
    my $size = undef;
    my $parent = undef;

    my $parser = sub {
	my $line = shift;

	if ($line =~ m/size (\d+) (M|G|T)B in (\d+) objects/) {
	    $size = $1 * rbd_unittobytes()->{$2} if ($1);
	} elsif ($line =~ m/parent:\s(\S+)\/(\S+)/) {
	    $parent = $2;
	}
    };

    run_command($cmd, errmsg => "rbd error", errfunc => sub {}, outfunc => $parser);

    return ($size, $parent);
}

sub addslashes {
    my $text = shift;
    $text =~ s/;/\\;/g;
    $text =~ s/:/\\:/g;
    return $text;
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
	content => [ {images => 1}, { images => 1 }],
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
    };
}

sub options {
    return {
	nodes => { optional => 1 },
	disable => { optional => 1 },
	monhost => { fixed => 1 },
	pool => { fixed => 1 },
	username => { fixed => 1 },
	authsupported => { fixed => 1 },
	content => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
	return ('images', $1, $2);
    }

    die "unable to parse rbd volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $monhost = addslashes($scfg->{monhost});
    my $pool = $scfg->{pool};
    my $username = $scfg->{username};
    my $authsupported = addslashes($scfg->{authsupported});
    
    my $path = "rbd:$pool/$name:id=$username:auth_supported=$authsupported:keyring=/etc/pve/priv/ceph/$storeid.keyring:mon_host=$monhost";

    return ($path, $vmid, $vtype);
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "not implemented";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid) = @_;

    die "not implemented";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    if (!$name) {
	my $rdb = rbd_ls($scfg, $storeid);

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($rdb->{$scfg->{pool}}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'create', '--format' , 2, '--size', ($size/1024), $name);
    run_command($cmd, errmsg => "rbd create $name' error", errfunc => sub {});

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'purge',  $volname);
    run_command($cmd, errmsg => "rbd snap purge $volname' error", outfunc => sub {}, errfunc => sub {});

    $cmd = &$rbd_cmd($scfg, $storeid, 'rm', $volname);
    run_command($cmd, errmsg => "rbd rm $volname' error", outfunc => sub {}, errfunc => sub {});

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{rbd} = rbd_ls($scfg, $storeid) if !$cache->{rbd};

    my $res = [];

    if (my $dat = $cache->{rbd}->{$scfg->{pool}}) {
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
	run_command($cmd, errmsg => "rados error", errfunc => sub {}, outfunc => $parser);
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
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($size, undef) = rbd_volume_info($scfg, $storeid, $volname);
    return $size;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    return 1 if $running;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'resize', '--size', ($size/1024/1024), $volname);
    run_command($cmd, errmsg => "rbd resize $volname' error", errfunc => sub {});
    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'create', '--snap', $snap, $volname);
    run_command($cmd, errmsg => "rbd snapshot $volname' error", errfunc => sub {});
    return undef;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'rollback', '--snap', $snap, $volname);
    run_command($cmd, errmsg => "rbd snapshot $volname to $snap' error", errfunc => sub {});
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running;

    my $cmd = &$rbd_cmd($scfg, $storeid, 'snap', 'rm', '--snap', $snap, $volname);
    run_command($cmd, errmsg => "rbd snapshot $volname' error", errfunc => sub {});
    return undef;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

   my $features = {
        snapshot => { current => 1, snap => 1},
        clone => { snap => 1},
    };

    my $snap = $snapname ? 'snap' : 'current';
    return 1 if $features->{$feature}->{$snap};

    return undef;
}

1;
