package PVE::Storage::RBDPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);


sub rbd_ls{
 my ($scfg, $storeid) = @_;

    my $rbdpool = $scfg->{pool};
    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $cmd = ['/usr/bin/rbd', '-p', $rbdpool, '-m', $monhost, '-n', "client.".$scfg->{username} ,'--keyfile', '/etc/pve/priv/ceph/'.$storeid.'.'.$scfg->{username}.'.key', '--auth_supported',$scfg->{authsupported}, 'ls' ];
    my $list = {};
    run_command($cmd, errfunc => sub {},outfunc => sub {
        my $line = shift;

        $line = trim($line);
        my ($image) = $line;
	
        $list->{$rbdpool}->{$image} = {
            name => $image,
            size => "",
        };

    });


    return $list;

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
    
    my $path = "rbd:$pool/$name:id=$username:auth_supported=$authsupported:keyfile=/etc/pve/priv/ceph/$storeid.$username.key:mon_host=$monhost";

    return ($path, $vmid, $vtype);
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;
    my $rbdpool = $scfg->{pool};
    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    if (!$name) {
	my $rdb = rbd_ls($scfg, $storeid);

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($rdb->{$rbdpool}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my $cmd = ['/usr/bin/rbd', '-p', $rbdpool, '-m', $monhost, '-n', "client.".$scfg->{username}, '--keyfile','/etc/pve/priv/ceph/'.$storeid.'.'.$scfg->{username}.'.key','--auth_supported', $scfg->{authsupported}, 'create', '--size', ($size/1024), $name  ];
    run_command($cmd, errmsg => "rbd create $name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $rbdpool = $scfg->{pool};
    my $monhost = $scfg->{monhost};
    $monhost =~ s/;/,/g;

    my $cmd = ['/usr/bin/rbd', '-p', $rbdpool, '-m', $monhost, '-n', "client.".$scfg->{username}, '--keyfile','/etc/pve/priv/ceph/'.$storeid.'.'.$scfg->{username}.'.key','--auth_supported',$scfg->{authsupported}, 'rm', $volname  ];
    run_command($cmd, errmsg => "rbd rm $volname' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{rbd} = rbd_ls($scfg, $storeid) if !$cache->{rbd};
    my $rbdpool = $scfg->{pool};
    my $res = [];

    if (my $dat = $cache->{rbd}->{$rbdpool}) {
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

            push @$res, $info;
        }
    }
    
   return $res;
}


sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 1;
    return ($total,$free,$used,$active);

    return undef;
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

1;
