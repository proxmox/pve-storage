package PVE::Storage::SheepdogPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);


sub sheepdog_ls{
 my ($scfg, $storeid) = @_;

    my $portal = $scfg->{portal};
    my ($server, $port) = split(':', $portal);
    my $cmd = ['/usr/sbin/collie', 'vdi', 'list', '-a', $server ];
    my $list = {};


    run_command($cmd,outfunc => sub {
        my $line = shift;
        $line = trim($line);
	if( $line =~ /(vm-(\d+)-\S+)\s+(\d+)\s+([\.0-9]*)\s(\w+)\s+([\.0-9]*)\s(\w+)\W+([\.0-9]*)\s(\w+)\s+([\-0-9]*)\s([:0-9]*)\W+/ ) { 

	    my $image = $1;
	    my $owner = $2;
	    my $size = $4;

	    $list->{$storeid}->{$image} = {
		name => $image,
		size => $size,
		vmid => $owner
	    };

	    

	}
    });

    return $list;

}

# Configuration


sub type {
    return 'sheepdog';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}


sub options {
    return {
	portal => { fixed => 1 },
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

    my $portal = $scfg->{portal};
    
    my $path = "sheepdog:$portal:$name";

    return ($path, $vmid, $vtype);
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;
    my $portal = $scfg->{portal};
    my ($server, $port) = split(':', $portal);

    if (!$name) {
	my $sheepdog = sheepdog_ls($scfg, $storeid);

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($sheepdog->{$storeid}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;
    my $cmd = ['/usr/sbin/collie', 'vdi', 'create' , $name , $size.'KB', '-a', $server ];
    run_command($cmd, errmsg => "sheepdog create $name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $portal = $scfg->{portal};
    my ($server, $port) = split(':', $portal);

    my $cmd = ['/usr/sbin/collie', 'vdi', 'delete' , $volname, '-a', $server ];

    run_command($cmd, errmsg => "sheepdog delete $volname' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{sheepdog} = sheepdog_ls($scfg, $storeid) if !$cache->{sheepdog};
    my $res = [];

    if (my $dat = $cache->{sheepdog}->{$storeid}) {
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
