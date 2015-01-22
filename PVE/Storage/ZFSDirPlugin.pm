package PVE::Storage::ZFSDirPlugin;

use strict;
use warnings;
use IO::File;
use POSIX;
use PVE::Tools qw(run_command);
use PVE::Storage::Plugin;


use base qw(PVE::Storage::Plugin);

sub type {
    return 'zfsdir';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1},
		     { images => 1 }],
    };
}   

sub properties {
    return {
	blocksize => {
	    description => "block size",
	    type => 'string',
	},
	sparse => {
	    description => "use sparse volumes",
	    type => 'boolean',
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	pool => { fixed => 1 },
	blocksize => { optional => 1 },
	sparse => { optional => 1 },
	nodes => { optional => 1 },
	disable => { optional => 1 },
        maxfiles => { optional => 1 },
	content => { optional => 1 },
    };
}

# static zfs helper methods

sub zfs_parse_size {
    my ($text) = @_;

    return 0 if !$text;
    
    if ($text =~ m/^(\d+(\.\d+)?)([TGMK])?$/) {

	my ($size, $reminder, $unit) = ($1, $2, $3);
	
	if ($unit) {
	    if ($unit eq 'K') {
		$size *= 1024;
	    } elsif ($unit eq 'M') {
		$size *= 1024*1024;
	    } elsif ($unit eq 'G') {
		$size *= 1024*1024*1024;
	    } elsif ($unit eq 'T') {
		$size *= 1024*1024*1024*1024;
	    } else {
		die "got unknown zfs size unit '$unit'\n";
	    }
	}

	if ($reminder) {
	    $size = ceil($size);
	}
	
	return $size;
    
    }

    warn "unable to parse zfs size '$text'\n";

    return 0;
}

sub zfs_parse_zvol_list {
    my ($text) = @_;

    my $list = ();

    return $list if !$text;

    my @lines = split /\n/, $text;
    foreach my $line (@lines) {
	if ($line =~ /^(.+)\s+([a-zA-Z0-9\.]+|\-)\s+(.+)$/) {
	    my $zvol = {};
	    my @parts = split /\//, $1;
	    my $name = pop @parts;
	    my $pool = join('/', @parts);

	    if ($pool !~ /^rpool$/) {
		next unless $name =~ m!^(\w+)-(\d+)-(\w+)-(\d+)$!;
		$name = $pool . '/' . $name;
	    } else {
		next;
	    }

	    $zvol->{pool} = $pool;
	    $zvol->{name} = $name;
	    $zvol->{size} = zfs_parse_size($2);
	    if ($3 !~ /^-$/) {
		$zvol->{origin} = $3;
	    }
	    push @$list, $zvol;
	}
    }

    return $list;
}

# virtual zfs methods (subclass can overwrite them)

sub zfs_request {
    my ($class, $scfg, $timeout, $method, @params) = @_;

    $timeout = 5 if !$timeout;

    my $cmd = [];

    if ($method eq 'zpool_list') {
	push @$cmd = 'zpool', 'list';
    } else {
	push @$cmd, 'zfs', $method;
    }

    push @$cmd, @params;
 
    my $msg = '';

    my $output = sub {
        my $line = shift;
        $msg .= "$line\n";
    };

    run_command($cmd, outfunc => $output, timeout => $timeout);

    return $msg;
}

sub zfs_get_pool_stats {
    my ($class, $scfg) = @_;

    my $available = 0;
    my $used = 0;

    my $text = $class->zfs_request($scfg, undef, 'get', '-o', 'value', '-Hp',
               'available,used', $scfg->{pool});

    my @lines = split /\n/, $text;

    if($lines[0] =~ /^(\d+)$/) {
	$available = $1;
    }

    if($lines[1] =~ /^(\d+)$/) {
	$used = $1;
    }

    return ($available, $used);
}

sub zfs_get_zvol_size {
    my ($class, $scfg, $zvol) = @_;

    my $text = $class->zfs_request($scfg, undef, 'get', '-Hp', 'volsize', "$scfg->{pool}/$zvol");

    if ($text =~ /volsize\s(\d+)/) {
	return $1;
    }

    die "Could not get zvol size";
}

sub zfs_create_zvol {
    my ($class, $scfg, $zvol, $size) = @_;
    
    my $cmd = ['create'];

    push @$cmd, '-s' if $scfg->{sparse};

    push @$cmd, '-b', $scfg->{blocksize} if $scfg->{blocksize};

    push @$cmd, '-V', "${size}k", "$scfg->{pool}/$zvol";

    $class->zfs_request($scfg, undef, @$cmd);
}

sub zfs_delete_zvol {
    my ($class, $scfg, $zvol) = @_;

    $class->zfs_request($scfg, undef, 'destroy', '-r', "$scfg->{pool}/$zvol");
}

sub zfs_list_zvol {
    my ($class, $scfg) = @_;

    my $text = $class->zfs_request($scfg, 10, 'list', '-o', 'name,volsize,origin', '-t', 'volume', '-Hr');
    my $zvols = zfs_parse_zvol_list($text);
    return undef if !$zvols;

    my $list = ();
    foreach my $zvol (@$zvols) {
	my @values = split('/', $zvol->{name});

	my $image = pop @values;
	my $pool = join('/', @values);

	next if $image !~ m/^((vm|base)-(\d+)-\S+)$/;
	my $owner = $3;

	my $parent = $zvol->{origin};
	if($zvol->{origin} && $zvol->{origin} =~ m/^$scfg->{pool}\/(\S+)$/){
	    $parent = $1;
	}

	$list->{$pool}->{$image} = {
	    name => $image,
	    size => $zvol->{size},
	    parent => $parent,
	    format => 'raw',
            vmid => $owner
        };
    }

    return $list;
}

sub zfs_find_free_diskname {
    my ($class, $storeid, $scfg, $vmid) = @_;

    my $name = undef;
    my $volumes = $class->zfs_list_zvol($scfg);

    my $disk_ids = {};
    my $dat = $volumes->{$scfg->{pool}};

    foreach my $image (keys %$dat) {
        my $volname = $dat->{$image}->{name};
        if ($volname =~ m/(vm|base)-$vmid-disk-(\d+)/){
            $disk_ids->{$2} = 1;
        }
    }

    for (my $i = 1; $i < 100; $i++) {
        if (!$disk_ids->{$i}) {
            return "vm-$vmid-disk-$i";
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n";
}

# fixme: implement me

1;
