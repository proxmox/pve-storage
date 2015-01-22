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

sub options {
    return {
	path => { fixed => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
        maxfiles => { optional => 1 },
	content => { optional => 1 },
    };
}

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

# fixme: implement me

1;
