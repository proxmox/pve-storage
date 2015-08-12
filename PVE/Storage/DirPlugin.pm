package PVE::Storage::DirPlugin;

use strict;
use warnings;
use File::Path;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

sub type {
    return 'dir';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1, none => 1 },
		     { images => 1,  rootdir => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1, subvol => 1 } , 'raw' ],
    };
}   

sub properties {
    return {
	path => {
	    description => "File system path.",
	    type => 'string', format => 'pve-storage-path',
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
        nodes => { optional => 1 },
	shared => { optional => 1 },
	disable => { optional => 1 },
        maxfiles => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
   };
}

# Storage implementation

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};
    mkpath $path;

    $class->SUPER::activate_storage($storeid, $scfg, $cache);    
}


1;
