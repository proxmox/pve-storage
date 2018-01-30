package PVE::Storage::DirPlugin;

use strict;
use warnings;
use Cwd;
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
	mkdir => {
	    description => "Create the directory if it doesn't exist.",
	    type => 'boolean',
	    default => 'yes',
	},
	is_mountpoint => {
	    description =>
		"Assume the given path is an externally managed mountpoint " .
		"and consider the storage offline if it is not mounted. ".
		"Using a boolean (yes/no) value serves as a shortcut to using the target path in this field.",
	    type => 'string',
	    default => 'no',
	},
	bwlimit => get_standard_option('bwlimit'),
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
	mkdir => { optional => 1 },
	is_mountpoint => { optional => 1 },
	bwlimit => { optional => 1 },
   };
}

# Storage implementation
#

# NOTE: should ProcFSTools::is_mounted accept an optional cache like this?
sub path_is_mounted {
    my ($mountpoint, $mountdata) = @_;

    $mountpoint = Cwd::realpath($mountpoint); # symlinks
    return 0 if !defined($mountpoint); # path does not exist

    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;
    return 1 if grep { $_->[1] eq $mountpoint } @$mountdata;
    return undef;
}

sub parse_is_mountpoint {
    my ($scfg) = @_;
    my $is_mp = $scfg->{is_mountpoint};
    return undef if !defined $is_mp;
    if (defined(my $bool = PVE::JSONSchema::parse_boolean($is_mp))) {
	return $bool ? $scfg->{path} : undef;
    }
    return $is_mp; # contains a path
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    if (defined(my $mp = parse_is_mountpoint($scfg))) {
	$cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	    if !$cache->{mountdata};

	return undef if !path_is_mounted($mp, $cache->{mountdata});
    }

    return $class->SUPER::status($storeid, $scfg, $cache);
}


sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};
    if (!defined($scfg->{mkdir}) || $scfg->{mkdir}) {
	mkpath $path;
    }

    my $mp = parse_is_mountpoint($scfg);
    if (defined($mp) && !path_is_mounted($mp, $cache->{mountdata})) {
	die "unable to activate storage '$storeid' - " .
	    "directory is expected to be a mount point but is not mounted: '$mp'\n";
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);    
}

sub check_config {
    my ($self, $sectionId, $config, $create, $skipSchemaCheck) = @_;
    my $opts = PVE::SectionConfig::check_config($self, $sectionId, $config, $create, $skipSchemaCheck);
    return $opts if !$create;
    if ($opts->{path} !~ m@^/[-/a-zA-Z0-9_.]+$@) {
	die "illegal path for directory storage: $opts->{path}\n";
    }
    return $opts;
}

1;
