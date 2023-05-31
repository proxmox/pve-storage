package PVE::Storage::DirPlugin;

use strict;
use warnings;

use Cwd;
use Encode qw(decode encode);
use File::Path;
use IO::File;
use POSIX;

use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

sub type {
    return 'dir';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1, snippets => 1, none => 1 },
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
	    description => "Create the directory if it doesn't exist and populate it with default sub-dirs."
		." NOTE: Deprecated, use the 'create-base-path' and 'create-subdirs' options instead.",
	    type => 'boolean',
	    default => 'yes',
	},
	'create-base-path' => {
	    description => "Create the base directory if it doesn't exist.",
	    type => 'boolean',
	    default => 'yes',
	},
	'create-subdirs' => {
	    description => "Populate the directory with the default structure.",
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
	'content-dirs' => { optional => 1 },
	nodes => { optional => 1 },
	shared => { optional => 1 },
	disable => { optional => 1 },
	maxfiles => { optional => 1 },
	'prune-backups' => { optional => 1 },
	'max-protected-backups' => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
	mkdir => { optional => 1 },
	'create-base-path' => { optional => 1 },
	'create-subdirs' => { optional => 1 },
	is_mountpoint => { optional => 1 },
	bwlimit => { optional => 1 },
	preallocation => { optional => 1 },
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

# FIXME move into 'get_volume_attribute' when removing 'get_volume_notes'
my $get_volume_notes_impl = sub {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($vtype) = $class->parse_volname($volname);
    return if $vtype ne 'backup';

    my $path = $class->filesystem_path($scfg, $volname);
    $path .= $class->SUPER::NOTES_EXT;

    if (-f $path) {
	my $data = PVE::Tools::file_get_contents($path);
	return eval { decode('UTF-8', $data, 1) } // $data;
    }

    return '';
};

# FIXME remove on the next APIAGE reset.
# Deprecated, use get_volume_attribute instead.
sub get_volume_notes {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    return $get_volume_notes_impl->($class, $scfg, $storeid, $volname, $timeout);
}

# FIXME move into 'update_volume_attribute' when removing 'update_volume_notes'
my $update_volume_notes_impl = sub {
    my ($class, $scfg, $storeid, $volname, $notes, $timeout) = @_;

    my ($vtype) = $class->parse_volname($volname);
    die "only backups can have notes\n" if $vtype ne 'backup';

    my $path = $class->filesystem_path($scfg, $volname);
    $path .= $class->SUPER::NOTES_EXT;

    if (defined($notes) && $notes ne '') {
	my $encoded = encode('UTF-8', $notes);
	PVE::Tools::file_set_contents($path, $encoded);
    } else {
	unlink $path or $! == ENOENT or die "could not delete notes - $!\n";
    }
    return;
};

# FIXME remove on the next APIAGE reset.
# Deprecated, use update_volume_attribute instead.
sub update_volume_notes {
    my ($class, $scfg, $storeid, $volname, $notes, $timeout) = @_;
    return $update_volume_notes_impl->($class, $scfg, $storeid, $volname, $notes, $timeout);
}

sub get_volume_attribute {
    my ($class, $scfg, $storeid, $volname, $attribute) = @_;

    if ($attribute eq 'notes') {
	return $get_volume_notes_impl->($class, $scfg, $storeid, $volname);
    }

    my ($vtype) = $class->parse_volname($volname);
    return if $vtype ne 'backup';

    if ($attribute eq 'protected') {
	my $path = $class->filesystem_path($scfg, $volname);
	return -e PVE::Storage::protection_file_path($path) ? 1 : 0;
    }

    return;
}

sub update_volume_attribute {
    my ($class, $scfg, $storeid, $volname, $attribute, $value) = @_;

    if ($attribute eq 'notes') {
	return $update_volume_notes_impl->($class, $scfg, $storeid, $volname, $value);
    }

    my ($vtype) = $class->parse_volname($volname);
    die "only backups support attribute '$attribute'\n" if $vtype ne 'backup';

    if ($attribute eq 'protected') {
	my $path = $class->filesystem_path($scfg, $volname);
	my $protection_path = PVE::Storage::protection_file_path($path);

	return if !((-e $protection_path) xor $value); # protection status already correct

	if ($value) {
	    my $fh = IO::File->new($protection_path, O_CREAT, 0644)
		or die "unable to create protection file '$protection_path' - $!\n";
	    close($fh);
	} else {
	    unlink $protection_path or $! == ENOENT
		or die "could not delete protection file '$protection_path' - $!\n";
	}

	return;
    }

    die "attribute '$attribute' is not supported for storage type '$scfg->{type}'\n";
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

    my $mp = parse_is_mountpoint($scfg);
    if (defined($mp) && !path_is_mounted($mp, $cache->{mountdata})) {
	die "unable to activate storage '$storeid' - " .
	    "directory is expected to be a mount point but is not mounted: '$mp'\n";
    }

    $class->config_aware_base_mkdir($scfg, $path);
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
