package PVE::Storage::BTRFSPlugin;

use strict;
use warnings;

use base qw(PVE::Storage::Plugin);

use Fcntl qw(S_ISDIR O_WRONLY O_CREAT O_EXCL);
use File::Basename qw(basename dirname);
use File::Path qw(mkpath);
use IO::Dir;
use POSIX qw(EEXIST);

use PVE::Tools qw(run_command dir_glob_foreach);

use PVE::Storage::DirPlugin;

use constant {
    BTRFS_FIRST_FREE_OBJECTID => 256,
    FS_NOCOW_FL => 0x00800000,
    FS_IOC_GETFLAGS => 0x40086602,
    FS_IOC_SETFLAGS => 0x80086601,
    BTRFS_MAGIC => 0x9123683e,
};

# Configuration (similar to DirPlugin)

sub type {
    return 'btrfs';
}

sub plugindata {
    return {
	content => [
	    {
		images => 1,
		rootdir => 1,
		vztmpl => 1,
		iso => 1,
		backup => 1,
		snippets => 1,
		none => 1,
	    },
	    { images => 1, rootdir => 1 },
	],
	format => [ { raw => 1, subvol => 1 }, 'raw', ],
    };
}

sub properties {
    return {
	nocow => {
	    description => "Set the NOCOW flag on files."
		. " Disables data checksumming and causes data errors to be unrecoverable from"
		. " while allowing direct I/O. Only use this if data does not need to be any more"
		. " safe than on a single ext4 formatted disk with no underlying raid system.",
	    type => 'boolean',
	    default => 0,
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
	'prune-backups'=> { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
	is_mountpoint => { optional => 1 },
	nocow => { optional => 1 },
	mkdir => { optional => 1 },
	# TODO: The new variant of mkdir with  `populate` vs `create`...
    };
}

# Storage implementation
#
# We use the same volume names are directory plugins, but map *raw* disk image file names into a
# subdirectory.
#
# `vm-VMID-disk-ID.raw`
#   -> `images/VMID/vm-VMID-disk-ID/disk.raw`
#   where the `vm-VMID-disk-ID/` subdirectory is a btrfs subvolume

# Reuse `DirPlugin`'s `check_config`. This simply checks for invalid paths.
sub check_config {
    my ($self, $sectionId, $config, $create, $skipSchemaCheck) = @_;
    return PVE::Storage::DirPlugin::check_config($self, $sectionId, $config, $create, $skipSchemaCheck);
}

my sub getfsmagic($) {
    my ($path) = @_;
    # The field type sizes in `struct statfs` are defined in a rather annoying way, and we only
    # need the first field, which is a `long` for our supported platforms.
    # Should be moved to pve-rs, so this can be the problem of the `libc` crate ;-)
    # Just round up and extract what we need:
    my $buf = pack('x160');
    if (0 != syscall(&PVE::Syscall::SYS_statfs, $path, $buf)) {
	die "statfs on '$path' failed - $!\n";
    }

    return unpack('L!', $buf);
}

my sub assert_btrfs($) {
    my ($path) = @_;
    die "'$path' is not a btrfs file system\n"
	if getfsmagic($path) != BTRFS_MAGIC;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};
    if (!defined($scfg->{mkdir}) || $scfg->{mkdir}) {
	mkpath $path;
    }

    my $mp = PVE::Storage::DirPlugin::parse_is_mountpoint($scfg);
    if (defined($mp) && !PVE::Storage::DirPlugin::path_is_mounted($mp, $cache->{mountdata})) {
	die "unable to activate storage '$storeid' - directory is expected to be a mount point but"
	." is not mounted: '$mp'\n";
    }

    assert_btrfs($path); # only assert this stuff now, ensures $path is there and better UX

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;
    return PVE::Storage::DirPlugin::status($class, $storeid, $scfg, $cache);
}

# TODO: sub get_volume_notes {}

# TODO: sub update_volume_notes {}

# croak would not include the caller from within this module
sub __error {
    my ($msg) = @_;
    my (undef, $f, $n) = caller(1);
    die "$msg at $f: $n\n";
}

# Given a name (eg. `vm-VMID-disk-ID.raw`), take the part up to the format suffix as the name of
# the subdirectory (subvolume).
sub raw_name_to_dir($) {
    my ($raw) = @_;

    # For the subvolume directory Strip the `.<format>` suffix:
    if ($raw =~ /^(.*)\.raw$/) {
	return $1;
    }

    __error "internal error: bad disk name: $raw";
}

sub raw_file_to_subvol($) {
    my ($file) = @_;

    if ($file =~ m|^(.*)/disk\.raw$|) {
	return "$1";
    }

    __error "internal error: bad raw path: $file";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    my $path = $class->get_subdir($scfg, $vtype);

    $path .= "/$vmid" if $vtype eq 'images';

    if (defined($format) && $format eq 'raw') {
	my $dir = raw_name_to_dir($name);
	if ($snapname) {
	    $dir .= "\@$snapname";
	}
	$path .= "/$dir/disk.raw";
    } elsif (defined($format) && $format eq 'subvol') {
	$path .= "/$name";
	if ($snapname) {
	    $path .= "\@$snapname";
	}
    } else {
	$path .= "/$name";
    }

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub btrfs_cmd {
    my ($class, $cmd, $outfunc) = @_;

    my $msg = '';
    my $func;
    if (defined($outfunc)) {
	$func = sub {
	    my $part = &$outfunc(@_);
	    $msg .= $part if defined($part);
	};
    } else {
	$func = sub { $msg .= "$_[0]\n" };
    }
    run_command(['btrfs', '-q', @$cmd], errmsg => 'btrfs error', outfunc => $func);

    return $msg;
}

sub btrfs_get_subvol_id {
    my ($class, $path) = @_;
    my $info = $class->btrfs_cmd(['subvolume', 'show', '--', $path]);
    if ($info !~ /^\s*(?:Object|Subvolume) ID:\s*(\d+)$/m) {
	die "failed to get btrfs subvolume ID from: $info\n";
    }
    return $1;
}

my sub chattr : prototype($$$) {
    my ($fh, $mask, $xor) = @_;

    my $flags = pack('L!', 0);
    ioctl($fh, FS_IOC_GETFLAGS, $flags) or die "FS_IOC_GETFLAGS failed - $!\n";
    $flags = pack('L!', (unpack('L!', $flags) & $mask) ^ $xor);
    ioctl($fh, FS_IOC_SETFLAGS, $flags) or die "FS_IOC_SETFLAGS failed - $!\n";
    return 1;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
	$class->parse_volname($volname);

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    # If we're not working with a 'raw' file, which is the only thing that's "different" for btrfs,
    # or a subvolume, we forward to the DirPlugin
    if ($format ne 'raw' && $format ne 'subvol') {
	return PVE::Storage::DirPlugin::create_base(@_);
    }

    my $path = $class->filesystem_path($scfg, $volname);
    my $newvolname = $basename ? "$basevmid/$basename/$vmid/$newname" : "$vmid/$newname";
    my $newpath = $class->filesystem_path($scfg, $newvolname);

    my $subvol = $path;
    my $newsubvol = $newpath;
    if ($format eq 'raw') {
	$subvol = raw_file_to_subvol($subvol);
	$newsubvol = raw_file_to_subvol($newsubvol);
    }

    rename($subvol, $newsubvol)
	|| die "rename '$subvol' to '$newsubvol' failed - $!\n";
    eval { $class->btrfs_cmd(['property', 'set', $newsubvol, 'ro', 'true']) };
    warn $@ if $@;

    return $newvolname;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    my ($vtype, $basename, $basevmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    # If we're not working with a 'raw' file, which is the only thing that's "different" for btrfs,
    # or a subvolume, we forward to the DirPlugin
    if ($format ne 'raw' && $format ne 'subvol') {
	return PVE::Storage::DirPlugin::clone_image(@_);
    }

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";
    mkpath $imagedir;

    my $path = $class->filesystem_path($scfg, $volname);
    my $newname = $class->find_free_diskname($storeid, $scfg, $vmid, $format, 1);

    # For btrfs subvolumes we don't actually need the "link":
    #my $newvolname = "$basevmid/$basename/$vmid/$newname";
    my $newvolname = "$vmid/$newname";
    my $newpath = $class->filesystem_path($scfg, $newvolname);

    my $subvol = $path;
    my $newsubvol = $newpath;
    if ($format eq 'raw') {
	$subvol = raw_file_to_subvol($subvol);
	$newsubvol = raw_file_to_subvol($newsubvol);
    }

    $class->btrfs_cmd(['subvolume', 'snapshot', '--', $subvol, $newsubvol]);

    return $newvolname;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    if ($fmt ne 'raw' && $fmt ne 'subvol') {
	return $class->SUPER::alloc_image($storeid, $scfg, $vmid, $fmt, $name, $size);
    }

    # From Plugin.pm:

    my $imagedir = $class->get_subdir($scfg, 'images') . "/$vmid";

    mkpath $imagedir;

    $name = $class->find_free_diskname($storeid, $scfg, $vmid, $fmt, 1) if !$name;

    my (undef, $tmpfmt) = PVE::Storage::Plugin::parse_name_dir($name);

    die "illegal name '$name' - wrong extension for format ('$tmpfmt != '$fmt')\n"
	if $tmpfmt ne $fmt;

    # End copy from Plugin.pm

    my $subvol = "$imagedir/$name";
    # .raw is not part of the directory name
    $subvol =~ s/\.raw$//;

    die "disk image '$subvol' already exists\n" if -e $subvol;

    my $path;
    if ($fmt eq 'raw') {
	$path = "$subvol/disk.raw";
    }

    if ($fmt eq 'subvol' && !!$size) {
	# NOTE: `btrfs send/recv` actually drops quota information so supporting subvolumes with
	# quotas doesn't play nice with send/recv.
	die "btrfs quotas are currently not supported, use an unsized subvolume or a raw file\n";
    }

    $class->btrfs_cmd(['subvolume', 'create', '--', $subvol]);

    eval {
	if ($fmt eq 'subvol') {
	    # Nothing to do for now...

	    # This is how we *would* do it:
	    # # Use the subvol's default 0/$id qgroup
	    # eval {
	    #     # This call should happen at storage creation instead and therefore governed by a
	    #     # configuration option!
	    #     # $class->btrfs_cmd(['quota', 'enable', $subvol]);
	    #     my $id = $class->btrfs_get_subvol_id($subvol);
	    #     $class->btrfs_cmd(['qgroup', 'limit', "${size}k", "0/$id", $subvol]);
	    # };
	} elsif ($fmt eq 'raw') {
	    sysopen my $fh, $path, O_WRONLY | O_CREAT | O_EXCL
		or die "failed to create raw file '$path' - $!\n";
	    chattr($fh, ~FS_NOCOW_FL, FS_NOCOW_FL) if $scfg->{nocow};
	    truncate($fh, $size * 1024)
		or die "failed to set file size for '$path' - $!\n";
	    close($fh);
	} else {
	    die "internal format error (format = $fmt)\n";
	}
    };

    if (my $err = $@) {
	eval { $class->btrfs_cmd(['subvolume', 'delete', '--', $subvol]); };
	warn $@ if $@;
	die $err;
    }

    return "$vmid/$name";
}

# Same as btrfsprogs does:
my sub path_is_subvolume : prototype($) {
    my ($path) = @_;
    my @stat = stat($path)
	or die "stat failed on '$path' - $!\n";
    my ($ino, $mode) = @stat[1, 2];
    return S_ISDIR($mode) && $ino == BTRFS_FIRST_FREE_OBJECTID;
}

my $BTRFS_VOL_REGEX = qr/((?:vm|base|subvol)-\d+-disk-\d+(?:\.subvol)?)(?:\@(\S+))$/;

# Calls `$code->($volume, $name, $snapshot)` for each subvol in a directory matching our volume
# regex.
my sub foreach_subvol : prototype($$) {
    my ($dir, $code) = @_;

    dir_glob_foreach($dir, $BTRFS_VOL_REGEX, sub {
	my ($volume, $name, $snapshot) = ($1, $2, $3);
	return if !path_is_subvolume("$dir/$volume");
	$code->($volume, $name, $snapshot);
    })
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $_format) = @_;

    my (undef, undef, $vmid, undef, undef, undef, $format) =
	$class->parse_volname($volname);

    if (!defined($format) || ($format ne 'subvol' && $format ne 'raw')) {
	return $class->SUPER::free_image($storeid, $scfg, $volname, $isBase, $_format);
    }

    my $path = $class->filesystem_path($scfg, $volname);

    my $subvol = $path;
    if ($format eq 'raw') {
	$subvol = raw_file_to_subvol($path);
    }

    my $dir = dirname($subvol);
    my $basename = basename($subvol);
    my @snapshot_vols;
    foreach_subvol($dir, sub {
	my ($volume, $name, $snapshot) = @_;
	return if $name ne $basename;
	return if !defined $snapshot;
	push @snapshot_vols, "$dir/$volume";
    });

    $class->btrfs_cmd(['subvolume', 'delete', '--', @snapshot_vols, $subvol]);
    # try to cleanup directory to not clutter storage with empty $vmid dirs if
    # all images from a guest got deleted
    rmdir($dir);

    return undef;
}

# Currently not used because quotas clash with send/recv.
# my sub btrfs_subvol_quota {
#     my ($class, $path) = @_;
#     my $id = '0/' . $class->btrfs_get_subvol_id($path);
#     my $search = qr/^\Q$id\E\s+(\d)+\s+\d+\s+(\d+)\s*$/;
#     my ($used, $size);
#     $class->btrfs_cmd(['qgroup', 'show', '--raw', '-rf', '--', $path], sub {
# 	return if defined($size);
# 	if ($_[0] =~ $search) {
# 	    ($used, $size) = ($1, $2);
# 	}
#     });
#     if (!defined($size)) {
# 	# syslog should include more information:
# 	syslog('err', "failed to get subvolume size for: $path (id $id)");
# 	# UI should only see the last path component:
# 	$path =~ s|^.*/||;
# 	die "failed to get subvolume size for $path\n";
#     }
#     return wantarray ? ($used, $size) : $size;
# }

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my $path = $class->filesystem_path($scfg, $volname);

    my $format = ($class->parse_volname($volname))[6];

    if (defined($format) && $format eq 'subvol') {
	my $ctime = (stat($path))[10];
	my ($used, $size) = (0, 0);
	#my ($used, $size) = btrfs_subvol_quota($class, $path); # uses wantarray
	return wantarray ? ($size, 'subvol', $used, undef, $ctime) : 1;
    }

    return PVE::Storage::Plugin::file_size_info($path, $timeout);
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my $format = ($class->parse_volname($volname))[6];
    if ($format eq 'subvol') {
	my $path = $class->filesystem_path($scfg, $volname);
	my $id = '0/' . $class->btrfs_get_subvol_id($path);
	$class->btrfs_cmd(['qgroup', 'limit', '--', "${size}k", "0/$id", $path]);
	return undef;
    }

    return PVE::Storage::Plugin::volume_resize(@_);
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($name, $vmid, $format) = ($class->parse_volname($volname))[1,2,6];
    if ($format ne 'subvol' && $format ne 'raw') {
	return PVE::Storage::Plugin::volume_snapshot(@_);
    }

    my $path = $class->filesystem_path($scfg, $volname);
    my $snap_path = $class->filesystem_path($scfg, $volname, $snap);

    if ($format eq 'raw') {
	$path = raw_file_to_subvol($path);
	$snap_path = raw_file_to_subvol($snap_path);
    }

    my $snapshot_dir = $class->get_subdir($scfg, 'images') . "/$vmid";
    mkpath $snapshot_dir;

    $class->btrfs_cmd(['subvolume', 'snapshot', '-r', '--', $path, $snap_path]);
    return undef;
}

sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap) = @_; 

    return 1; 
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($name, $format) = ($class->parse_volname($volname))[1,6];

    if ($format ne 'subvol' && $format ne 'raw') {
	return PVE::Storage::Plugin::volume_snapshot_rollback(@_);
    }

    my $path = $class->filesystem_path($scfg, $volname);
    my $snap_path = $class->filesystem_path($scfg, $volname, $snap);

    if ($format eq 'raw') {
	$path = raw_file_to_subvol($path);
	$snap_path = raw_file_to_subvol($snap_path);
    }

    # Simple version would be:
    #   rename old to temp
    #   create new
    #   on error rename temp back
    # But for atomicity in case the rename after create-failure *also* fails, we create the new
    # subvol first, then use RENAME_EXCHANGE, 
    my $tmp_path = "$path.tmp.$$";
    $class->btrfs_cmd(['subvolume', 'snapshot', '--', $snap_path, $tmp_path]);
    # The paths are absolute, so pass -1 as file descriptors.
    my $ok = PVE::Tools::renameat2(-1, $tmp_path, -1, $path, &PVE::Tools::RENAME_EXCHANGE);

    eval { $class->btrfs_cmd(['subvolume', 'delete', '--', $tmp_path]) };
    warn "failed to remove '$tmp_path' subvolume: $@" if $@;

    if (!$ok) {
	die "failed to rotate '$tmp_path' into place at '$path' - $!\n";
    }

    return undef;
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($name, $vmid, $format) = ($class->parse_volname($volname))[1,2,6];

    if ($format ne 'subvol' && $format ne 'raw') {
	return PVE::Storage::Plugin::volume_snapshot_delete(@_);
    }

    my $path = $class->filesystem_path($scfg, $volname, $snap);

    if ($format eq 'raw') {
	$path = raw_file_to_subvol($path);
    }

    $class->btrfs_cmd(['subvolume', 'delete', '--', $path]);

    return undef;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => {
	    current => { qcow2 => 1, raw => 1, subvol => 1 },
	    snap => { qcow2 => 1, raw => 1, subvol => 1 }
	},
	clone => {
	    base => { qcow2 => 1, raw => 1, subvol => 1, vmdk => 1 },
	    current => { raw => 1 },
	    snap => { raw => 1 },
	},
	template => { current => { qcow2 => 1, raw => 1, vmdk => 1, subvol => 1 } },
	copy => {
	    base => { qcow2 => 1, raw => 1, subvol => 1, vmdk => 1 },
	    current => { qcow2 => 1, raw => 1, subvol => 1, vmdk => 1 },
	    snap => { qcow2 => 1, raw => 1, subvol => 1 },
	},
	sparseinit => { base => {qcow2 => 1, raw => 1, vmdk => 1 },
			current => {qcow2 => 1, raw => 1, vmdk => 1 } },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
	$class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    } else {
        $key =  $isBase ? 'base' : 'current';
    }

    return 1 if defined($features->{$feature}->{$key}->{$format});

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;
    my $imagedir = $class->get_subdir($scfg, 'images');

    my $res = [];

    # Copied from Plugin.pm, with file_size_info calls adapted:
    foreach my $fn (<$imagedir/[0-9][0-9]*/*>) {
	# different to in Plugin.pm the regex below also excludes '@' as valid file name
	next if $fn !~ m@^(/.+/(\d+)/([^/\@.]+(?:\.(qcow2|vmdk|subvol))?))$@;
	$fn = $1; # untaint

	my $owner = $2;
	my $name = $3;
	my $ext = $4;

	next if !$vollist && defined($vmid) && ($owner ne $vmid);

	my $volid = "$storeid:$owner/$name";
	my ($size, $format, $used, $parent, $ctime);

	if (!$ext) { # raw
	    $volid .= '.raw';
	    ($size, $format, $used, $parent, $ctime) = PVE::Storage::Plugin::file_size_info("$fn/disk.raw");
	} elsif ($ext eq 'subvol') {
	    ($used, $size) = (0, 0);
	    #($used, $size) = btrfs_subvol_quota($class, $fn);
	    $format = 'subvol';
	} else {
	    ($size, $format, $used, $parent, $ctime) = PVE::Storage::Plugin::file_size_info($fn);
	}
	next if !($format && defined($size));

	if ($vollist) {
	    next if ! grep { $_ eq $volid } @$vollist;
	}

	my $info = {
	    volid => $volid, format => $format,
	    size => $size, vmid => $owner, used => $used, parent => $parent,
	};

        $info->{ctime} = $ctime if $ctime;

        push @$res, $info;
    }

    return $res;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    # We can do whatever `DirPlugin` can do.
    my @result = PVE::Storage::Plugin::volume_export_formats(@_);

    # `btrfs send` only works on snapshots:
    return @result if !defined $snapshot;

    # Incremental stream with snapshots is only supported if the snapshots are listed (new api):
    return @result if defined($base_snapshot) && $with_snapshots && ref($with_snapshots) ne 'ARRAY';

    # Otherwise we do also support `with_snapshots`.

    # Finally, `btrfs send` only works on formats where we actually use btrfs subvolumes:
    my $format = ($class->parse_volname($volname))[6];
    return @result if $format ne 'raw' && $format ne 'subvol';

    return ('btrfs', @result);
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    # Same as export-formats, beware the parameter order:
    return volume_export_formats(
	$class,
	$scfg,
	$storeid,
	$volname,
	$snapshot,
	$base_snapshot,
	$with_snapshots,
    );
}

sub volume_export {
    my (
	$class,
	$scfg,
	$storeid,
	$fh,
	$volname,
	$format,
	$snapshot,
	$base_snapshot,
	$with_snapshots,
    ) = @_;

    if ($format ne 'btrfs') {
	return PVE::Storage::Plugin::volume_export(@_);
    }

    die "format 'btrfs' only works on snapshots\n"
	if !defined $snapshot;

    die "'btrfs' format in incremental mode requires snapshots to be listed explicitly\n"
	if defined($base_snapshot) && $with_snapshots && ref($with_snapshots) ne 'ARRAY';

    my $volume_format = ($class->parse_volname($volname))[6];

    die "btrfs-sending volumes of type $volume_format ('$volname') is not supported\n"
	if $volume_format ne 'raw' && $volume_format ne 'subvol';

    my $path = $class->path($scfg, $volname, $storeid);

    if ($volume_format eq 'raw') {
	$path = raw_file_to_subvol($path);
    }

    my $cmd = ['btrfs', '-q', 'send', '-e'];
    if ($base_snapshot) {
	my $base = $class->path($scfg, $volname, $storeid, $base_snapshot);
	if ($volume_format eq 'raw') {
	    $base = raw_file_to_subvol($base);
	}
	push @$cmd, '-p', $base;
    }
    push @$cmd, '--';
    if (ref($with_snapshots) eq 'ARRAY') {
	push @$cmd, (map { "$path\@$_" } ($with_snapshots // [])->@*), $path;
    } else {
	dir_glob_foreach(dirname($path), $BTRFS_VOL_REGEX, sub {
	    push @$cmd, "$path\@$_[2]" if !(defined($snapshot) && $_[2] eq $snapshot);
	});
    }
    $path .= "\@$snapshot" if defined($snapshot);
    push @$cmd, $path;

    run_command($cmd, output => '>&'.fileno($fh));
    return;
}

sub volume_import {
    my (
	$class,
	$scfg,
	$storeid,
	$fh,
	$volname,
	$format,
	$snapshot,
	$base_snapshot,
	$with_snapshots,
	$allow_rename,
    ) = @_;

    if ($format ne 'btrfs') {
	return PVE::Storage::Plugin::volume_import(@_);
    }

    die "format 'btrfs' only works on snapshots\n"
	if !defined $snapshot;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $volume_format) =
	$class->parse_volname($volname);

    die "btrfs-receiving volumes of type $volume_format ('$volname') is not supported\n"
	if $volume_format ne 'raw' && $volume_format ne 'subvol';

    if (defined($base_snapshot)) {
	my $path = $class->path($scfg, $volname, $storeid, $base_snapshot);
	die "base snapshot '$base_snapshot' not found - no such directory '$path'\n"
	    if !path_is_subvolume($path);
    }

    my $destination = $class->filesystem_path($scfg, $volname);
    if ($volume_format eq 'raw') {
	$destination = raw_file_to_subvol($destination);
    }

    if (!defined($base_snapshot) && -e $destination) {
	die "volume $volname already exists\n" if !$allow_rename;
	$volname = $class->find_free_diskname($storeid, $scfg, $vmid, $volume_format, 1);
    }

    my $imagedir = $class->get_subdir($scfg, $vtype);
    $imagedir .= "/$vmid" if $vtype eq 'images';

    my $tmppath = "$imagedir/recv.$vmid.tmp";
    mkdir($imagedir); # FIXME: if $scfg->{mkdir};
    if (!mkdir($tmppath)) {
	die "temp receive directory already exists at '$tmppath', incomplete concurrent import?\n"
	    if $! == EEXIST;
	die "failed to create temporary receive directory at '$tmppath' - $!\n";
    }

    my $dh = IO::Dir->new($tmppath)
	or die "failed to open temporary receive directory '$tmppath' - $!\n";
    eval {
	run_command(['btrfs', '-q', 'receive', '-e', '--', $tmppath], input => '<&'.fileno($fh));

	# Analyze the received subvolumes;
	my ($diskname, $found_snapshot, @snapshots);
	$dh->rewind;
	while (defined(my $entry = $dh->read)) {
	    next if $entry eq '.' || $entry eq '..';
	    next if $entry !~ /^$BTRFS_VOL_REGEX$/;
	    my ($cur_diskname, $cur_snapshot) = ($1, $2);

	    die "send stream included a non-snapshot subvolume\n"
		if !defined($cur_snapshot);

	    if (!defined($diskname)) {
		$diskname = $cur_diskname;
	    } else {
		die "multiple disks contained in stream ('$diskname' vs '$cur_diskname')\n"
		    if $diskname ne $cur_diskname;
	    }

	    if ($cur_snapshot eq $snapshot) {
		$found_snapshot = 1;
	    } else {
		push @snapshots, $cur_snapshot;
	    }
	}

	die "send stream did not contain the expected current snapshot '$snapshot'\n"
	    if !$found_snapshot;

	# Rotate the disk into place, first the current state:
	# Note that read-only subvolumes cannot be moved into different directories, but for the
	# "current" state we also want a writable copy, so start with that:
	$class->btrfs_cmd(['property', 'set', "$tmppath/$diskname\@$snapshot", 'ro', 'false']);
	PVE::Tools::renameat2(
	    -1,
	    "$tmppath/$diskname\@$snapshot",
	    -1,
	    $destination,
	    &PVE::Tools::RENAME_NOREPLACE,
	) or die "failed to move received snapshot '$tmppath/$diskname\@$snapshot'"
	    . " into place at '$destination' - $!\n";

	# Now recreate the actual snapshot:
	$class->btrfs_cmd([
	    'subvolume',
	    'snapshot',
	    '-r',
	    '--',
	    $destination,
	    "$destination\@$snapshot",
	]);

	# Now go through the remaining snapshots (if any)
	foreach my $snap (@snapshots) {
	    $class->btrfs_cmd(['property', 'set', "$tmppath/$diskname\@$snap", 'ro', 'false']);
	    PVE::Tools::renameat2(
		-1,
		"$tmppath/$diskname\@$snap",
		-1,
		"$destination\@$snap",
		&PVE::Tools::RENAME_NOREPLACE,
	    ) or die "failed to move received snapshot '$tmppath/$diskname\@$snap'"
		. " into place at '$destination\@$snap' - $!\n";
	    eval { $class->btrfs_cmd(['property', 'set', "$destination\@$snap", 'ro', 'true']) };
	    warn "failed to make $destination\@$snap read-only - $!\n" if $@;
	}
    };
    my $err = $@;

    eval {
	# Cleanup all the received snapshots we did not move into place, so we can remove the temp
	# directory.
	if ($dh) {
	    $dh->rewind;
	    while (defined(my $entry = $dh->read)) {
		next if $entry eq '.' || $entry eq '..';
		eval { $class->btrfs_cmd(['subvolume', 'delete', '--', "$tmppath/$entry"]) };
		warn $@ if $@;
	    }
	    $dh->close; undef $dh;
	}
	if (!rmdir($tmppath)) {
	    warn "failed to remove temporary directory '$tmppath' - $!\n"
	}
    };
    warn $@ if $@;
    if ($err) {
	# clean up if the directory ended up being empty after an error
	rmdir($tmppath);
	die $err;
    }

    return "$storeid:$volname";
}

1
