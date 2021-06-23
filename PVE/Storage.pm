package PVE::Storage;

use strict;
use warnings;
use Data::Dumper;

use POSIX;
use IO::Select;
use IO::File;
use IO::Socket::IP;
use IPC::Open3;
use File::Basename;
use File::Path;
use Cwd 'abs_path';
use Socket;
use Time::Local qw(timelocal);

use PVE::Tools qw(run_command file_read_firstline dir_glob_foreach $IPV6RE);
use PVE::Cluster qw(cfs_read_file cfs_write_file cfs_lock_file);
use PVE::DataCenterConfig;
use PVE::Exception qw(raise_param_exc raise);
use PVE::JSONSchema;
use PVE::INotify;
use PVE::RPCEnvironment;
use PVE::SSHInfo;

use PVE::Storage::Plugin;
use PVE::Storage::DirPlugin;
use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;
use PVE::Storage::NFSPlugin;
use PVE::Storage::CIFSPlugin;
use PVE::Storage::ISCSIPlugin;
use PVE::Storage::RBDPlugin;
use PVE::Storage::CephFSPlugin;
use PVE::Storage::ISCSIDirectPlugin;
use PVE::Storage::GlusterfsPlugin;
use PVE::Storage::ZFSPoolPlugin;
use PVE::Storage::ZFSPlugin;
use PVE::Storage::PBSPlugin;

# Storage API version. Increment it on changes in storage API interface.
use constant APIVER => 8;
# Age is the number of versions we're backward compatible with.
# This is like having 'current=APIVER' and age='APIAGE' in libtool,
# see https://www.gnu.org/software/libtool/manual/html_node/Libtool-versioning.html
use constant APIAGE => 7;

# load standard plugins
PVE::Storage::DirPlugin->register();
PVE::Storage::LVMPlugin->register();
PVE::Storage::LvmThinPlugin->register();
PVE::Storage::NFSPlugin->register();
PVE::Storage::CIFSPlugin->register();
PVE::Storage::ISCSIPlugin->register();
PVE::Storage::RBDPlugin->register();
PVE::Storage::CephFSPlugin->register();
PVE::Storage::ISCSIDirectPlugin->register();
PVE::Storage::GlusterfsPlugin->register();
PVE::Storage::ZFSPoolPlugin->register();
PVE::Storage::ZFSPlugin->register();
PVE::Storage::PBSPlugin->register();

# load third-party plugins
if ( -d '/usr/share/perl5/PVE/Storage/Custom' ) {
    dir_glob_foreach('/usr/share/perl5/PVE/Storage/Custom', '.*\.pm$', sub {
	my ($file) = @_;
	my $modname = 'PVE::Storage::Custom::' . $file;
	$modname =~ s!\.pm$!!;
	$file = 'PVE/Storage/Custom/' . $file;

	eval {
	    require $file;

	    # Check perl interface:
	    die "not derived from PVE::Storage::Plugin\n" if !$modname->isa('PVE::Storage::Plugin');
	    die "does not provide an api() method\n" if !$modname->can('api');
	    # Check storage API version and that file is really storage plugin.
	    my $version = $modname->api();
	    die "implements an API version newer than current ($version > " . APIVER . ")\n"
		if $version > APIVER;
	    my $min_version = (APIVER - APIAGE);
	    die "API version too old, please update the plugin ($version < $min_version)\n"
		if $version < $min_version;
	    # all OK, do import and register (i.e., "use")
	    import $file;
	    $modname->register();

	    # If we got this far and the API version is not the same, make some noise:
	    warn "Plugin \"$modname\" is implementing an older storage API, an upgrade is recommended\n"
		if $version != APIVER;
	};
	if ($@) {
	    warn "Error loading storage plugin \"$modname\": $@";
	}
    });
}

# initialize all plugins
PVE::Storage::Plugin->init();

our $iso_extension_re = qr/\.(?:iso|img)/i;

our $vztmpl_extension_re = qr/\.tar\.([gx]z)/i;

#  PVE::Storage utility functions

sub config {
    return cfs_read_file("storage.cfg");
}

sub write_config {
    my ($cfg) = @_;

    cfs_write_file('storage.cfg', $cfg);
}

sub lock_storage_config {
    my ($code, $errmsg) = @_;

    cfs_lock_file("storage.cfg", undef, $code);
    my $err = $@;
    if ($err) {
	$errmsg ? die "$errmsg: $err" : die $err;
    }
}

# FIXME remove maxfiles for PVE 8.0 or PVE 9.0
my $convert_maxfiles_to_prune_backups = sub {
    my ($scfg) = @_;

    return if !$scfg;

    my $maxfiles = delete $scfg->{maxfiles};

    if (!defined($scfg->{'prune-backups'}) && defined($maxfiles)) {
	my $prune_backups;
	if ($maxfiles) {
	    $prune_backups = { 'keep-last' => $maxfiles };
	} else { # maxfiles 0 means no limit
	    $prune_backups = { 'keep-all' => 1 };
	}
	$scfg->{'prune-backups'} = PVE::JSONSchema::print_property_string(
	    $prune_backups,
	    'prune-backups'
	);
    }
};

sub storage_config {
    my ($cfg, $storeid, $noerr) = @_;

    die "no storage ID specified\n" if !$storeid;

    my $scfg = $cfg->{ids}->{$storeid};

    die "storage '$storeid' does not exist\n" if (!$noerr && !$scfg);

    $convert_maxfiles_to_prune_backups->($scfg);

    return $scfg;
}

sub storage_check_node {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config($cfg, $storeid);

    if ($scfg->{nodes}) {
	$node = PVE::INotify::nodename() if !$node || ($node eq 'localhost');
	if (!$scfg->{nodes}->{$node}) {
	    die "storage '$storeid' is not available on node '$node'\n" if !$noerr;
	    return undef;
	}
    }

    return $scfg;
}

sub storage_check_enabled {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config($cfg, $storeid);

    if ($scfg->{disable}) {
	die "storage '$storeid' is disabled\n" if !$noerr;
	return undef;
    }

    return storage_check_node($cfg, $storeid, $node, $noerr);
}

# storage_can_replicate:
# return true if storage supports replication
# (volumes allocated with vdisk_alloc() has replication feature)
sub storage_can_replicate {
    my ($cfg, $storeid, $format) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->storage_can_replicate($scfg, $storeid, $format);
}

sub storage_ids {
    my ($cfg) = @_;

    return keys %{$cfg->{ids}};
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    return PVE::Storage::Plugin::file_size_info($filename, $timeout);
}

sub get_volume_notes {
    my ($cfg, $volid, $timeout) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_volume_notes($scfg, $storeid, $volname, $timeout);
}

sub update_volume_notes {
    my ($cfg, $volid, $notes, $timeout) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    $plugin->update_volume_notes($scfg, $storeid, $volname, $notes, $timeout);
}

sub volume_size_info {
    my ($cfg, $volid, $timeout) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	return $plugin->volume_size_info($scfg, $storeid, $volname, $timeout);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	return file_size_info($volid, $timeout);
    } else {
	return 0;
    }
}

sub volume_resize {
    my ($cfg, $volid, $size, $running) = @_;

    my $padding = (1024 - $size % 1024) % 1024;
    $size = $size + $padding;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_resize($scfg, $storeid, $volname, $size, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "resize file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_rollback_is_possible {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_rollback_is_possible($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot rollback file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_snapshot($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot_rollback {
    my ($cfg, $volid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$plugin->volume_rollback_is_possible($scfg, $storeid, $volname, $snap);
        return $plugin->volume_snapshot_rollback($scfg, $storeid, $volname, $snap);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot rollback file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

sub volume_snapshot_delete {
    my ($cfg, $volid, $snap, $running) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_snapshot_delete($scfg, $storeid, $volname, $snap, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
        die "snapshot delete file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
}

# check if a filesystem on top of a volume needs to flush its journal for
# consistency (see fsfreeze(8)) before a snapshot is taken - needed for
# container mountpoints
sub volume_snapshot_needs_fsfreeze {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_snapshot_needs_fsfreeze();
}

# check if a volume or snapshot supports a given feature
# $feature - one of:
#            clone - linked clone is possible
#            copy  - full clone is possible
#            replicate - replication is possible
#            snapshot - taking a snapshot is possible
#            sparseinit - volume is sparsely initialized
#            template - conversion to base image is possible
# $snap - check if the feature is supported for a given snapshot
# $running - if the guest owning the volume is running
# $opts - hash with further options:
#         valid_target_formats - list of formats for the target of a copy/clone
#                                operation that the caller could work with. The
#                                format of $volid is always considered valid and if
#                                no list is specified, all formats are considered valid.
sub volume_has_feature {
    my ($cfg, $feature, $volid, $snap, $running, $opts) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_has_feature($scfg, $feature, $storeid, $volname, $snap, $running, $opts);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	return undef;
    } else {
	return undef;
    }
}

sub volume_snapshot_list {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	return $plugin->volume_snapshot_list($scfg, $storeid, $volname);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	die "send file/device '$volid' is not possible\n";
    } else {
	die "unable to parse volume ID '$volid'\n";
    }
    # return an empty array if dataset does not exist.
}

sub get_image_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $path = $plugin->get_subdir($scfg, 'images');

    return $vmid ? "$path/$vmid" : $path;
}

sub get_private_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $path = $plugin->get_subdir($scfg, 'rootdir');

    return $vmid ? "$path/$vmid" : $path;
}

sub get_iso_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'iso');
}

sub get_vztmpl_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'vztmpl');
}

sub get_backup_dir {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->get_subdir($scfg, 'backup');
}

# library implementation

sub parse_vmid {
    my $vmid = shift;

    die "VMID '$vmid' contains illegal characters\n" if $vmid !~ m/^\d+$/;

    return int($vmid);
}

# NOTE: basename and basevmid are always undef for LVM-thin, where the
# clone -> base reference is not encoded in the volume ID.
# see note in PVE::Storage::LvmThinPlugin for details.
sub parse_volname {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    # returns ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format)

    return $plugin->parse_volname($volname);
}

sub parse_volume_id {
    my ($volid, $noerr) = @_;

    return PVE::Storage::Plugin::parse_volume_id($volid, $noerr);
}

# test if we have read access to volid
sub check_volume_access {
    my ($rpcenv, $user, $cfg, $vmid, $volid) = @_;

    my ($sid, $volname) = parse_volume_id($volid, 1);
    if ($sid) {
	my ($vtype, undef, $ownervm) = parse_volname($cfg, $volid);
	if ($vtype eq 'iso' || $vtype eq 'vztmpl') {
	    # require at least read access to storage, (custom) templates/ISOs could be sensitive
	    $rpcenv->check_any($user, "/storage/$sid", ['Datastore.AllocateSpace', 'Datastore.Audit']);
	} elsif (defined($ownervm) && defined($vmid) && ($ownervm == $vmid)) {
	    # we are owner - allow access
	} elsif ($vtype eq 'backup' && $ownervm) {
	    $rpcenv->check($user, "/storage/$sid", ['Datastore.AllocateSpace']);
	    $rpcenv->check($user, "/vms/$ownervm", ['VM.Backup']);
	} else {
	    # allow if we are Datastore administrator
	    $rpcenv->check($user, "/storage/$sid", ['Datastore.Allocate']);
	}
    } else {
	die "Only root can pass arbitrary filesystem paths."
	    if $user ne 'root@pam';
    }

    return undef;
}

# NOTE: this check does not work for LVM-thin, where the clone -> base
# reference is not encoded in the volume ID.
# see note in PVE::Storage::LvmThinPlugin for details.
sub volume_is_base_and_used {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my ($vtype, $name, $vmid, undef, undef, $isBase, undef) =
	$plugin->parse_volname($volname);

    if ($isBase) {
	my $vollist = $plugin->list_images($storeid, $scfg);
	foreach my $info (@$vollist) {
	    my (undef, $tmpvolname) = parse_volume_id($info->{volid});
	    my $basename = undef;
	    my $basevmid = undef;

	    eval{
		(undef, undef, undef, $basename, $basevmid) =
		    $plugin->parse_volname($tmpvolname);
	    };

	    if ($basename && defined($basevmid) && $basevmid == $vmid && $basename eq $name) {
		return 1;
	    }
	}
    }
    return 0;
}

# try to map a filesystem path to a volume identifier
sub path_to_volume_id {
    my ($cfg, $path) = @_;

    my $ids = $cfg->{ids};

    my ($sid, $volname) = parse_volume_id($path, 1);
    if ($sid) {
	if (my $scfg = $ids->{$sid}) {
	    if ($scfg->{path}) {
		my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
		my ($vtype, $name, $vmid) = $plugin->parse_volname($volname);
		return ($vtype, $path);
	    }
	}
	return ('');
    }

    # Note: abs_path() return undef if $path doesn not exist
    # for example when nfs storage is not mounted
    $path = abs_path($path) || $path;

    foreach my $sid (keys %$ids) {
	my $scfg = $ids->{$sid};
	next if !$scfg->{path};
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	my $imagedir = $plugin->get_subdir($scfg, 'images');
	my $isodir = $plugin->get_subdir($scfg, 'iso');
	my $tmpldir = $plugin->get_subdir($scfg, 'vztmpl');
	my $backupdir = $plugin->get_subdir($scfg, 'backup');
	my $privatedir = $plugin->get_subdir($scfg, 'rootdir');
	my $snippetsdir = $plugin->get_subdir($scfg, 'snippets');

	if ($path =~ m!^$imagedir/(\d+)/([^/\s]+)$!) {
	    my $vmid = $1;
	    my $name = $2;

	    my $vollist = $plugin->list_images($sid, $scfg, $vmid);
	    foreach my $info (@$vollist) {
		my ($storeid, $volname) = parse_volume_id($info->{volid});
		my $volpath = $plugin->path($scfg, $volname, $storeid);
		if ($volpath eq $path) {
		    return ('images', $info->{volid});
		}
	    }
	} elsif ($path =~ m!^$isodir/([^/]+$iso_extension_re)$!) {
	    my $name = $1;
	    return ('iso', "$sid:iso/$name");
	} elsif ($path =~ m!^$tmpldir/([^/]+$vztmpl_extension_re)$!) {
	    my $name = $1;
	    return ('vztmpl', "$sid:vztmpl/$name");
	} elsif ($path =~ m!^$privatedir/(\d+)$!) {
	    my $vmid = $1;
	    return ('rootdir', "$sid:rootdir/$vmid");
	} elsif ($path =~ m!^$backupdir/([^/]+\.(?:tgz|(?:(?:tar|vma)(?:\.(?:${\PVE::Storage::Plugin::COMPRESSOR_RE}))?)))$!) {
	    my $name = $1;
	    return ('backup', "$sid:backup/$name");
	} elsif ($path =~ m!^$snippetsdir/([^/]+)$!) {
	    my $name = $1;
	    return ('snippets', "$sid:snippets/$name");
	}
    }

    # can't map path to volume id
    return ('');
}

sub path {
    my ($cfg, $volid, $snapname) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    my ($path, $owner, $vtype) = $plugin->path($scfg, $volname, $storeid, $snapname);
    return wantarray ? ($path, $owner, $vtype) : $path;
}

sub abs_filesystem_path {
    my ($cfg, $volid, $allow_blockdev) = @_;

    my $path;
    if (parse_volume_id ($volid, 1)) {
	activate_volumes($cfg, [ $volid ]);
	$path = PVE::Storage::path($cfg, $volid);
    } else {
	if (-f $volid || ($allow_blockdev && -b $volid)) {
	    my $abspath = abs_path($volid);
	    if ($abspath && $abspath =~ m|^(/.+)$|) {
		$path = $1; # untaint any path
	    }
	}
    }
    die "can't find file '$volid'\n"
	if !($path && (-f $path || ($allow_blockdev && -b $path)));

    return $path;
}

my $volname_for_storage = sub {
    my ($cfg, $volid, $target_storeid) = @_;

    my (undef, $name, $vmid, undef, undef, undef, $format) = parse_volname($cfg, $volid);
    my $target_scfg = storage_config($cfg, $target_storeid);

    my (undef, $valid_formats) = PVE::Storage::Plugin::default_format($target_scfg);
    my $format_is_valid = grep { $_ eq $format } @$valid_formats;
    die "unsupported format '$format' for storage type $target_scfg->{type}\n" if !$format_is_valid;

    (my $name_without_extension = $name) =~ s/\.$format$//;

    if ($target_scfg->{path}) {
       return "$vmid/$name_without_extension.$format";
    } else {
       return "$name_without_extension";
    }
};

sub storage_migrate {
    my ($cfg, $volid, $target_sshinfo, $target_storeid, $opts, $logfunc) = @_;

    my $base_snapshot = $opts->{base_snapshot};
    my $snapshot = $opts->{snapshot};
    my $ratelimit_bps = $opts->{ratelimit_bps};
    my $insecure = $opts->{insecure};
    my $with_snapshots = $opts->{with_snapshots} ? 1 : 0;
    my $allow_rename = $opts->{allow_rename} ? 1 : 0;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    # no need to migrate shared content
    return $volid if $storeid eq $target_storeid && $scfg->{shared};

    my $tcfg = storage_config($cfg, $target_storeid);

    my $target_volname;
    if ($opts->{target_volname}) {
	$target_volname = $opts->{target_volname};
    } elsif ($scfg->{type} eq $tcfg->{type}) {
	$target_volname = $volname;
    } else {
	$target_volname = $volname_for_storage->($cfg, $volid, $target_storeid);
    }

    my $target_volid = "${target_storeid}:${target_volname}";

    my $target_ip = $target_sshinfo->{ip};

    my $ssh = PVE::SSHInfo::ssh_info_to_command($target_sshinfo);
    my $ssh_base = PVE::SSHInfo::ssh_info_to_command_base($target_sshinfo);
    local $ENV{RSYNC_RSH} = PVE::Tools::cmd2string($ssh_base);

    my @cstream;
    if (defined($ratelimit_bps)) {
	@cstream = ([ '/usr/bin/cstream', '-t', $ratelimit_bps ]);
	$logfunc->("using a bandwidth limit of $ratelimit_bps bps for transferring '$volid'") if $logfunc;
    }

    my $migration_snapshot;
    if (!defined($snapshot)) {
	if ($scfg->{type} eq 'zfspool') {
	    $migration_snapshot = 1;
	    $snapshot = '__migration__';
	}
    }

    my @formats = volume_transfer_formats($cfg, $volid, $target_volid, $snapshot, $base_snapshot, $with_snapshots);
    die "cannot migrate from storage type '$scfg->{type}' to '$tcfg->{type}'\n" if !@formats;
    my $format = $formats[0];

    my $import_fn = '-'; # let pvesm import read from stdin per default
    if ($insecure) {
	my $net = $target_sshinfo->{network} // $target_sshinfo->{ip};
	$import_fn = "tcp://$net";
    }

    my $target_apiver = 1; # if there is no apiinfo call, assume 1
    my $get_api_version = [@$ssh, 'pvesm', 'apiinfo'];
    my $match_api_version = sub { $target_apiver = $1 if $_[0] =~ m!^APIVER (\d+)$!; };
    eval { run_command($get_api_version, logfunc => $match_api_version); };

    my $send = ['pvesm', 'export', $volid, $format, '-', '-with-snapshots', $with_snapshots];
    my $recv = [@$ssh, '--', 'pvesm', 'import', $target_volid, $format, $import_fn, '-with-snapshots', $with_snapshots];
    if (defined($snapshot)) {
	push @$send, '-snapshot', $snapshot
    }
    if ($migration_snapshot) {
	push @$recv, '-delete-snapshot', $snapshot;
    }
    push @$recv, '-allow-rename', $allow_rename if $target_apiver >= 5;

    if (defined($base_snapshot)) {
	# Check if the snapshot exists on the remote side:
	push @$send, '-base', $base_snapshot;
	push @$recv, '-base', $base_snapshot;
    }

    my $new_volid;
    my $pattern = volume_imported_message(undef, 1);
    my $match_volid_and_log = sub {
	my $line = shift;

	$new_volid = $1 if ($line =~ $pattern);

	if ($logfunc) {
	    chomp($line);
	    $logfunc->($line);
	}
    };

    volume_snapshot($cfg, $volid, $snapshot) if $migration_snapshot;

    if (defined($snapshot)) {
	activate_volumes($cfg, [$volid], $snapshot);
    } else {
	activate_volumes($cfg, [$volid]);
    }

    eval {
	if ($insecure) {
	    my $input = IO::File->new();
	    my $info = IO::File->new();
	    open3($input, $info, $info, @{$recv})
		or die "receive command failed: $!\n";
	    close($input);

	    my $try_ip = <$info> // '';
	    my ($ip) = $try_ip =~ /^($PVE::Tools::IPRE)$/ # untaint
		or die "no tunnel IP received, got '$try_ip'\n";

	    my $try_port = <$info> // '';
	    my ($port) = $try_port =~ /^(\d+)$/ # untaint
		or die "no tunnel port received, got '$try_port'\n";

	    my $socket = IO::Socket::IP->new(PeerHost => $ip, PeerPort => $port, Type => SOCK_STREAM)
		or die "failed to connect to tunnel at $ip:$port\n";
	    # we won't be reading from the socket
	    shutdown($socket, 0);

	    eval { run_command([$send, @cstream], output => '>&'.fileno($socket), errfunc => $logfunc); };
	    my $send_error = $@;

	    # don't close the connection entirely otherwise the receiving end
	    # might not get all buffered data (and fails with 'connection reset by peer')
	    shutdown($socket, 1);

	    # wait for the remote process to finish
	    while (my $line = <$info>) {
		$match_volid_and_log->("[$target_sshinfo->{name}] $line");
	    }

	    # now close the socket
	    close($socket);
	    if (!close($info)) { # does waitpid()
		die "import failed: $!\n" if $!;
		die "import failed: exit code ".($?>>8)."\n";
	    }

	    die $send_error if $send_error;
	} else {
	    run_command([$send, @cstream, $recv], logfunc => $match_volid_and_log);
	}

	die "unable to get ID of the migrated volume\n"
	    if !defined($new_volid) && $target_apiver >= 5;
    };
    my $err = $@;
    warn "send/receive failed, cleaning up snapshot(s)..\n" if $err;
    if ($migration_snapshot) {
	eval { volume_snapshot_delete($cfg, $volid, $snapshot, 0) };
	warn "could not remove source snapshot: $@\n" if $@;
    }
    die $err if $err;

    return $new_volid // $target_volid;
}

sub vdisk_clone {
    my ($cfg, $volid, $vmid, $snap) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $volname = $plugin->clone_image($scfg, $storeid, $volname, $vmid, $snap);
	return "$storeid:$volname";
    });
}

sub vdisk_create_base {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $volname = $plugin->create_base($storeid, $scfg, $volname);
	return "$storeid:$volname";
    });
}

sub map_volume {
    my ($cfg, $volid, $snapname) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->map_volume($storeid, $scfg, $volname, $snapname);
}

sub unmap_volume {
    my ($cfg, $volid, $snapname) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);

    my $scfg = storage_config($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    return $plugin->unmap_volume($storeid, $scfg, $volname, $snapname);
}

sub vdisk_alloc {
    my ($cfg, $storeid, $vmid, $fmt, $name, $size) = @_;

    die "no storage ID specified\n" if !$storeid;

    PVE::JSONSchema::parse_storage_id($storeid);

    my $scfg = storage_config($cfg, $storeid);

    die "no VMID specified\n" if !$vmid;

    $vmid = parse_vmid($vmid);

    my $defformat = PVE::Storage::Plugin::default_format($scfg);

    $fmt = $defformat if !$fmt;

    activate_storage($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    # lock shared storage
    return $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	my $old_umask = umask(umask|0037);
	my $volname = eval { $plugin->alloc_image($storeid, $scfg, $vmid, $fmt, $name, $size) };
	my $err = $@;
	umask $old_umask;
	die $err if $err;
	return "$storeid:$volname";
    });
}

sub vdisk_free {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    my $cleanup_worker;

    # lock shared storage
    $plugin->cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	# LVM-thin allows deletion of still referenced base volumes!
	die "base volume '$volname' is still in use by linked clones\n"
	    if volume_is_base_and_used($cfg, $volid);

	my (undef, undef, undef, undef, undef, $isBase, $format) =
	    $plugin->parse_volname($volname);
	$cleanup_worker = $plugin->free_image($storeid, $scfg, $volname, $isBase, $format);
    });

    return if !$cleanup_worker;

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();

    $rpcenv->fork_worker('imgdel', undef, $authuser, $cleanup_worker);
}

sub vdisk_list {
    my ($cfg, $storeid, $vmid, $vollist, $ctype) = @_;

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = $storeid ? { $storeid => [] } : {};

    # prepare/activate/refresh all storages

    my $storage_list = [];
    if ($vollist) {
	foreach my $volid (@$vollist) {
	    my ($sid, undef) = parse_volume_id($volid);
	    next if !defined($ids->{$sid});
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    push @$storage_list, $sid;
	}
    } else {
	foreach my $sid (keys %$ids) {
	    next if $storeid && $storeid ne $sid;
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    my $content = $ids->{$sid}->{content};
	    next if defined($ctype) && !$content->{$ctype};
	    next if !($content->{rootdir} || $content->{images});
	    push @$storage_list, $sid;
	}
    }

    my $cache = {};

    activate_storage_list($cfg, $storage_list, $cache);

    for my $sid ($storage_list->@*) {
	next if $storeid && $storeid ne $sid;

	my $scfg = $ids->{$sid};
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$res->{$sid} = $plugin->list_images($sid, $scfg, $vmid, $vollist, $cache);
	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}

sub template_list {
    my ($cfg, $storeid, $tt) = @_;

    die "unknown template type '$tt'\n"
	if !($tt eq 'iso' || $tt eq 'vztmpl' || $tt eq 'backup' || $tt eq 'snippets');

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # query the storage
    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;

	my $scfg = $ids->{$sid};
	my $type = $scfg->{type};

	next if !$scfg->{content}->{$tt};

	next if !storage_check_enabled($cfg, $sid, undef, 1);

	$res->{$sid} = volume_list($cfg, $sid, undef, $tt);
    }

    return $res;
}

sub volume_list {
    my ($cfg, $storeid, $vmid, $content) = @_;

    my @ctypes = qw(rootdir images vztmpl iso backup snippets);

    my $cts = $content ? [ $content ] : [ @ctypes ];

    my $scfg = PVE::Storage::storage_config($cfg, $storeid);

    $cts = [ grep { defined($scfg->{content}->{$_}) } @$cts ];

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    activate_storage($cfg, $storeid);

    my $res = $plugin->list_volumes($storeid, $scfg, $vmid, $cts);

    @$res = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @$res;

    return $res;
}

sub uevent_seqnum {

    my $filename = "/sys/kernel/uevent_seqnum";

    my $seqnum = 0;
    if (my $fh = IO::File->new($filename, "r")) {
	my $line = <$fh>;
	if ($line =~ m/^(\d+)$/) {
	    $seqnum = int($1);
	}
	close ($fh);
    }
    return $seqnum;
}

sub activate_storage {
    my ($cfg, $storeid, $cache) = @_;

    $cache = {} if !$cache;

    my $scfg = storage_check_enabled($cfg, $storeid);

    return if $cache->{activated}->{$storeid};

    $cache->{uevent_seqnum} = uevent_seqnum() if !$cache->{uevent_seqnum};

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    if ($scfg->{base}) {
	my ($baseid, undef) = parse_volume_id ($scfg->{base});
	activate_storage($cfg, $baseid, $cache);
    }

    if (!$plugin->check_connection($storeid, $scfg)) {
	die "storage '$storeid' is not online\n";
    }

    $plugin->activate_storage($storeid, $scfg, $cache);

    my $newseq = uevent_seqnum ();

    # only call udevsettle if there are events
    if ($newseq > $cache->{uevent_seqnum}) {
	system ("udevadm settle --timeout=30"); # ignore errors
	$cache->{uevent_seqnum} = $newseq;
    }

    $cache->{activated}->{$storeid} = 1;
}

sub activate_storage_list {
    my ($cfg, $storeid_list, $cache) = @_;

    $cache = {} if !$cache;

    foreach my $storeid (@$storeid_list) {
	activate_storage($cfg, $storeid, $cache);
    }
}

sub deactivate_storage {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config ($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

    my $cache = {};
    $plugin->deactivate_storage($storeid, $scfg, $cache);
}

sub activate_volumes {
    my ($cfg, $vollist, $snapname) = @_;

    return if !($vollist && scalar(@$vollist));

    my $storagehash = {};
    foreach my $volid (@$vollist) {
	my ($storeid, undef) = parse_volume_id($volid);
	$storagehash->{$storeid} = 1;
    }

    my $cache = {};

    activate_storage_list($cfg, [keys %$storagehash], $cache);

    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id($volid);
	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$plugin->activate_volume($storeid, $scfg, $volname, $snapname, $cache);
    }
}

sub deactivate_volumes {
    my ($cfg, $vollist, $snapname) = @_;

    return if !($vollist && scalar(@$vollist));

    my $cache = {};

    my @errlist = ();
    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id($volid);

	my $scfg = storage_config($cfg, $storeid);
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

	eval {
	    $plugin->deactivate_volume($storeid, $scfg, $volname, $snapname, $cache);
	};
	if (my $err = $@) {
	    warn $err;
	    push @errlist, $volid;
	}
    }

    die "volume deactivation failed: " . join(' ', @errlist)
	if scalar(@errlist);
}

sub storage_info {
    my ($cfg, $content, $includeformat) = @_;

    my $ids = $cfg->{ids};

    my $info = {};

    my @ctypes = PVE::Tools::split_list($content);

    my $slist = [];
    foreach my $storeid (keys %$ids) {
	my $storage_enabled = defined(storage_check_enabled($cfg, $storeid, undef, 1));

	if (defined($content)) {
	    my $want_ctype = 0;
	    foreach my $ctype (@ctypes) {
		if ($ids->{$storeid}->{content}->{$ctype}) {
		    $want_ctype = 1;
		    last;
		}
	    }
	    next if !$want_ctype || !$storage_enabled;
	}

	my $type = $ids->{$storeid}->{type};

	$info->{$storeid} = {
	    type => $type,
	    total => 0,
	    avail => 0,
	    used => 0,
	    shared => $ids->{$storeid}->{shared} ? 1 : 0,
	    content => PVE::Storage::Plugin::content_hash_to_string($ids->{$storeid}->{content}),
	    active => 0,
	    enabled => $storage_enabled ? 1 : 0,
	};

	push @$slist, $storeid;
    }

    my $cache = {};

    foreach my $storeid (keys %$ids) {
	my $scfg = $ids->{$storeid};

	next if !$info->{$storeid};
	next if !$info->{$storeid}->{enabled};

	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	if ($includeformat) {
	    my $pd = $plugin->plugindata();
	    $info->{$storeid}->{format} = $pd->{format}
		if $pd->{format};
	    $info->{$storeid}->{select_existing} = $pd->{select_existing}
		if $pd->{select_existing};
	}

	eval { activate_storage($cfg, $storeid, $cache); };
	if (my $err = $@) {
	    warn $err;
	    next;
	}

	my ($total, $avail, $used, $active) = eval { $plugin->status($storeid, $scfg, $cache); };
	warn $@ if $@;
	next if !$active;
	$info->{$storeid}->{total} = int($total);
	$info->{$storeid}->{avail} = int($avail);
	$info->{$storeid}->{used} = int($used);
	$info->{$storeid}->{active} = $active;
    }

    return $info;
}

sub resolv_server {
    my ($server) = @_;

    my ($packed_ip, $family);
    eval {
	my @res = PVE::Tools::getaddrinfo_all($server);
	$family = $res[0]->{family};
	$packed_ip = (PVE::Tools::unpack_sockaddr_in46($res[0]->{addr}))[2];
    };
    if (defined $packed_ip) {
	return Socket::inet_ntop($family, $packed_ip);
    }
    return undef;
}

sub scan_nfs {
    my ($server_in) = @_;

    my $server;
    if (!($server = resolv_server ($server_in))) {
	die "unable to resolve address for server '${server_in}'\n";
    }

    my $cmd = ['/sbin/showmount',  '--no-headers', '--exports', $server];

    my $res = {};
    run_command($cmd, outfunc => sub {
	my $line = shift;

	# note: howto handle white spaces in export path??
	if ($line =~ m!^(/\S+)\s+(.+)$!) {
	    $res->{$1} = $2;
	}
    });

    return $res;
}

sub scan_cifs {
    my ($server_in, $user, $password, $domain) = @_;

    my $server = resolv_server($server_in);
    die "unable to resolve address for server '${server_in}'\n" if !$server;

    # we only support Windows 2012 and newer, so just use smb3
    my $cmd = ['/usr/bin/smbclient', '-m', 'smb3', '-d', '0', '-L', $server];
    push @$cmd, '-W', $domain if defined($domain);

    push @$cmd, '-N' if !defined($password);
    local $ENV{USER} = $user if defined($user);
    local $ENV{PASSWD} = $password if defined($password);

    my $res = {};
    my $err = '';
    run_command($cmd,
	noerr => 1,
	errfunc => sub {
	    $err .= "$_[0]\n"
	},
	outfunc => sub {
	    my $line = shift;
	    if ($line =~ m/(\S+)\s*Disk\s*(\S*)/) {
		$res->{$1} = $2;
	    } elsif ($line =~ m/(NT_STATUS_(\S+))/) {
		my $status = $1;
		$err .= "unexpected status: $1\n" if uc($1) ne 'SUCCESS';
	    }
	},
    );
    # only die if we got no share, else it's just some followup check error
    # (like workgroup querying)
    raise($err) if $err && !%$res;

    return $res;
}

sub scan_zfs {

    my $cmd = ['zfs',  'list', '-t', 'filesystem', '-Hp', '-o', 'name,avail,used'];

    my $res = [];
    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~m/^(\S+)\s+(\S+)\s+(\S+)$/) {
	    my ($pool, $size_str, $used_str) = ($1, $2, $3);
	    my $size = $size_str + 0;
	    my $used = $used_str + 0;
	    # ignore subvolumes generated by our ZFSPoolPlugin
	    return if $pool =~ m!/subvol-\d+-[^/]+$!;
	    return if $pool =~ m!/basevol-\d+-[^/]+$!;
	    push @$res, { pool => $pool, size => $size, free => $size-$used };
	}
    });

    return $res;
}

sub resolv_portal {
    my ($portal, $noerr) = @_;

    my ($server, $port) = PVE::Tools::parse_host_and_port($portal);
    if ($server) {
	if (my $ip = resolv_server($server)) {
	    $server = $ip;
	    $server = "[$server]" if $server =~ /^$IPV6RE$/;
	    return $port ? "$server:$port" : $server;
	}
    }
    return undef if $noerr;

    raise_param_exc({ portal => "unable to resolve portal address '$portal'" });
}


sub scan_iscsi {
    my ($portal_in) = @_;

    my $portal;
    if (!($portal = resolv_portal($portal_in))) {
	die "unable to parse/resolve portal address '${portal_in}'\n";
    }

    return PVE::Storage::ISCSIPlugin::iscsi_discovery($portal);
}

sub storage_default_format {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config ($cfg, $storeid);

    return PVE::Storage::Plugin::default_format($scfg);
}

sub vgroup_is_used {
    my ($cfg, $vgname) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{type} eq 'lvm' && $scfg->{vgname} eq $vgname) {
	    return 1;
	}
    }

    return undef;
}

sub target_is_used {
    my ($cfg, $target) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{type} eq 'iscsi' && $scfg->{target} eq $target) {
	    return 1;
	}
    }

    return undef;
}

sub volume_is_used {
    my ($cfg, $volid) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{base} && $scfg->{base} eq $volid) {
	    return 1;
	}
    }

    return undef;
}

sub storage_is_used {
    my ($cfg, $storeid) = @_;

    foreach my $sid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config($cfg, $sid);
	next if !$scfg->{base};
	my ($st) = parse_volume_id($scfg->{base});
	return 1 if $st && $st eq $storeid;
    }

    return undef;
}

sub foreach_volid {
    my ($list, $func) = @_;

    return if !$list;

    foreach my $sid (keys %$list) {
       foreach my $info (@{$list->{$sid}}) {
           my $volid = $info->{volid};
	   my ($sid1, $volname) = parse_volume_id($volid, 1);
	   if ($sid1 && $sid1 eq $sid) {
	       &$func ($volid, $sid, $info);
	   } else {
	       warn "detected strange volid '$volid' in volume list for '$sid'\n";
	   }
       }
    }
}

sub decompressor_info {
    my ($format, $comp) = @_;

    if ($format eq 'tgz' && !defined($comp)) {
	($format, $comp) = ('tar', 'gz');
    }

    my $decompressor = {
	tar => {
	    gz => ['tar', '-z'],
	    lzo => ['tar', '--lzop'],
	    zst => ['tar', '--zstd'],
	},
	vma => {
	    gz => ['zcat'],
	    lzo => ['lzop', '-d', '-c'],
	    zst => ['zstd', '-q', '-d', '-c'],
	},
    };

    die "ERROR: archive format not defined\n"
	if !defined($decompressor->{$format});

    my $decomp;
    $decomp = $decompressor->{$format}->{$comp} if $comp;

    my $info = {
	format => $format,
	compression => $comp,
	decompressor => $decomp,
    };

    return $info;
}

sub archive_info {
    my ($archive) = shift;
    my $info;

    my $volid = basename($archive);
    if ($volid =~ /^(vzdump-(lxc|openvz|qemu)-.+\.(tgz$|tar|vma)(?:\.(${\PVE::Storage::Plugin::COMPRESSOR_RE}))?)$/) {
	my $filename = "$1"; # untaint
	my ($type, $format, $comp) = ($2, $3, $4);
	my $format_re = defined($comp) ? "$format.$comp" : "$format";
	$info = decompressor_info($format, $comp);
	$info->{filename} = $filename;
	$info->{type} = $type;

	if ($volid =~ /^(vzdump-${type}-([1-9][0-9]{2,8})-(\d{4})_(\d{2})_(\d{2})-(\d{2})_(\d{2})_(\d{2}))\.${format_re}$/) {
	    $info->{logfilename} = "$1.log";
	    $info->{vmid} = int($2);
	    $info->{ctime} = timelocal($8, $7, $6, $5, $4 - 1, $3);
	    $info->{is_std_name} = 1;
	} else {
	    $info->{is_std_name} = 0;
	}
    } else {
	die "ERROR: couldn't determine archive info from '$archive'\n";
    }

    return $info;
}

sub archive_remove {
    my ($archive_path) = @_;

    my $dirname = dirname($archive_path);
    my $archive_info = eval { archive_info($archive_path) } // {};
    my $logfn = $archive_info->{logfilename};

    unlink $archive_path or die "removing archive $archive_path failed: $!\n";

    if (defined($logfn)) {
	my $logpath = "$dirname/$logfn";
	if (-e $logpath) {
	    unlink $logpath or warn "removing log file $logpath failed: $!\n";
	}
    }
}

sub extract_vzdump_config_tar {
    my ($archive, $conf_re) = @_;

    die "ERROR: file '$archive' does not exist\n" if ! -f $archive;

    my $pid = open(my $fh, '-|', 'tar', 'tf', $archive) ||
       die "unable to open file '$archive'\n";

    my $file;
    while (defined($file = <$fh>)) {
	if ($file =~ $conf_re) {
	    $file = $1; # untaint
	    last;
	}
    }

    kill 15, $pid;
    waitpid $pid, 0;
    close $fh;

    die "ERROR: archive contains no configuration file\n" if !$file;
    chomp $file;

    my $raw = '';
    my $out = sub {
	my $output = shift;
	$raw .= "$output\n";
    };

    run_command(['tar', '-xpOf', $archive, $file, '--occurrence'], outfunc => $out);

    return wantarray ? ($raw, $file) : $raw;
}

sub extract_vzdump_config_vma {
    my ($archive, $comp) = @_;

    my $raw = '';
    my $out = sub { $raw .= "$_[0]\n"; };

    my $info = archive_info($archive);
    $comp //= $info->{compression};
    my $decompressor = $info->{decompressor};

    if ($comp) {
	my $cmd = [ [@$decompressor, $archive], ["vma", "config", "-"] ];

	# lzop/zcat exits with 1 when the pipe is closed early by vma, detect this and ignore the exit code later
	my $broken_pipe;
	my $errstring;
	my $err = sub {
	    my $output = shift;
	    if ($output =~ m/lzop: Broken pipe: <stdout>/ || $output =~ m/gzip: stdout: Broken pipe/ || $output =~ m/zstd: error 70 : Write error : Broken pipe/) {
		$broken_pipe = 1;
	    } elsif (!defined ($errstring) && $output !~ m/^\s*$/) {
		$errstring = "Failed to extract config from VMA archive: $output\n";
	    }
	};

	my $rc = eval { run_command($cmd, outfunc => $out, errfunc => $err, noerr => 1) };
	my $rerr = $@;

	$broken_pipe ||= $rc == 141; # broken pipe from vma POV

	if (!$errstring && !$broken_pipe && $rc != 0) {
	    die "$rerr\n" if $rerr;
	    die "config extraction failed with exit code $rc\n";
	}
	die "$errstring\n" if $errstring;
    } else {
	run_command(["vma", "config", $archive], outfunc => $out);
    }

    return wantarray ? ($raw, undef) : $raw;
}

sub extract_vzdump_config {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    if (defined($storeid)) {
	my $scfg = storage_config($cfg, $storeid);
	if ($scfg->{type} eq 'pbs') {
	    storage_check_enabled($cfg, $storeid);
	    return PVE::Storage::PBSPlugin->extract_vzdump_config($scfg, $volname, $storeid);
	}
    }

    my $archive = abs_filesystem_path($cfg, $volid);
    my $info = archive_info($archive);
    my $format = $info->{format};
    my $comp = $info->{compression};
    my $type = $info->{type};

    if ($type eq 'lxc' || $type eq 'openvz') {
	return extract_vzdump_config_tar($archive, qr!^(\./etc/vzdump/(pct|vps)\.conf)$!);
    } elsif ($type eq 'qemu') {
	if ($format eq 'tar') {
	    return extract_vzdump_config_tar($archive, qr!\(\./qemu-server\.conf\)!);
	} else {
	    return extract_vzdump_config_vma($archive, $comp);
	}
    } else {
	die "cannot determine backup guest type for backup archive '$volid'\n";
    }
}

sub prune_backups {
    my ($cfg, $storeid, $keep, $vmid, $type, $dryrun, $logfunc) = @_;

    my $scfg = storage_config($cfg, $storeid);
    die "storage '$storeid' does not support backups\n" if !$scfg->{content}->{backup};

    if (!defined($keep)) {
	die "no prune-backups options configured for storage '$storeid'\n"
	    if !defined($scfg->{'prune-backups'});
	$keep = PVE::JSONSchema::parse_property_string('prune-backups', $scfg->{'prune-backups'});
    }

    activate_storage($cfg, $storeid);

    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->prune_backups($scfg, $storeid, $keep, $vmid, $type, $dryrun, $logfunc);
}

my $prune_mark = sub {
    my ($prune_entries, $keep_count, $id_func) = @_;

    return if !$keep_count;

    my $already_included = {};
    my $newly_included = {};

    foreach my $prune_entry (@{$prune_entries}) {
	my $mark = $prune_entry->{mark};
	my $id = $id_func->($prune_entry->{ctime});
	$already_included->{$id} = 1 if defined($mark) && $mark eq 'keep';
    }

    foreach my $prune_entry (@{$prune_entries}) {
	my $mark = $prune_entry->{mark};
	my $id = $id_func->($prune_entry->{ctime});

	next if defined($mark) || $already_included->{$id};

	if (!$newly_included->{$id}) {
	    last if scalar(keys %{$newly_included}) >= $keep_count;
	    $newly_included->{$id} = 1;
	    $prune_entry->{mark} = 'keep';
	} else {
	    $prune_entry->{mark} = 'remove';
	}
    }
};

sub prune_mark_backup_group {
    my ($backup_group, $keep) = @_;

    my $keep_all = delete $keep->{'keep-all'};

    if ($keep_all || !scalar(grep {$_ > 0} values %{$keep})) {
	$keep = { 'keep-all' => 1 } if $keep_all;
	foreach my $prune_entry (@{$backup_group}) {
	    $prune_entry->{mark} = 'keep';
	}
	return;
    }

    my $prune_list = [ sort { $b->{ctime} <=> $a->{ctime} } @{$backup_group} ];

    $prune_mark->($prune_list, $keep->{'keep-last'}, sub {
	my ($ctime) = @_;
	return $ctime;
    });
    $prune_mark->($prune_list, $keep->{'keep-hourly'}, sub {
	my ($ctime) = @_;
	my (undef, undef, $hour, $day, $month, $year) = localtime($ctime);
	return "$hour/$day/$month/$year";
    });
    $prune_mark->($prune_list, $keep->{'keep-daily'}, sub {
	my ($ctime) = @_;
	my (undef, undef, undef, $day, $month, $year) = localtime($ctime);
	return "$day/$month/$year";
    });
    $prune_mark->($prune_list, $keep->{'keep-weekly'}, sub {
	my ($ctime) = @_;
	my ($sec, $min, $hour, $day, $month, $year) = localtime($ctime);
	my $iso_week = int(strftime("%V", $sec, $min, $hour, $day, $month, $year));
	my $iso_week_year = int(strftime("%G", $sec, $min, $hour, $day, $month, $year));
	return "$iso_week/$iso_week_year";
    });
    $prune_mark->($prune_list, $keep->{'keep-monthly'}, sub {
	my ($ctime) = @_;
	my (undef, undef, undef, undef, $month, $year) = localtime($ctime);
	return "$month/$year";
    });
    $prune_mark->($prune_list, $keep->{'keep-yearly'}, sub {
	my ($ctime) = @_;
	my $year = (localtime($ctime))[5];
	return "$year";
    });

    foreach my $prune_entry (@{$prune_list}) {
	$prune_entry->{mark} //= 'remove';
    }
}

sub volume_export {
    my ($cfg, $fh, $volid, $format, $snapshot, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    die "cannot export volume '$volid'\n" if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_export($scfg, $storeid, $fh, $volname, $format,
                                  $snapshot, $base_snapshot, $with_snapshots);
}

sub volume_import {
    my ($cfg, $fh, $volid, $format, $base_snapshot, $with_snapshots, $allow_rename) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    die "cannot import into volume '$volid'\n" if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_import($scfg, $storeid, $fh, $volname, $format,
                                  $base_snapshot, $with_snapshots, $allow_rename) // $volid;
}

sub volume_export_formats {
    my ($cfg, $volid, $snapshot, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    return if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_export_formats($scfg, $storeid, $volname,
                                          $snapshot, $base_snapshot,
                                          $with_snapshots);
}

sub volume_import_formats {
    my ($cfg, $volid, $base_snapshot, $with_snapshots) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    return if !$storeid;
    my $scfg = storage_config($cfg, $storeid);
    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
    return $plugin->volume_import_formats($scfg, $storeid, $volname,
                                          $base_snapshot, $with_snapshots);
}

sub volume_transfer_formats {
    my ($cfg, $src_volid, $dst_volid, $snapshot, $base_snapshot, $with_snapshots) = @_;
    my @export_formats = volume_export_formats($cfg, $src_volid, $snapshot, $base_snapshot, $with_snapshots);
    my @import_formats = volume_import_formats($cfg, $dst_volid, $base_snapshot, $with_snapshots);
    my %import_hash = map { $_ => 1 } @import_formats;
    my @common = grep { $import_hash{$_} } @export_formats;
    return @common;
}

sub volume_imported_message {
    my ($volid, $want_pattern) = @_;

    if ($want_pattern) {
	return qr/successfully imported '([^']*)'$/;
    } else {
	return "successfully imported '$volid'\n";
    }
}

# bash completion helper

sub complete_storage {
    my ($cmdname, $pname, $cvalue) = @_;

    my $cfg = PVE::Storage::config();

    return  $cmdname eq 'add' ? [] : [ PVE::Storage::storage_ids($cfg) ];
}

sub complete_storage_enabled {
    my ($cmdname, $pname, $cvalue) = @_;

    my $res = [];

    my $cfg = PVE::Storage::config();
    foreach my $sid (keys %{$cfg->{ids}}) {
	next if !storage_check_enabled($cfg, $sid, undef, 1);
	push @$res, $sid;
    }
    return $res;
}

sub complete_content_type {
    my ($cmdname, $pname, $cvalue) = @_;

    return [qw(rootdir images vztmpl iso backup snippets)];
}

sub complete_volume {
    my ($cmdname, $pname, $cvalue) = @_;

    my $cfg = config();

    my $storage_list = complete_storage_enabled();

    if ($cvalue =~ m/^([^:]+):/) {
	$storage_list = [ $1 ];
    } else {
	if (scalar(@$storage_list) > 1) {
	    # only list storage IDs to avoid large listings
	    my $res = [];
	    foreach my $storeid (@$storage_list) {
		# Hack: simply return 2 artificial values, so that
		# completions does not finish
		push @$res, "$storeid:volname", "$storeid:...";
	    }
	    return $res;
	}
    }

    my $res = [];
    foreach my $storeid (@$storage_list) {
	my $vollist = PVE::Storage::volume_list($cfg, $storeid);

	foreach my $item (@$vollist) {
	    push @$res, $item->{volid};
	}
    }

    return $res;
}

# Various io-heavy operations require io/bandwidth limits which can be
# configured on multiple levels: The global defaults in datacenter.cfg, and
# per-storage overrides. When we want to do a restore from storage A to storage
# B, we should take the smaller limit defined for storages A and B, and if no
# such limit was specified, use the one from datacenter.cfg.
sub get_bandwidth_limit {
    my ($operation, $storage_list, $override) = @_;

    # called for each limit (global, per-storage) with the 'default' and the
    # $operation limit and should update $override for every limit affecting
    # us.
    my $use_global_limits = 0;
    my $apply_limit = sub {
	my ($bwlimit) = @_;
	if (defined($bwlimit)) {
	    my $limits = PVE::JSONSchema::parse_property_string('bwlimit', $bwlimit);
	    my $limit = $limits->{$operation} // $limits->{default};
	    if (defined($limit)) {
		if (!$override || $limit < $override) {
		    $override = $limit;
		}
		return;
	    }
	}
	# If there was no applicable limit, try to apply the global ones.
	$use_global_limits = 1;
    };

    my ($rpcenv, $authuser);
    if (defined($override)) {
	$rpcenv = PVE::RPCEnvironment->get();
	$authuser = $rpcenv->get_user();
    }

    # Apply per-storage limits - if there are storages involved.
    if (defined($storage_list) && @$storage_list) {
	my $config = config();

	# The Datastore.Allocate permission allows us to modify the per-storage
	# limits, therefore it also allows us to override them.
	# Since we have most likely multiple storages to check, do a quick check on
	# the general '/storage' path to see if we can skip the checks entirely:
	return $override if $rpcenv && $rpcenv->check($authuser, '/storage', ['Datastore.Allocate'], 1);

	my %done;
	foreach my $storage (@$storage_list) {
	    next if !defined($storage);
	    # Avoid duplicate checks:
	    next if $done{$storage};
	    $done{$storage} = 1;

	    # Otherwise we may still have individual /storage/$ID permissions:
	    if (!$rpcenv || !$rpcenv->check($authuser, "/storage/$storage", ['Datastore.Allocate'], 1)) {
		# And if not: apply the limits.
		my $storecfg = storage_config($config, $storage);
		$apply_limit->($storecfg->{bwlimit});
	    }
	}

	# Storage limits take precedence over the datacenter defaults, so if
	# a limit was applied:
	return $override if !$use_global_limits;
    }

    # Sys.Modify on '/' means we can change datacenter.cfg which contains the
    # global default limits.
    if (!$rpcenv || !$rpcenv->check($authuser, '/', ['Sys.Modify'], 1)) {
	# So if we cannot modify global limits, apply them to our currently
	# requested override.
	my $dc = cfs_read_file('datacenter.cfg');
	$apply_limit->($dc->{bwlimit});
    }

    return $override;
}

# checks if the storage id is available and dies if not
sub assert_sid_unused {
    my ($sid) = @_;

    my $cfg = config();
    if (my $scfg = storage_config($cfg, $sid, 1)) {
	die "storage ID '$sid' already defined\n";
    }

    return undef;
}

1;
