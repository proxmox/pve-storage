package PVE::Storage;

use strict;
use warnings;
use Data::Dumper;

use POSIX;
use IO::Select;
use IO::File;
use File::Basename;
use File::Path;
use Cwd 'abs_path';
use Socket;

use PVE::Tools qw(run_command file_read_firstline $IPV6RE);
use PVE::Cluster qw(cfs_read_file cfs_lock_file);
use PVE::Exception qw(raise_param_exc);
use PVE::JSONSchema;
use PVE::INotify;
use PVE::RPCEnvironment;

use PVE::Storage::Plugin;
use PVE::Storage::DirPlugin;
use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;
use PVE::Storage::NFSPlugin;
use PVE::Storage::ISCSIPlugin;
use PVE::Storage::RBDPlugin;
use PVE::Storage::SheepdogPlugin;
use PVE::Storage::ISCSIDirectPlugin;
use PVE::Storage::GlusterfsPlugin;
use PVE::Storage::ZFSPoolPlugin;
use PVE::Storage::ZFSPlugin;
use PVE::Storage::DRBDPlugin;

# load and initialize all plugins
PVE::Storage::DirPlugin->register();
PVE::Storage::LVMPlugin->register();
PVE::Storage::LvmThinPlugin->register();
PVE::Storage::NFSPlugin->register();
PVE::Storage::ISCSIPlugin->register();
PVE::Storage::RBDPlugin->register();
PVE::Storage::SheepdogPlugin->register();
PVE::Storage::ISCSIDirectPlugin->register();
PVE::Storage::GlusterfsPlugin->register();
PVE::Storage::ZFSPoolPlugin->register();
PVE::Storage::ZFSPlugin->register();
PVE::Storage::DRBDPlugin->register();
PVE::Storage::Plugin->init();

my $UDEVADM = '/sbin/udevadm';

#  PVE::Storage utility functions

sub config {
    return cfs_read_file("storage.cfg");
}

sub lock_storage_config {
    my ($code, $errmsg) = @_;

    cfs_lock_file("storage.cfg", undef, $code);
    my $err = $@;
    if ($err) {
	$errmsg ? die "$errmsg: $err" : die $err;
    }
}

sub storage_config {
    my ($cfg, $storeid, $noerr) = @_;

    die "no storage id specified\n" if !$storeid;

    my $scfg = $cfg->{ids}->{$storeid};

    die "storage '$storeid' does not exists\n" if (!$noerr && !$scfg);

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

sub storage_ids {
    my ($cfg) = @_;

    return keys %{$cfg->{ids}};
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    return PVE::Storage::Plugin::file_size_info($filename, $timeout);
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

sub volume_has_feature {
    my ($cfg, $feature, $volid, $snap, $running) = @_;

    my ($storeid, $volname) = parse_volume_id($volid, 1);
    if ($storeid) {
        my $scfg = storage_config($cfg, $storeid);
        my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
        return $plugin->volume_has_feature($scfg, $feature, $storeid, $volname, $snap, $running);
    } elsif ($volid =~ m|^(/.+)$| && -e $volid) {
	return undef;
    } else {
	return undef;
    }
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

sub volume_is_base {
    my ($cfg, $volid) = @_;

    my ($sid, $volname) = parse_volume_id($volid, 1);
    return 0 if !$sid;

    if (my $scfg = $cfg->{ids}->{$sid}) {
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	    $plugin->parse_volname($volname);
	return $isBase ? 1 : 0;
    } else {
	# stale volid with undefined storage - so we can just guess
	if ($volid =~ m/base-/) {
	    return 1;
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
	} elsif ($path =~ m!^$isodir/([^/]+\.[Ii][Ss][Oo])$!) {
	    my $name = $1;
	    return ('iso', "$sid:iso/$name");
	} elsif ($path =~ m!^$tmpldir/([^/]+\.tar\.gz)$!) {
	    my $name = $1;
	    return ('vztmpl', "$sid:vztmpl/$name");
	} elsif ($path =~ m!^$privatedir/(\d+)$!) {
	    my $vmid = $1;
	    return ('rootdir', "$sid:rootdir/$vmid");
	} elsif ($path =~ m!^$backupdir/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz|vma|vma\.gz|vma\.lzo))$!) {
	    my $name = $1;
	    return ('iso', "$sid:backup/$name");
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
    my ($cfg, $volid) = @_;

    my $path;
    if (PVE::Storage::parse_volume_id ($volid, 1)) {
	PVE::Storage::activate_volumes($cfg, [ $volid ]);
	$path = PVE::Storage::path($cfg, $volid);
    } else {
	if (-f $volid) {
	    my $abspath = abs_path($volid);
	    if ($abspath && $abspath =~ m|^(/.+)$|) {
		$path = $1; # untaint any path
	    }
	}
    }

    die "can't find file '$volid'\n" if !($path && -f $path);

    return $path;
}

sub storage_migrate {
    my ($cfg, $volid, $target_host, $target_storeid, $target_volname) = @_;

    my ($storeid, $volname) = parse_volume_id($volid);
    $target_volname = $volname if !$target_volname;

    my $scfg = storage_config($cfg, $storeid);

    # no need to migrate shared content
    return if $storeid eq $target_storeid && $scfg->{shared};

    my $tcfg = storage_config($cfg, $target_storeid);

    my $target_volid = "${target_storeid}:${target_volname}";

    my $errstr = "unable to migrate '$volid' to '${target_volid}' on host '$target_host'";

    my $sshoptions = "-o 'BatchMode=yes'";
    my $ssh = "/usr/bin/ssh $sshoptions";

    local $ENV{RSYNC_RSH} = $ssh;

    # only implemented for file system based storage
    if ($scfg->{path}) {
	if ($tcfg->{path}) {

	    my $src_plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	    my $dst_plugin = PVE::Storage::Plugin->lookup($tcfg->{type});
	    my $src = $src_plugin->path($scfg, $volname, $storeid);
	    my $dst = $dst_plugin->path($tcfg, $target_volname, $target_storeid);

	    my $dirname = dirname($dst);

	    if ($tcfg->{shared}) { # we can do a local copy

		run_command(['/bin/mkdir', '-p', $dirname]);

		run_command(['/bin/cp', $src, $dst]);

	    } else {

		run_command(['/usr/bin/ssh', "root\@${target_host}",
			     '/bin/mkdir', '-p', $dirname]);

		# we use rsync with --sparse, so we can't use --inplace,
		# so we remove file on the target if it already exists to
		# save space
		my ($size, $format) = PVE::Storage::Plugin::file_size_info($src);
		if ($format && ($format eq 'raw') && $size) {
		    run_command(['/usr/bin/ssh', "root\@${target_host}",
				 'rm', '-f', $dst],
				outfunc => sub {});
		}

        $src .= "/" if(-d $src);
        my $cmd = ['/usr/bin/rsync', '-a','--numeric-ids','--info=progress2', '--sparse', '--whole-file',        
			   $src, "root\@${target_host}:$dst"];

		my $percent = -1;

		run_command($cmd, outfunc => sub {
		    my $line = shift;

            if ($line =~ m/^\s*([0-9,]+\s+(\d+)%\s.*)$/) {                
			if ($2 > $percent) {
			    $percent = $2;
			    print "rsync status: $1\n";
			    *STDOUT->flush();
			}
		    } else {
			print "$line\n";
			*STDOUT->flush();
		    }
		});
	    }
	} else {
	    die "$errstr - target type '$tcfg->{type}' not implemented\n";
	}

    } elsif ($scfg->{type} eq 'zfspool') {

	if ($tcfg->{type} eq 'zfspool') {

	    die "$errstr - pool on target has not same name as source!"
		if $tcfg->{pool} ne $scfg->{pool};

	    my (undef, $volname) = parse_volname($cfg, $volid);

	    my $zfspath = "$scfg->{pool}\/$volname";

	    my $snap = "zfs snapshot $zfspath\@__migration__";

	    my $send = "zfs send -pv $zfspath\@__migration__ \| ssh root\@$target_host zfs recv $zfspath";

	    my $destroy_target = "ssh root\@$target_host zfs destroy $zfspath\@__migration__";
 	    run_command($snap);
	    eval{
		run_command($send);
	    };
	    my $err;
	    if ($err = $@){
		run_command("zfs destroy $zfspath\@__migration__");
		die $err;
	    }
	    run_command($destroy_target);

 	} else {
 	    die "$errstr - target type $tcfg->{type} is not valid\n";
 	}
    } else {
	die "$errstr - source type '$scfg->{type}' not implemented\n";
    }
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

sub vdisk_alloc {
    my ($cfg, $storeid, $vmid, $fmt, $name, $size) = @_;

    die "no storage id specified\n" if !$storeid;

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

	my ($vtype, $name, $vmid, undef, undef, $isBase, $format) =
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
		    die "base volume '$volname' is still in use " .
			"(use by '$tmpvolname')\n";
		}
	    }
	}
	$cleanup_worker = $plugin->free_image($storeid, $scfg, $volname, $isBase, $format);
    });

    return if !$cleanup_worker;

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();

    $rpcenv->fork_worker('imgdel', undef, $authuser, $cleanup_worker);
}

#list iso or openvz template ($tt = <iso|vztmpl|backup>)
sub template_list {
    my ($cfg, $storeid, $tt) = @_;

    die "unknown template type '$tt'\n"
	if !($tt eq 'iso' || $tt eq 'vztmpl' || $tt eq 'backup');

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # query the storage

    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;

	my $scfg = $ids->{$sid};
	my $type = $scfg->{type};

	next if !storage_check_enabled($cfg, $sid, undef, 1);

	next if $tt eq 'iso' && !$scfg->{content}->{iso};
	next if $tt eq 'vztmpl' && !$scfg->{content}->{vztmpl};
	next if $tt eq 'backup' && !$scfg->{content}->{backup};

	activate_storage($cfg, $sid);

	if ($scfg->{path}) {
	    my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

	    my $path = $plugin->get_subdir($scfg, $tt);

	    foreach my $fn (<$path/*>) {

		my $info;

		if ($tt eq 'iso') {
		    next if $fn !~ m!/([^/]+\.[Ii][Ss][Oo])$!;

		    $info = { volid => "$sid:iso/$1", format => 'iso' };

		} elsif ($tt eq 'vztmpl') {
		    next if $fn !~ m!/([^/]+\.tar\.([gx]z))$!;

		    $info = { volid => "$sid:vztmpl/$1", format => "t$2" };

		} elsif ($tt eq 'backup') {
		    next if $fn !~ m!/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz|vma|vma\.gz|vma\.lzo))$!;

		    $info = { volid => "$sid:backup/$1", format => $2 };
		}

		$info->{size} = -s $fn;

		push @{$res->{$sid}}, $info;
	    }

	}

	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}


sub vdisk_list {
    my ($cfg, $storeid, $vmid, $vollist) = @_;

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

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
	    push @$storage_list, $sid;
	}
    }

    my $cache = {};

    activate_storage_list($cfg, $storage_list, $cache);

    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;
	next if !storage_check_enabled($cfg, $sid, undef, 1);

	my $scfg = $ids->{$sid};
	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	$res->{$sid} = $plugin->list_images($sid, $scfg, $vmid, $vollist, $cache);
	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}

sub volume_list {
    my ($cfg, $storeid, $vmid, $content) = @_;

    my @ctypes = qw(images vztmpl iso backup);

    my $cts = $content ? [ $content ] : [ @ctypes ];

    my $scfg = PVE::Storage::storage_config($cfg, $storeid);

    my $res = [];
    foreach my $ct (@$cts) {
	my $data;
	if ($ct eq 'images') {
	    $data = vdisk_list($cfg, $storeid, $vmid);
	} elsif ($ct eq 'iso' && !defined($vmid)) {
	    $data = template_list($cfg, $storeid, 'iso');
	} elsif ($ct eq 'vztmpl'&& !defined($vmid)) {
	    $data = template_list ($cfg, $storeid, 'vztmpl');
	} elsif ($ct eq 'backup') {
	    $data = template_list ($cfg, $storeid, 'backup');
	    foreach my $item (@{$data->{$storeid}}) {
		if (defined($vmid)) {
		    @{$data->{$storeid}} = grep { $_->{volid} =~ m/\S+-$vmid-\S+/ } @{$data->{$storeid}};
		}
	    }
	}

	next if !$data || !$data->{$storeid};

	foreach my $item (@{$data->{$storeid}}) {
	    $item->{content} = $ct;
	    push @$res, $item;
	}
    }

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
	my $timeout = 30;
	system ("$UDEVADM settle --timeout=$timeout"); # ignore errors
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

    die "volume deativation failed: " . join(' ', @errlist)
	if scalar(@errlist);
}

sub storage_info {
    my ($cfg, $content) = @_;

    my $ids = $cfg->{ids};

    my $info = {};
    
    my @ctypes = PVE::Tools::split_list($content);
    
    my $slist = [];
    foreach my $storeid (keys %$ids) {

	next if !storage_check_enabled($cfg, $storeid, undef, 1);

	if (defined($content)) {
	    my $want_ctype = 0;
	    foreach my $ctype (@ctypes) {
		if ($ids->{$storeid}->{content}->{$ctype}) {
		    $want_ctype = 1;
		    last;
		}
	    }
	    next if !$want_ctype;
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
	};

	push @$slist, $storeid;
    }

    my $cache = {};

    foreach my $storeid (keys %$ids) {
	my $scfg = $ids->{$storeid};
	next if !$info->{$storeid};

	eval { activate_storage($cfg, $storeid, $cache); };
	if (my $err = $@) {
	    warn $err;
	    next;
	}

	my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});
	my ($total, $avail, $used, $active);
	eval { ($total, $avail, $used, $active) = $plugin->status($storeid, $scfg, $cache); };
	warn $@ if $@;
	next if !$active;
	$info->{$storeid}->{total} = $total;
	$info->{$storeid}->{avail} = $avail;
	$info->{$storeid}->{used} = $used;
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

sub scan_zfs {

    my $cmd = ['zfs',  'list', '-t', 'filesystem', '-H', '-o', 'name,avail,used'];

    my $res = [];
    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~m/^(\S+)\s+(\S+)\s+(\S+)$/) {
	    my ($pool, $size_str, $used_str) = ($1, $2, $3);
	    my $size = PVE::Storage::ZFSPoolPlugin::zfs_parse_size($size_str);
	    my $used = PVE::Storage::ZFSPoolPlugin::zfs_parse_size($used_str);
	    # ignore subvolumes generated by our ZFSPoolPlugin
	    return if $pool =~ m!/subvol-\d+-[^/]+$!; 
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

# idea is from usbutils package (/usr/bin/usb-devices) script
sub __scan_usb_device {
    my ($res, $devpath, $parent, $level) = @_;

    return if ! -d $devpath;
    return if $level && $devpath !~ m/^.*[-.](\d+)$/;
    my $port = $level ? int($1 - 1) : 0;

    my $busnum = int(file_read_firstline("$devpath/busnum"));
    my $devnum = int(file_read_firstline("$devpath/devnum"));

    my $d = {
	port => $port,
	level => $level,
	busnum => $busnum,
	devnum => $devnum,
	speed => file_read_firstline("$devpath/speed"),
	class => hex(file_read_firstline("$devpath/bDeviceClass")),
	vendid => file_read_firstline("$devpath/idVendor"),
	prodid => file_read_firstline("$devpath/idProduct"),
    };

    if ($level) {
	my $usbpath = $devpath;
	$usbpath =~ s|^.*/\d+\-||;
	$d->{usbpath} = $usbpath;
    }

    my $product = file_read_firstline("$devpath/product");
    $d->{product} = $product if $product;

    my $manu = file_read_firstline("$devpath/manufacturer");
    $d->{manufacturer} = $manu if $manu;

    my $serial => file_read_firstline("$devpath/serial");
    $d->{serial} = $serial if $serial;

    push @$res, $d;

    foreach my $subdev (<$devpath/$busnum-*>) {
	next if $subdev !~ m|/$busnum-[0-9]+(\.[0-9]+)*$|;
	__scan_usb_device($res, $subdev, $devnum, $level + 1);
    }

};

sub scan_usb {

    my $devlist = [];

    foreach my $device (</sys/bus/usb/devices/usb*>) {
	__scan_usb_device($devlist, $device, 0, 0);
    }

    return $devlist;
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

    return [qw(rootdir images vztmpl iso backup)];
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

1;
