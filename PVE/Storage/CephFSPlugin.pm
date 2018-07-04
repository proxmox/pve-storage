package PVE::Storage::CephFSPlugin;

use strict;
use warnings;

use IO::File;
use Net::IP;
use File::Path;

use PVE::Tools qw(run_command);
use PVE::ProcFSTools;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Storage::CephTools;

use base qw(PVE::Storage::Plugin);

sub cephfs_is_mounted {
    my ($scfg, $storeid, $mountdata) = @_;

    my $cmd_option = PVE::Storage::CephTools::ceph_connect_option($scfg, $storeid);
    my $configfile = $cmd_option->{ceph_conf};
    my $server = $cmd_option->{mon_host} // PVE::Storage::CephTools::get_monaddr_list($configfile);

    my $subdir = $scfg->{subdir} // '/';
    my $mountpoint = $scfg->{path};
    my $source = "$server:$subdir";

    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;
    return $mountpoint if grep {
	$_->[2] =~ m#^ceph|fuse\.ceph-fuse# &&
	$_->[0] =~ m#^\Q$source\E|ceph-fuse$# &&
	$_->[1] eq $mountpoint
    } @$mountdata;

    warn "A filesystem is already mounted on $mountpoint\n"
	if grep { $_->[1] eq $mountpoint } @$mountdata;

    return undef;
}

sub cephfs_mount {
    my ($scfg, $storeid) = @_;

    my $cmd;
    my $mountpoint = $scfg->{path};
    my $subdir = $scfg->{subdir} // '/';

    my $cmd_option = PVE::Storage::CephTools::ceph_connect_option($scfg, $storeid);
    my $configfile = $cmd_option->{ceph_conf};
    my $secretfile = $cmd_option->{keyring};
    my $server = $cmd_option->{mon_host} // PVE::Storage::CephTools::get_monaddr_list($configfile);

    # fuse -> client-enforced quotas (kernel doesn't), updates w/ ceph-fuse pkg
    # kernel -> better performance, less frequent updates
    if ($scfg->{fuse}) {
	    # FIXME: ceph-fuse client complains about missing ceph.conf or keyring if
	    # not provided on its default locations but still connects. Fix upstream??
	    $cmd = ['/usr/bin/ceph-fuse', '-n', "client.$cmd_option->{userid}", '-m', $server];
	    push @$cmd, '--keyfile', $secretfile if defined($secretfile);
	    push @$cmd, '-r', $subdir if !($subdir =~ m|^/$|);
	    push @$cmd, $mountpoint;
	    push @$cmd, '--conf', $configfile if defined($configfile);
    } else {
	my $source = "$server:$subdir";
	$cmd = ['/bin/mount', '-t', 'ceph', $source, $mountpoint, '-o', "name=$cmd_option->{userid}"];
	push @$cmd, '-o', "secretfile=$secretfile" if defined($secretfile);
    }

    if ($scfg->{options}) {
	push @$cmd, '-o', $scfg->{options};
    }

    run_command($cmd, errmsg => "mount error");
}

# Configuration

sub type {
    return 'cephfs';
}

sub plugindata {
    return {
	content => [ { vztmpl => 1, iso => 1, backup => 1},
		     { backup => 1 }],
    };
}

sub properties {
    return {
	fuse => {
	    description => "Mount CephFS through FUSE.",
	    type => 'boolean',
	},
	subdir => {
	    description => "Subdir to mount.",
	    type => 'string', format => 'pve-storage-path',
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	monhost => { optional => 1},
	nodes => { optional => 1 },
	subdir => { optional => 1 },
	disable => { optional => 1 },
	options => { optional => 1 },
	username => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
	mkdir => { optional => 1 },
	fuse => { optional => 1 },
	bwlimit => { optional => 1 },
    };
}

sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} = "/mnt/pve/$sectionId" if $create && !$config->{path};

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

# Storage implementation

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    return if defined($scfg->{monhost}); # nothing to do if not pve managed ceph

    PVE::Storage::CephTools::ceph_create_keyfile($scfg->{type}, $storeid);
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    return if defined($scfg->{monhost}); # nothing to do if not pve managed ceph

    PVE::Storage::CephTools::ceph_remove_keyfile($scfg->{type}, $storeid);
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} //= PVE::ProcFSTools::parse_proc_mounts();

    return undef if !cephfs_is_mounted($scfg, $storeid, $cache->{mountdata});

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} //= PVE::ProcFSTools::parse_proc_mounts();

    # NOTE: mkpath may hang if storage is mounted but not reachable
    if (!cephfs_is_mounted($scfg, $storeid, $cache->{mountdata})) {
	my $path = $scfg->{path};

	mkpath $path if !(defined($scfg->{mkdir}) && !$scfg->{mkdir});

	die "unable to activate storage '$storeid' - " .
	    "directory '$path' does not exist\n" if ! -d $path;

	cephfs_mount($scfg, $storeid);
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} //= PVE::ProcFSTools::parse_proc_mounts();

    my $path = $scfg->{path};

    if (cephfs_is_mounted($scfg, $storeid, $cache->{mountdata})) {
	run_command(['/bin/umount', $path], errmsg => 'umount error');
    }
}

1;
