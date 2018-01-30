package PVE::Storage::NFSPlugin;

use strict;
use warnings;
use IO::File;
use Net::IP;
use File::Path;
use PVE::Tools qw(run_command);
use PVE::ProcFSTools;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# NFS helper functions

sub nfs_is_mounted {
    my ($server, $export, $mountpoint, $mountdata) = @_;

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "$server:$export";

    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;
    return $mountpoint if grep {
	$_->[2] =~ /^nfs/ &&
	$_->[0] =~ m|^\Q$source\E/?$| &&
	$_->[1] eq $mountpoint
    } @$mountdata;
    return undef;
}

sub nfs_mount {
    my ($server, $export, $mountpoint, $options) = @_;

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "$server:$export";

    my $cmd = ['/bin/mount', '-t', 'nfs', $source, $mountpoint];
    if ($options) {
	push @$cmd, '-o', $options;
    } 

    run_command($cmd, errmsg => "mount error");
}

# Configuration

sub type {
    return 'nfs';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1},
		     { images => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    };
}   

sub properties {
    return {
	export => {
	    description => "NFS export path.",
	    type => 'string', format => 'pve-storage-path',
	},
	server => {
	    description => "Server IP or DNS name.",
	    type => 'string', format => 'pve-storage-server',
	},
	options => {
	    description => "NFS mount options (see 'man nfs')",
	    type => 'string',  format => 'pve-storage-options',
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	server => { fixed => 1 },
	export => { fixed => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
        maxfiles => { optional => 1 },
	options => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
	mkdir => { optional => 1 },
	bwlimit => { optional => 1 },
    };
}


sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} = "/mnt/pve/$sectionId" if $create && !$config->{path};

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

# Storage implementation

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $export = $scfg->{export};

    return undef if !nfs_is_mounted($server, $export, $path, $cache->{mountdata}); 

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $export = $scfg->{export};

    if (!nfs_is_mounted($server, $export, $path, $cache->{mountdata})) {    
		    
	# NOTE: only call mkpath when not mounted (avoid hang 
	# when NFS server is offline 
		    
	mkpath $path if !(defined($scfg->{mkdir}) && !$scfg->{mkdir});

	die "unable to activate storage '$storeid' - " .
	    "directory '$path' does not exist\n" if ! -d $path;

	nfs_mount($server, $export, $path, $scfg->{options});
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $export = $scfg->{export};

    if (nfs_is_mounted($server, $export, $path, $cache->{mountdata})) {    
	my $cmd = ['/bin/umount', $path];
	run_command($cmd, errmsg => 'umount error'); 
    }
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    my $server = $scfg->{server};

    my $cmd = ['/sbin/showmount', '--no-headers', '--exports', $server];

    eval {
	run_command($cmd, timeout => 2, outfunc => sub {}, errfunc => sub {});
    };
    if (my $err = $@) {
	return 0;
    }

    return 1;
}

1;
