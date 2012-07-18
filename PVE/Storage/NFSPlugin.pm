package PVE::Storage::NFSPlugin;

use strict;
use warnings;
use IO::File;
use File::Path;
use PVE::Tools qw(run_command);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# NFS helper functions

sub read_proc_mounts {
    
    local $/; # enable slurp mode
    
    my $data = "";
    if (my $fd = IO::File->new("/proc/mounts", "r")) {
	$data = <$fd>;
	close ($fd);
    }

    return $data;
}

sub nfs_is_mounted {
    my ($server, $export, $mountpoint, $mountdata) = @_;

    my $source = "$server:$export";

    $mountdata = read_proc_mounts() if !$mountdata;

    if ($mountdata =~ m|^$source/?\s$mountpoint\snfs|m) {
	return $mountpoint;
    } 

    return undef;
}

sub nfs_mount {
    my ($server, $export, $mountpoint, $options) = @_;

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

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $export = $scfg->{export};

    return undef if !nfs_is_mounted($server, $export, $path, $cache->{mountdata}); 

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $export = $scfg->{export};

    if (!nfs_is_mounted($server, $export, $path, $cache->{mountdata})) {    
		    
	# NOTE: only call mkpath when not mounted (avoid hang 
	# when NFS server is offline 
		    
	mkpath $path;

	die "unable to activate storage '$storeid' - " .
	    "directory '$path' does not exist\n" if ! -d $path;

	nfs_mount($server, $export, $path, $scfg->{options});
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

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
    my $p = Net::Ping->new();
    return $p->ping($server, 2);

}
1;
