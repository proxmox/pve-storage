package PVE::Storage::NFSPlugin;

use strict;
use warnings;
use IO::File;
use Net::IP;
use File::Path;

use PVE::Network;
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
        $_->[2] =~ /^nfs/
            && $_->[0] =~ m|^\Q$source\E/?$|
            && $_->[1] eq $mountpoint
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
        content => [
            {
                images => 1,
                rootdir => 1,
                vztmpl => 1,
                iso => 1,
                backup => 1,
                snippets => 1,
                import => 1,
            },
            { images => 1 },
        ],
        format => [{ raw => 1, qcow2 => 1, vmdk => 1 }, 'raw'],
        'sensitive-properties' => {},
    };
}

sub properties {
    return {
        export => {
            description => "NFS export path.",
            type => 'string',
            format => 'pve-storage-path',
        },
        server => {
            description => "Server IP or DNS name.",
            type => 'string',
            format => 'pve-storage-server',
        },
    };
}

sub options {
    return {
        path => { fixed => 1 },
        'content-dirs' => { optional => 1 },
        server => { fixed => 1 },
        export => { fixed => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        maxfiles => { optional => 1 },
        'prune-backups' => { optional => 1 },
        'max-protected-backups' => { optional => 1 },
        options => { optional => 1 },
        content => { optional => 1 },
        format => { optional => 1 },
        mkdir => { optional => 1 },
        'create-base-path' => { optional => 1 },
        'create-subdirs' => { optional => 1 },
        bwlimit => { optional => 1 },
        preallocation => { optional => 1 },
        'snapshot-as-volume-chain' => { optional => 1, fixed => 1 },
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
        # NOTE: only call mkpath when not mounted (avoid hang when NFS server is offline
        $class->config_aware_base_mkdir($scfg, $path);

        die "unable to activate storage '$storeid' - " . "directory '$path' does not exist\n"
            if !-d $path;

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
    my $opts = $scfg->{options};

    my $cmd;

    my $is_v4 = defined($opts) && $opts =~ /vers=4.*/;
    if ($is_v4) {
        my $ip = PVE::JSONSchema::pve_verify_ip($server, 1);
        if (!defined($ip)) {
            $ip = PVE::Network::get_ip_from_hostname($server);
        }

        my $transport = PVE::JSONSchema::pve_verify_ipv4($ip, 1) ? 'tcp' : 'tcp6';

        # nfsv4 uses a pseudo-filesystem always beginning with /
        # no exports are listed
        $cmd = ['/usr/sbin/rpcinfo', '-T', $transport, $ip, 'nfs', '4'];
    } else {
        $cmd = ['/sbin/showmount', '--no-headers', '--exports', $server];
    }

    eval {
        run_command($cmd, timeout => 10, outfunc => sub { }, errfunc => sub { });
    };
    if (my $err = $@) {
        if ($is_v4) {
            my $port = 2049;
            $port = $1 if defined($opts) && $opts =~ /port=(\d+)/;

            # rpcinfo is expected to work when the port is 0 (see 'man 5 nfs') and tcp_ping()
            # defaults to port 7 when passing in 0.
            return 0 if $port == 0;

            return PVE::Network::tcp_ping($server, $port, 2);
        }
        return 0;
    }

    return 1;
}

# FIXME remove on the next APIAGE reset.
# Deprecated, use get_volume_attribute instead.
sub get_volume_notes {
    my $class = shift;
    PVE::Storage::DirPlugin::get_volume_notes($class, @_);
}

# FIXME remove on the next APIAGE reset.
# Deprecated, use update_volume_attribute instead.
sub update_volume_notes {
    my $class = shift;
    PVE::Storage::DirPlugin::update_volume_notes($class, @_);
}

sub get_volume_attribute {
    return PVE::Storage::DirPlugin::get_volume_attribute(@_);
}

sub update_volume_attribute {
    return PVE::Storage::DirPlugin::update_volume_attribute(@_);
}

sub get_import_metadata {
    return PVE::Storage::DirPlugin::get_import_metadata(@_);
}

1;
