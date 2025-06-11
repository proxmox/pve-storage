package PVE::Storage::CephFSPlugin;

use strict;
use warnings;

use IO::File;
use Net::IP;
use File::Path;

use PVE::CephConfig;
use PVE::JSONSchema qw(get_standard_option);
use PVE::ProcFSTools;
use PVE::Storage::Plugin;
use PVE::Systemd;
use PVE::Tools qw(run_command file_set_contents);

use base qw(PVE::Storage::Plugin);

sub cephfs_is_mounted {
    my ($scfg, $storeid, $mountdata) = @_;

    my $cmd_option = PVE::CephConfig::ceph_connect_option($scfg, $storeid);
    my $configfile = $cmd_option->{ceph_conf};

    my $subdir = $scfg->{subdir} // '/';
    my $mountpoint = $scfg->{path};

    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;
    return $mountpoint if grep {
        $_->[2] =~ m#^ceph|fuse\.ceph-fuse#
            && $_->[0] =~ m#\Q:$subdir\E$|^ceph-fuse$#
            && $_->[1] eq $mountpoint
    } @$mountdata;

    warn "A filesystem is already mounted on $mountpoint\n"
        if grep { $_->[1] eq $mountpoint } @$mountdata;

    return undef;
}

# FIXME: remove once it's possible to specify _netdev for fuse.ceph mounts
sub systemd_netmount {
    my ($where, $type, $what, $opts) = @_;

    # don't do default deps, systemd v241 generator produces ordering deps on both
    # local-fs(-pre) and remote-fs(-pre) targets if we use the required _netdev
    # option. Over three corners this gets us an ordering cycle on shutdown, which
    # may make shutdown hang if the random cycle breaking hits the "wrong" unit to
    # delete.
    my $unit = <<"EOF";
[Unit]
Description=${where}
DefaultDependencies=no
Requires=system.slice
Wants=network-online.target
Before=umount.target remote-fs.target
After=systemd-journald.socket system.slice network.target -.mount remote-fs-pre.target network-online.target
Conflicts=umount.target

[Mount]
Where=${where}
What=${what}
Type=${type}
Options=${opts}
EOF

    my $unit_fn = PVE::Systemd::escape_unit($where, 1) . ".mount";
    my $unit_path = "/run/systemd/system/$unit_fn";
    my $daemon_needs_reload = -e $unit_path;

    file_set_contents($unit_path, $unit);

    run_command(['systemctl', 'daemon-reload'], errmsg => "daemon-reload error")
        if $daemon_needs_reload;
    run_command(['systemctl', 'start', $unit_fn], errmsg => "mount error");

}

sub cephfs_mount {
    my ($scfg, $storeid) = @_;

    my $mountpoint = $scfg->{path};
    my $subdir = $scfg->{subdir} // '/';

    my $cmd_option = PVE::CephConfig::ceph_connect_option($scfg, $storeid);
    my $configfile = $cmd_option->{ceph_conf};
    my $secretfile = $cmd_option->{keyring};
    my $server = $cmd_option->{mon_host} // PVE::CephConfig::get_monaddr_list($configfile);
    my $type = 'ceph';
    my $fs_name = $scfg->{'fs-name'};

    my @opts = ();
    if ($scfg->{fuse}) {
        $type = 'fuse.ceph';
        push @opts, "ceph.id=$cmd_option->{userid}";
        push @opts, "ceph.keyfile=$secretfile" if defined($secretfile);
        push @opts, "ceph.conf=$configfile" if defined($configfile);
        push @opts, "ceph.client_fs=$fs_name" if defined($fs_name);
    } else {
        push @opts, "name=$cmd_option->{userid}";
        push @opts, "secretfile=$secretfile" if defined($secretfile);
        push @opts, "conf=$configfile" if defined($configfile);
        push @opts, "fs=$fs_name" if defined($fs_name);
    }

    push @opts, $scfg->{options} if $scfg->{options};

    systemd_netmount($mountpoint, $type, "$server:$subdir", join(',', @opts));
}

# Configuration

sub type {
    return 'cephfs';
}

sub plugindata {
    return {
        content =>
            [{ vztmpl => 1, iso => 1, backup => 1, snippets => 1, import => 1 }, { backup => 1 }],
        'sensitive-properties' => { keyring => 1 },
    };
}

sub properties {
    return {
        fuse => {
            description => "Mount CephFS through FUSE.",
            type => 'boolean',
        },
        'fs-name' => {
            description => "The Ceph filesystem name.",
            type => 'string',
            format => 'pve-configid',
        },
    };
}

sub options {
    return {
        path => { fixed => 1 },
        'content-dirs' => { optional => 1 },
        monhost => { optional => 1 },
        nodes => { optional => 1 },
        subdir => { optional => 1 },
        disable => { optional => 1 },
        options => { optional => 1 },
        username => { optional => 1 },
        content => { optional => 1 },
        format => { optional => 1 },
        mkdir => { optional => 1 },
        'create-base-path' => { optional => 1 },
        'create-subdirs' => { optional => 1 },
        fuse => { optional => 1 },
        bwlimit => { optional => 1 },
        maxfiles => { optional => 1 },
        keyring => { optional => 1 },
        'prune-backups' => { optional => 1 },
        'max-protected-backups' => { optional => 1 },
        'fs-name' => { optional => 1 },
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

    PVE::CephConfig::ceph_create_keyfile($scfg->{type}, $storeid, $param{keyring});

    return;
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (exists($param{keyring})) {
        if (defined($param{keyring})) {
            PVE::CephConfig::ceph_create_keyfile($scfg->{type}, $storeid, $param{keyring});
        } else {
            PVE::CephConfig::ceph_remove_keyfile($scfg->{type}, $storeid);
        }
    }

    return;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;
    PVE::CephConfig::ceph_remove_keyfile($scfg->{type}, $storeid);
    return;
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

        $class->config_aware_base_mkdir($scfg, $path);

        die "unable to activate storage '$storeid' - " . "directory '$path' does not exist\n"
            if !-d $path;

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
