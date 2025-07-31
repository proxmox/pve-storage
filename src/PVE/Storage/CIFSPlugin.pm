package PVE::Storage::CIFSPlugin;

use strict;
use warnings;
use Net::IP;
use PVE::Tools qw(run_command);
use PVE::ProcFSTools;
use File::Path;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# CIFS helper functions

sub cifs_is_mounted : prototype($$) {
    my ($scfg, $mountdata) = @_;

    my ($mountpoint, $server, $share) = $scfg->@{ 'path', 'server', 'share' };
    my $subdir = $scfg->{subdir} // '';

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "//${server}/$share$subdir";
    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;

    return $mountpoint if grep {
        $_->[2] =~ /^cifs/
            && $_->[0] =~ m|^\Q$source\E/?$|
            && $_->[1] eq $mountpoint
    } @$mountdata;
    return undef;
}

sub cifs_cred_file_name {
    my ($storeid) = @_;
    return "/etc/pve/priv/storage/${storeid}.pw";
}

sub cifs_delete_credentials {
    my ($storeid) = @_;

    if (my $cred_file = get_cred_file($storeid)) {
        unlink($cred_file) or warn "removing cifs credientials '$cred_file' failed: $!\n";
    }
}

sub cifs_set_credentials {
    my ($password, $storeid) = @_;

    my $cred_file = cifs_cred_file_name($storeid);
    mkdir "/etc/pve/priv/storage";

    PVE::Tools::file_set_contents($cred_file, "password=$password\n");

    return $cred_file;
}

sub get_cred_file {
    my ($storeid) = @_;

    my $cred_file = cifs_cred_file_name($storeid);

    if (-e $cred_file) {
        return $cred_file;
    }
    return undef;
}

sub cifs_mount : prototype($$$$$) {
    my ($scfg, $storeid, $smbver, $user, $domain) = @_;

    my ($mountpoint, $server, $share, $options) = $scfg->@{ 'path', 'server', 'share', 'options' };
    my $subdir = $scfg->{subdir} // '';

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "//${server}/$share$subdir";

    my $cmd = ['/bin/mount', '-t', 'cifs', $source, $mountpoint, '-o', 'soft', '-o'];

    if (my $cred_file = get_cred_file($storeid)) {
        push @$cmd, "username=$user", '-o', "credentials=$cred_file";
        push @$cmd, '-o', "domain=$domain" if defined($domain);
    } else {
        push @$cmd, 'guest,username=guest';
    }

    push @$cmd, '-o', defined($smbver) ? "vers=$smbver" : "vers=default";
    push @$cmd, '-o', $options if $options;

    run_command($cmd, errmsg => "mount error");
}

# Configuration

sub type {
    return 'cifs';
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
        'sensitive-properties' => { password => 1 },
    };
}

sub properties {
    return {
        share => {
            description => "CIFS share.",
            type => 'string',
        },
        password => {
            description => "Password for accessing the share/datastore.",
            type => 'string',
            maxLength => 256,
        },
        domain => {
            description => "CIFS domain.",
            type => 'string',
            optional => 1,
            maxLength => 256,
        },
        smbversion => {
            description =>
                "SMB protocol version. 'default' if not set, negotiates the highest SMB2+"
                . " version supported by both the client and server.",
            type => 'string',
            default => 'default',
            enum => ['default', '2.0', '2.1', '3', '3.0', '3.11'],
            optional => 1,
        },
    };
}

sub options {
    return {
        path => { fixed => 1 },
        'content-dirs' => { optional => 1 },
        server => { fixed => 1 },
        share => { fixed => 1 },
        subdir => { optional => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        'prune-backups' => { optional => 1 },
        'max-protected-backups' => { optional => 1 },
        content => { optional => 1 },
        format => { optional => 1 },
        username => { optional => 1 },
        password => { optional => 1 },
        domain => { optional => 1 },
        smbversion => { optional => 1 },
        mkdir => { optional => 1 },
        'create-base-path' => { optional => 1 },
        'create-subdirs' => { optional => 1 },
        bwlimit => { optional => 1 },
        preallocation => { optional => 1 },
        options => { optional => 1 },
        'snapshot-as-volume-chain' => { optional => 1, fixed => 1 },
    };
}

sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} = "/mnt/pve/$sectionId" if $create && !$config->{path};

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

# Storage implementation

sub on_add_hook {
    my ($class, $storeid, $scfg, %sensitive) = @_;

    if (defined($sensitive{password})) {
        cifs_set_credentials($sensitive{password}, $storeid);
        if (!exists($scfg->{username})) {
            warn "storage $storeid: ignoring password parameter, no user set\n";
        }
    } else {
        cifs_delete_credentials($storeid);
    }

    return;
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %sensitive) = @_;

    return if !exists($sensitive{password});

    if (defined($sensitive{password})) {
        cifs_set_credentials($sensitive{password}, $storeid);
        if (!exists($scfg->{username})) {
            warn "storage $storeid: ignoring password parameter, no user set\n";
        }
    } else {
        cifs_delete_credentials($storeid);
    }

    return;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    cifs_delete_credentials($storeid);

    return;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
        if !$cache->{mountdata};

    return undef
        if !cifs_is_mounted($scfg, $cache->{mountdata});

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
        if !$cache->{mountdata};

    my $path = $scfg->{path};

    if (!cifs_is_mounted($scfg, $cache->{mountdata})) {

        $class->config_aware_base_mkdir($scfg, $path);

        die "unable to activate storage '$storeid' - " . "directory '$path' does not exist\n"
            if !-d $path;

        cifs_mount($scfg, $storeid, $scfg->{smbversion}, $scfg->{username}, $scfg->{domain});
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
        if !$cache->{mountdata};

    my $path = $scfg->{path};

    if (cifs_is_mounted($scfg, $cache->{mountdata})) {
        my $cmd = ['/bin/umount', $path];
        run_command($cmd, errmsg => 'umount error');
    }
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    my $servicename = '//' . $scfg->{server} . '/' . $scfg->{share};

    my $cmd = ['/usr/bin/smbclient', $servicename, '-d', '0'];

    if (defined($scfg->{smbversion}) && $scfg->{smbversion} ne 'default') {
        # max-protocol version, so basically only relevant for smb2 vs smb3
        push @$cmd, '-m', "smb" . int($scfg->{smbversion});
    }

    if (my $cred_file = get_cred_file($storeid)) {
        push @$cmd, '-U', $scfg->{username}, '-A', $cred_file;
        push @$cmd, '-W', $scfg->{domain} if $scfg->{domain};
    } else {
        push @$cmd, '-U', 'Guest', '-N';
    }
    push @$cmd, '-c', 'echo 1 0';

    my $out_str;
    my $out = sub { $out_str .= shift };

    eval {
        run_command($cmd, timeout => 10, outfunc => $out, errfunc => sub { });
    };

    if (my $err = $@) {
        die "$out_str\n"
            if defined($out_str)
            && ($out_str =~ m/NT_STATUS_(ACCESS_DENIED|INVALID_PARAMETER|LOGON_FAILURE)/);
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

sub volume_qemu_snapshot_method {
    return PVE::Storage::DirPlugin::volume_qemu_snapshot_method(@_);
}

1;
