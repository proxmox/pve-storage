package PVE::API2::Storage::FileRestore;

use strict;
use warnings;

use MIME::Base64;
use PVE::Exception qw(raise_param_exc);
use PVE::JSONSchema qw(get_standard_option);
use PVE::PBSClient;
use PVE::Storage;
use PVE::Tools qw(extract_param);

use PVE::RESTHandler;
use base qw(PVE::RESTHandler);

my $parse_volname_or_id = sub {
    my ($storeid, $volume) = @_;

    my $volid;
    my ($sid, $volname) = PVE::Storage::parse_volume_id($volume, 1);

    if (defined($sid)) {
        raise_param_exc({ volume => "storage ID mismatch ($sid != $storeid)." })
            if $sid ne $storeid;

        $volid = $volume;
    } elsif ($volume =~ m/^backup\//) {
        $volid = "$storeid:$volume";
    } else {
        $volid = "$storeid:backup/$volume";
    }

    return $volid;
};

__PACKAGE__->register_method({
    name => 'list',
    path => 'list',
    method => 'GET',
    proxyto => 'node',
    permissions => {
        description => "You need read access for the volume.",
        user => 'all',
    },
    description => "List files and directories for single file restore under the given path.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option(
                'pve-storage-id',
                {
                    completion => \&PVE::Storage::complete_storage_enabled,
                },
            ),
            volume => {
                description =>
                    "Backup volume ID or name. Currently only PBS snapshots are supported.",
                type => 'string',
                completion => \&PVE::Storage::complete_volume,
            },
            filepath => {
                description => 'base64-path to the directory or file being listed, or "/".',
                type => 'string',
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                filepath => {
                    description => "base64 path of the current entry",
                    type => 'string',
                },
                type => {
                    description => "Entry type.",
                    type => 'string',
                },
                text => {
                    description => "Entry display text.",
                    type => 'string',
                },
                leaf => {
                    description => "If this entry is a leaf in the directory graph.",
                    type => 'boolean',
                },
                size => {
                    description => "Entry file size.",
                    type => 'integer',
                    optional => 1,
                },
                mtime => {
                    description => "Entry last-modified time (unix timestamp).",
                    type => 'integer',
                    optional => 1,
                },
            },
        },
    },
    protected => 1,
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $user = $rpcenv->get_user();

        my $path = extract_param($param, 'filepath') || "/";
        my $base64 = $path ne "/";

        my $storeid = extract_param($param, 'storage');

        my $volid = $parse_volname_or_id->($storeid, $param->{volume});
        my $cfg = PVE::Storage::config();
        my $scfg = PVE::Storage::storage_config($cfg, $storeid);

        PVE::Storage::check_volume_access($rpcenv, $user, $cfg, undef, $volid, 'backup');

        raise_param_exc({ 'storage' => "Only PBS storages supported for file-restore." })
            if $scfg->{type} ne 'pbs';

        my (undef, $snap) = PVE::Storage::parse_volname($cfg, $volid);

        my $client = PVE::PBSClient->new($scfg, $storeid);
        my $ret = $client->file_restore_list($snap, $path, $base64, { timeout => 25 });

        if (ref($ret) eq "HASH") {
            my $msg = $ret->{message};
            if (my $code = $ret->{code}) {
                die PVE::Exception->new("$msg\n", code => $code);
            } else {
                die "$msg\n";
            }
        } elsif (ref($ret) eq "ARRAY") {
            # 'leaf' is a proper JSON boolean, map to perl-y bool
            # TODO: make PBSClient decode all bools always as 1/0?
            foreach my $item (@$ret) {
                $item->{leaf} = $item->{leaf} ? 1 : 0;
            }

            return $ret;
        }

        die "invalid proxmox-file-restore output";
    },
});

__PACKAGE__->register_method({
    name => 'download',
    path => 'download',
    method => 'GET',
    proxyto => 'node',
    download_allowed => 1,
    permissions => {
        description => "You need read access for the volume.",
        user => 'all',
    },
    description => "Extract a file or directory (as zip archive) from a PBS backup.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            storage => get_standard_option(
                'pve-storage-id',
                {
                    completion => \&PVE::Storage::complete_storage_enabled,
                },
            ),
            volume => {
                description =>
                    "Backup volume ID or name. Currently only PBS snapshots are supported.",
                type => 'string',
                completion => \&PVE::Storage::complete_volume,
            },
            filepath => {
                description => 'base64-path to the directory or file to download.',
                type => 'string',
            },
            tar => {
                description => "Download dirs as 'tar.zst' instead of 'zip'.",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
        },
    },
    returns => {
        type => 'any', # download
    },
    protected => 1,
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $user = $rpcenv->get_user();

        my $path = extract_param($param, 'filepath');
        my $storeid = extract_param($param, 'storage');
        my $volid = $parse_volname_or_id->($storeid, $param->{volume});
        my $tar = extract_param($param, 'tar') // 0;

        my $cfg = PVE::Storage::config();
        my $scfg = PVE::Storage::storage_config($cfg, $storeid);

        PVE::Storage::check_volume_access($rpcenv, $user, $cfg, undef, $volid, 'backup');

        raise_param_exc({ 'storage' => "Only PBS storages supported for file-restore." })
            if $scfg->{type} ne 'pbs';

        my (undef, $snap) = PVE::Storage::parse_volname($cfg, $volid);

        my $client = PVE::PBSClient->new($scfg, $storeid);
        my $fifo = $client->file_restore_extract_prepare();

        $rpcenv->fork_worker(
            'pbs-download',
            undef,
            $user,
            sub {
                my $name = decode_base64($path);
                print "Starting download of file: $name\n";
                $client->file_restore_extract($fifo, $snap, $path, 1, $tar);
            },
        );

        my $ret = {
            download => {
                path => $fifo,
                stream => 1,
                'content-type' => 'application/octet-stream',
            },
        };
        return $ret;
    },
});

1;
