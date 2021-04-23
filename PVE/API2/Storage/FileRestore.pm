package PVE::API2::Storage::FileRestore;

use strict;
use warnings;

use MIME::Base64;
use PVE::JSONSchema qw(get_standard_option);
use PVE::PBSClient;
use PVE::Storage;
use PVE::Tools qw(extract_param);

use PVE::RESTHandler;
use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
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
	    storage => get_standard_option('pve-storage-id'),
	    snapshot => {
		description => "Backup snapshot identifier.",
		type => 'string',
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
	my $snap = extract_param($param, 'snapshot');
	my $storeid = extract_param($param, 'storage');
	my $cfg = PVE::Storage::config();
	my $scfg = PVE::Storage::storage_config($cfg, $storeid);

	my $volid = "$storeid:backup/$snap";
	PVE::Storage::check_volume_access($rpcenv, $user, $cfg, undef, $volid);

	my $client = PVE::PBSClient->new($scfg, $storeid);
	my $ret = $client->file_restore_list($snap, $path, $base64);


	# 'leaf' is a proper JSON boolean, map to perl-y bool
	# TODO: make PBSClient decode all bools always as 1/0?
	foreach my $item (@$ret) {
	    $item->{leaf} = $item->{leaf} ? 1 : 0;
	}

	return $ret;
    }});

__PACKAGE__->register_method ({
    name => 'download',
    path => 'download',
    method => 'GET',
    proxyto => 'node',
    permissions => {
	description => "You need read access for the volume.",
	user => 'all',
    },
    description => "Extract a file or directory (as zip archive) from a PBS backup.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    snapshot => {
		description => "Backup snapshot identifier.",
		type => 'string',
	    },
	    filepath => {
		description => 'base64-path to the directory or file to download.',
		type => 'string',
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
	my $snap = extract_param($param, 'snapshot');
	my $storeid = extract_param($param, 'storage');
	my $cfg = PVE::Storage::config();
	my $scfg = PVE::Storage::storage_config($cfg, $storeid);

	my $volid = "$storeid:backup/$snap";
	PVE::Storage::check_volume_access($rpcenv, $user, $cfg, undef, $volid);

	my $client = PVE::PBSClient->new($scfg, $storeid);
	my $fifo = $client->file_restore_extract_prepare();

	$rpcenv->fork_worker('pbs-download', undef, $user, sub {
	    my $name = decode_base64($path);
	    print "Starting download of file: $name\n";
	    $client->file_restore_extract($fifo, $snap, $path, 1);
	});

	my $ret = {
	    download => {
		path => $fifo,
		stream => 1,
		'content-type' => 'application/octet-stream',
	    },
	};
	return $ret;
    }});

1;
