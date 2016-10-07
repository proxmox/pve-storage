package PVE::API2::Disks;

use strict;
use warnings;

use PVE::SafeSyslog;
use PVE::Diskmanage;
use HTTP::Status qw(:constants);
use PVE::JSONSchema qw(get_standard_option);

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

use Data::Dumper;

__PACKAGE__->register_method ({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    permissions => { user => 'all' },
    description => "Node index.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {},
	},
	links => [ { rel => 'child', href => "{name}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $result = [
	    { name => 'list' },
	    { name => 'initgpt' },
	    { name => 'smart' },
	    ];

	return $result;
    }});

__PACKAGE__->register_method ({
    name => 'list',
    path => 'list',
    method => 'GET',
    description => "List local disks.",
    protected => 1,
    proxyto => 'node',
    permissions => {
	check => ['perm', '/', ['Sys.Audit', 'Datastore.Audit'], any => 1],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => 'object',
	    properties => {
		devpath => {
		    type => 'string',
		    description => 'The device path',
		},
		used => { type => 'string', optional => 1 },
		gpt => { type => 'boolean' },
		size => { type => 'integer'},
		osdid => { type => 'integer'},
		vendor =>  { type => 'string', optional => 1 },
		model =>  { type => 'string', optional => 1 },
		serial =>  { type => 'string', optional => 1 },
		wwn => { type => 'string', optional => 1},
		health => { type => 'string', optional => 1},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $disks = PVE::Diskmanage::get_disks();

	my $result = [];

	foreach my $disk (sort keys %$disks) {
	    my $entry = $disks->{$disk};
	    push @$result, $entry;
	}
	return $result;
    }});

__PACKAGE__->register_method ({
    name => 'smart',
    path => 'smart',
    method => 'GET',
    description => "Get SMART Health of a disk.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/', ['Sys.Audit', 'Datastore.Audit'], any => 1],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    disk => {
		type => 'string',
		pattern => '^/dev/[a-zA-Z0-9\/]+$',
		description => "Block device name",
	    },
	    healthonly => {
		type => 'boolean',
		description => "If true returns only the health status",
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'object',
	properties => {
	    health => { type => 'string' },
	    type => { type => 'string', optional => 1 },
	    attributes => { type => 'array', optional => 1},
	    text => { type => 'string', optional => 1 },
	},
    },
    code => sub {
	my ($param) = @_;

	my $disk = PVE::Diskmanage::verify_blockdev_path($param->{disk});

	my $result = PVE::Diskmanage::get_smart_data($disk, $param->{healthonly});

	$result->{health} = 'UNKNOWN' if !defined $result->{health};
	$result = { health => $result->{health} } if $param->{healthonly};

	return $result;
    }});

__PACKAGE__->register_method ({
    name => 'initgpt',
    path => 'initgpt',
    method => 'POST',
    description => "Initialize Disk with GPT",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    disk => {
		type => 'string',
		description => "Block device name",
		pattern => '^/dev/[a-zA-Z0-9\/]+$',
	    },
	    uuid => {
		type => 'string',
		description => 'UUID for the GPT table',
		pattern => '[a-fA-F0-9\-]+',
		maxLength => 36,
		optional => 1,
	    },
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $disk = PVE::Diskmanage::verify_blockdev_path($param->{disk});

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	die "disk $disk already in use\n" if PVE::Diskmanage::disk_is_used($disk);
	my $worker = sub {
	    PVE::Diskmanage::init_disk($disk, $param->{uuid});
	};

	my $diskid = $disk;
	$diskid =~ s|^.*/||; # remove all up to the last slash
	return $rpcenv->fork_worker('diskinit', $diskid, $authuser, $worker);
    }});

1;
