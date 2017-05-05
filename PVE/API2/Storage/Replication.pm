package PVE::API2::Storage::Replication;

use warnings;
use strict;

use PVE::JSONSchema qw(get_standard_option);
use PVE::ReplicationTools;

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'index',
    path => '',
    method => 'GET',
    permissions => { user => 'all' },
    description => "Directory index.",
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

	return [
	    { name => 'jobs' },
	];
    }});


__PACKAGE__->register_method ({
    name => 'jobs',
    path => 'jobs',
    method => 'GET',
    description => "List replication jobs.",
    permissions => {
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
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
	links => [ { rel => 'child', href => "{vmid}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $jobs = PVE::ReplicationTools::get_all_jobs();

	return PVE::RESTHandler::hash_to_array($jobs, 'vmid');
    }});

1;
