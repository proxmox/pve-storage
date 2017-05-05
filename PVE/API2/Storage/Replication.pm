package PVE::API2::Storage::Replication;

use warnings;
use strict;

use PVE::JSONSchema qw(get_standard_option);
use PVE::RPCEnvironment;
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
	description => "Only list jobs where you have VM.Audit permissons on /vms/<vmid>.",
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

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my $jobs = PVE::ReplicationTools::get_all_jobs();

	my $res = [];
	foreach my $vmid (sort keys %$jobs) {
	    next if !$rpcenv->check($authuser, "/vms/$vmid", [ 'VM.Audit' ]);
	    my $job = $jobs->{$vmid};
	    $job->{vmid} = $vmid;
	    push @$res, $job;
	}

	return $res;
    }});

__PACKAGE__->register_method ({
    name => 'destroy_job',
    path => 'jobs/vmid',
    method => 'DELETE',
    description => "Destroy replication job.",
    permissions => {
	check => ['perm', '/vms/{vmid}', ['VM.Config.Disk']],
    },
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    vmid => {
		description => "The VMID of the guest.",
		type => 'string', format => 'pve-vmid',
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	PVE::ReplicationTools::destroy_replica($param->{vmid});

	return undef;
    }});

1;
