package PVE::API2::Storage::Replication;

use warnings;
use strict;

use PVE::JSONSchema qw(get_standard_option);
use PVE::ReplicationTools;

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'list',
    path => 'list',
    method => 'GET',
    description => "List of all replication jobs.",
    permissions => {
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    nodes => get_standard_option('pve-node-list' ,
					 {description => "Notes where the jobs is located.",
					  optional => 1}),
	},
    },
    returns => { type => 'object' },
    code => sub {
	my ($param) = @_;

	if ($param->{nodes}) {
	    foreach my $node (PVE::Tools::split_list($param->{nodes})) {
		die "Node: $node does not exists.\n" if
		    !PVE::Cluster::check_node_exists($node);
	    }
	}

	my $nodes = $param->{nodes} ?
	    $param->{nodes} : $param->{node};

	return PVE::ReplicationTools::get_all_jobs($nodes);
}});

1;
