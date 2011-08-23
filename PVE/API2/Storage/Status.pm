package PVE::API2::Storage::Status;

use strict;
use warnings;

use PVE::Cluster qw(cfs_read_file);
use PVE::Storage;
use PVE::API2::Storage::Content;
use PVE::RESTHandler;
use PVE::RPCEnvironment;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Exception qw(raise_param_exc);

use base qw(PVE::RESTHandler);


__PACKAGE__->register_method ({
    subclass => "PVE::API2::Storage::Content", 
    # set fragment delimiter (no subdirs) - we need that, because volume
    # IDs may contain a slash '/' 
    fragmentDelimiter => '', 
    path => '{storage}/content',
});

__PACKAGE__->register_method ({
    name => 'index', 
    path => '',
    method => 'GET',
    description => "Get status for all datastores.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option
		('pve-storage-id', {
		    description => "Only list status for  specified storage",
		    optional => 1,
		 }),
	    content => { 
		description => "Only list stores which support this content type.",
		type => 'string', format => 'pve-storage-content',
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => { storage => { type => 'string' } },
	},
	links => [ { rel => 'child', href => "{storage}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	my $info = PVE::Storage::storage_info($cfg, $param->{content});

	if ($param->{storage}) {
	    my $data = $info->{$param->{storage}};

	    raise_param_exc({ storage => "No such storage." })
		if !defined($data);

	    $data->{storage} = $param->{storage};

	    return [ $data ];
	}
	return PVE::RESTHandler::hash_to_array($info, 'storage');
    }});

__PACKAGE__->register_method ({
    name => 'diridx',
    path => '{storage}', 
    method => 'GET',
    description => "",
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		subdir => { type => 'string' },
	    },
	},
	links => [ { rel => 'child', href => "{subdir}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $res = [
	    { subdir => 'status' },
	    { subdir => 'content' },
	    { subdir => 'rrd' },
	    { subdir => 'rrddata' },
	    ];
	
	return $res;
    }});

__PACKAGE__->register_method ({
    name => 'read_status',
    path => '{storage}/status', 
    method => 'GET',
    description => "Read storage status.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => {
	type => "object",
	properties => {},
    },
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	my $info = PVE::Storage::storage_info($cfg, $param->{content});

	my $data = $info->{$param->{storage}};

	raise_param_exc({ storage => "No such storage." })
	    if !defined($data);
    
	return $data;
    }});

__PACKAGE__->register_method ({
    name => 'rrd',
    path => '{storage}/rrd', 
    method => 'GET',
    description => "Read storage RRD statistics (returns PNG).",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    timeframe => {
		description => "Specify the time frame you are interested in.",
		type => 'string',
		enum => [ 'hour', 'day', 'week', 'month', 'year' ],
	    },
	    ds => {
		description => "The list of datasources you want to display.",
 		type => 'string', format => 'pve-configid-list',
	    },
	    cf => {
		description => "The RRD consolidation function",
 		type => 'string',
		enum => [ 'AVERAGE', 'MAX' ],
		optional => 1,
	    },
	},
    },
    returns => {
	type => "object",
	properties => {
	    filename => { type => 'string' },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Cluster::create_rrd_graph(
	    "pve2-storage/$param->{node}/$param->{storage}", 
	    $param->{timeframe}, $param->{ds}, $param->{cf});
					      
    }});

__PACKAGE__->register_method ({
    name => 'rrddata',
    path => '{storage}/rrddata', 
    method => 'GET',
    description => "Read storage RRD statistics.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    timeframe => {
		description => "Specify the time frame you are interested in.",
		type => 'string',
		enum => [ 'hour', 'day', 'week', 'month', 'year' ],
	    },
	    cf => {
		description => "The RRD consolidation function",
 		type => 'string',
		enum => [ 'AVERAGE', 'MAX' ],
		optional => 1,
	    },
	},
    },
    returns => {
	type => "array",
	items => {
	    type => "object",
	    properties => {},
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Cluster::create_rrd_data(
	    "pve2-storage/$param->{node}/$param->{storage}", 
	    $param->{timeframe}, $param->{cf});	      
    }});
    
1;
