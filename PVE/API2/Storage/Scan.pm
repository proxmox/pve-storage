package PVE::API2::Storage::Scan;

use strict;
use warnings;

use PVE::SafeSyslog;
use PVE::Storage;
use PVE::Storage::LVMPlugin;
use HTTP::Status qw(:constants);
use PVE::JSONSchema qw(get_standard_option);

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'index', 
    path => '', 
    method => 'GET',
    description => "Index of available scan methods",
    permissions => { 
	user => 'all',
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
	    type => "object",
	    properties => { method => { type => 'string'} },
	},
	links => [ { rel => 'child', href => "{method}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $res = [ 
	    { method => 'lvm' },
	    { method => 'iscsi' },
	    { method => 'nfs' },
	    { method => 'glusterfs' },
	    { method => 'usb' },
	    { method => 'zfs' },
	    { method => 'cifs' },
	    ];

	return $res;
    }});

__PACKAGE__->register_method ({
    name => 'usbscan', 
    path => 'usb', 
    method => 'GET',
    description => "List local USB devices.",
    protected => 1,
    proxyto => "node",
    permissions => { 
	check => ['perm', '/', ['Sys.Modify']],
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
	    type => "object",
	    properties => { 
		busnum => { type => 'integer'},
		devnum => { type => 'integer'},
		port => { type => 'integer'},
		usbpath => { type => 'string', optional => 1},
		level => { type => 'integer'},
		class => { type => 'integer'},
		vendid => { type => 'string'},
		prodid => { type => 'string'},
		speed => { type => 'string'},

		product => { type => 'string', optional => 1 },
		serial => { type => 'string', optional => 1 },
		manufacturer => { type => 'string', optional => 1 },
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Storage::scan_usb();
    }});

1;
