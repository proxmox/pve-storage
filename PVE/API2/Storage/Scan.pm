package PVE::API2::Storage::Scan;

use strict;
use warnings;

# NOTE: This API endpoints are mounted by pve-manager's API2::Node module and pvesm CLI

use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::SafeSyslog;
use PVE::Storage::LVMPlugin;
use PVE::Storage;
use PVE::SysFSTools;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
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
	    properties => {
		method => { type => 'string'},
	    },
	},
	links => [ { rel => 'child', href => "{method}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $res = [
	    { method => 'cifs' },
	    { method => 'glusterfs' },
	    { method => 'iscsi' },
	    { method => 'lvm' },
	    { method => 'nfs' },
	    { method => 'pbs' },
	    { method => 'usb' },
	    { method => 'zfs' },
	];

	return $res;
    }});

__PACKAGE__->register_method({
    name => 'nfsscan',
    path => 'nfs',
    method => 'GET',
    description => "Scan remote NFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		path => {
		    description => "The exported path.",
		    type => 'string',
		},
		options => {
		    description => "NFS export options.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};
	my $res = PVE::Storage::scan_nfs($server);

	my $data = [];
	foreach my $k (sort keys %$res) {
	    push @$data, { path => $k, options => $res->{$k} };
	}
	return $data;
    }});

__PACKAGE__->register_method({
    name => 'cifsscan',
    path => 'cifs',
    method => 'GET',
    description => "Scan remote CIFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	    username => {
		description => "User name.",
		type => 'string',
		optional => 1,
	    },
	    password => {
		description => "User password.",
		type => 'string',
		optional => 1,
	    },
	    domain => {
		description => "SMB domain (Workgroup).",
		type => 'string',
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		share => {
		    description => "The cifs share name.",
		    type => 'string',
		},
		description => {
		    description => "Descriptive text from server.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};

	my $username = $param->{username};
	my $password = $param->{password};
	my $domain = $param->{domain};

	my $res = PVE::Storage::scan_cifs($server, $username, $password, $domain);

	my $data = [];
	foreach my $k (sort keys %$res) {
	    next if $k =~ m/NT_STATUS_/;
	    push @$data, { share => $k, description => $res->{$k} };
	}

	return $data;
    }});

__PACKAGE__->register_method({
    name => 'pbsscan',
    path => 'pbs',
    method => 'GET',
    description => "Scan remote Proxmox Backup Server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	    username => {
		description => "User-name or API token-ID.",
		type => 'string',
	    },
	    password => {
		description => "User password or API token secret.",
		type => 'string',
	    },
	    fingerprint => get_standard_option('fingerprint-sha256', {
		optional => 1,
	    }),
	    port => {
		description => "Optional port.",
		type => 'integer',
		minimum => 1,
		maximum => 65535,
		default => 8007,
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		store => {
		    description => "The datastore name.",
		    type => 'string',
		},
		comment => {
		    description => "Comment from server.",
		    type => 'string',
		    optional => 1,
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $password = delete $param->{password};

	return PVE::Storage::PBSPlugin::scan_datastores($param, $password);
    }
});

# Note: GlusterFS currently does not have an equivalent of showmount.
# As workaround, we simply use nfs showmount.
# see http://www.gluster.org/category/volumes/
__PACKAGE__->register_method({
    name => 'glusterfsscan',
    path => 'glusterfs',
    method => 'GET',
    description => "Scan remote GlusterFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		volname => {
		    description => "The volume name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};
	my $res = PVE::Storage::scan_nfs($server);

	my $data = [];
	foreach my $path (sort keys %$res) {
	    if ($path =~ m!^/([^\s/]+)$!) {
		push @$data, { volname => $1 };
	    }
	}
	return $data;
    }});

__PACKAGE__->register_method({
    name => 'iscsiscan',
    path => 'iscsi',
    method => 'GET',
    description => "Scan remote iSCSI server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    portal => {
		description => "The iSCSI portal (IP or DNS name with optional port).",
		type => 'string', format => 'pve-storage-portal-dns',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		target => {
		    description => "The iSCSI target name.",
		    type => 'string',
		},
		portal => {
		    description => "The iSCSI portal name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $res = PVE::Storage::scan_iscsi($param->{portal});

	my $data = [];
	foreach my $k (sort keys %$res) {
	    push @$data, { target => $k, portal => join(',', @{$res->{$k}}) };
	}

	return $data;
    }});

__PACKAGE__->register_method({
    name => 'lvmscan',
    path => 'lvm',
    method => 'GET',
    description => "List local LVM volume groups.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
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
		vg => {
		    description => "The LVM logical volume group name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $res = PVE::Storage::LVMPlugin::lvm_vgs();
	return PVE::RESTHandler::hash_to_array($res, 'vg');
    }});

__PACKAGE__->register_method({
    name => 'lvmthinscan',
    path => 'lvmthin',
    method => 'GET',
    description => "List local LVM Thin Pools.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    vg => {
		type => 'string',
		pattern => '[a-zA-Z0-9\.\+\_][a-zA-Z0-9\.\+\_\-]+', # see lvm(8) manpage
		maxLength => 100,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		lv => {
		    description => "The LVM Thin Pool name (LVM logical volume).",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Storage::LvmThinPlugin::list_thinpools($param->{vg});
    }});

__PACKAGE__->register_method({
    name => 'zfsscan',
    path => 'zfs',
    method => 'GET',
    description => "Scan zfs pool list on local node.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
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
		pool => {
		    description => "ZFS pool name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Storage::scan_zfs();
    }});

__PACKAGE__->register_method({
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
		class => { type => 'integer'},
		devnum => { type => 'integer'},
		level => { type => 'integer'},
		manufacturer => { type => 'string', optional => 1 },
		port => { type => 'integer'},
		prodid => { type => 'string'},
		product => { type => 'string', optional => 1 },
		serial => { type => 'string', optional => 1 },
		speed => { type => 'string'},
		usbpath => { type => 'string', optional => 1},
		vendid => { type => 'string'},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::SysFSTools::scan_usb();
    }});

1;
