package PVE::API2::Storage::PruneBackups;

use strict;
use warnings;

use PVE::Cluster;
use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::RPCEnvironment;
use PVE::Storage;
use PVE::Tools qw(extract_param);

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'dryrun',
    path => '',
    method => 'GET',
    description => "Get prune information for backups. NOTE: this is only a preview and might not be " .
		   "what a subsequent prune call does if backups are removed/added in the meantime.",
    permissions => {
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
		completion => \&PVE::Storage::complete_storage_enabled,
            }),
	    'prune-backups' => get_standard_option('prune-backups', {
		description => "Use these retention options instead of those from the storage configuration.",
		optional => 1,
	    }),
	    type => {
		description => "Either 'qemu' or 'lxc'. Only consider backups for guests of this type.",
		type => 'string',
		optional => 1,
		enum => ['qemu', 'lxc'],
	    },
	    vmid => get_standard_option('pve-vmid', {
		description => "Only consider backups for this guest.",
		optional => 1,
		completion => \&PVE::Cluster::complete_vmid,
	    }),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => 'object',
	    properties => {
		volid => {
		    description => "Backup volume ID.",
		    type => 'string',
		},
		'ctime' => {
		    description => "Creation time of the backup (seconds since the UNIX epoch).",
		    type => 'integer',
		},
		'mark' => {
		    description => "Whether the backup would be kept or removed. For backups that don't " .
				   "use the standard naming scheme, it's 'protected'.",
		    type => 'string',
		},
		type => {
		    description => "One of 'qemu', 'lxc', 'openvz' or 'unknown'.",
		    type => 'string',
		},
		'vmid' => {
		    description => "The VM the backup belongs to.",
		    type => 'integer',
		    optional => 1,
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $cfg = PVE::Storage::config();

	my $vmid = extract_param($param, 'vmid');
	my $type = extract_param($param, 'type');
	my $storeid = extract_param($param, 'storage');

	my $prune_backups = extract_param($param, 'prune-backups');
	$prune_backups = PVE::JSONSchema::parse_property_string('prune-backups', $prune_backups)
	    if defined($prune_backups);

	return PVE::Storage::prune_backups($cfg, $storeid, $prune_backups, $vmid, $type, 1);
    }});

__PACKAGE__->register_method ({
    name => 'delete',
    path => '',
    method => 'DELETE',
    description => "Prune backups. Only those using the standard naming scheme are considered.",
    permissions => {
	description => "You need the 'Datastore.Allocate' privilege on the storage " .
		       "(or if a VM ID is specified, 'Datastore.AllocateSpace' and 'VM.Backup' for the VM).",
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
                completion => \&PVE::Storage::complete_storage,
            }),
	    'prune-backups' => get_standard_option('prune-backups', {
		description => "Use these retention options instead of those from the storage configuration.",
	    }),
	    type => {
		description => "Either 'qemu' or 'lxc'. Only consider backups for guests of this type.",
		type => 'string',
		optional => 1,
		enum => ['qemu', 'lxc'],
	    },
	    vmid => get_standard_option('pve-vmid', {
		description => "Only prune backups for this VM.",
		completion => \&PVE::Cluster::complete_vmid,
		optional => 1,
	    }),
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my $cfg = PVE::Storage::config();

	my $vmid = extract_param($param, 'vmid');
	my $type = extract_param($param, 'type');
	my $storeid = extract_param($param, 'storage');

	my $prune_backups = extract_param($param, 'prune-backups');
	$prune_backups = PVE::JSONSchema::parse_property_string('prune-backups', $prune_backups)
	    if defined($prune_backups);

	if (defined($vmid)) {
	    $rpcenv->check($authuser, "/storage/$storeid", ['Datastore.AllocateSpace']);
	    $rpcenv->check($authuser, "/vms/$vmid", ['VM.Backup']);
	} else {
	    $rpcenv->check($authuser, "/storage/$storeid", ['Datastore.Allocate']);
	}

	my $id = (defined($vmid) ? "$vmid@" : '') . $storeid;
	my $worker = sub {
	    PVE::Storage::prune_backups($cfg, $storeid, $prune_backups, $vmid, $type, 0);
	};

	return $rpcenv->fork_worker('prunebackups', $id, $authuser, $worker);
    }});

1;
