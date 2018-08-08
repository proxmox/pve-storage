package PVE::API2::Disks::LVMThin;

use strict;
use warnings;

use PVE::Storage::LvmThinPlugin;
use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::API2::Storage::Config;
use PVE::Storage;
use PVE::Tools qw(run_command lock_file);

use PVE::RPCEnvironment;
use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Audit', 'Datastore.Audit'], any => 1],
    },
    description => "List LVM thinpools",
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
		lv => {
		    type => 'string',
		    description => 'The name of the thinpool.',
		},
		lv_size => {
		    type => 'integer',
		    description => 'The size of the thinpool in bytes.',
		},
		used => {
		    type => 'integer',
		    description => 'The used bytes of the thinpool.',
		},
		metadata_size => {
		    type => 'integer',
		    description => 'The size of the metadata lv in bytes.',
		},
		metadata_used => {
		    type => 'integer',
		    description => 'The used bytes of the metadata lv.',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;
	return PVE::Storage::LvmThinPlugin::list_thinpools(undef);
    }});

__PACKAGE__->register_method ({
    name => 'create',
    path => '',
    method => 'POST',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Modify', 'Datastore.Allocate']],
    },
    description => "Create an LVM thinpool",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    name => get_standard_option('pve-storage-id'),
	    device => {
		type => 'string',
		description => 'The block device you want to create the thinpool on.',
	    },
	    add_storage => {
		description => "Configure storage using the thinpool.",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $user = $rpcenv->get_user();

	my $name = $param->{name};
	my $dev = $param->{device};
	my $node = $param->{node};

	$dev = PVE::Diskmanage::verify_blockdev_path($dev);
	die "device $dev is already in use\n" if PVE::Diskmanage::disk_is_used($dev);

	my $cfg = PVE::Storage::config();

	if (my $scfg = PVE::Storage::storage_config($cfg, $name, 1)) {
	    die "storage ID '$name' already defined\n";
	}

	my $worker = sub {
	    PVE::Diskmanage::locked_disk_action(sub {
		PVE::Storage::LVMPlugin::lvm_create_volume_group($dev, $name);

		# create thinpool with size 100%, let lvm handle the metadata size
		run_command(['/sbin/lvcreate', '--type', 'thin-pool', '-l100%FREE', '-n', $name, $name]);

		if ($param->{add_storage}) {
		    my $storage_params = {
			type => 'lvmthin',
			vgname => $name,
			thinpool => $name,
			storage => $name,
			content => 'rootdir,images',
			nodes => $node,
		    };

		    PVE::API2::Storage::Config->create($storage_params);
		}
	    });
	};

	return $rpcenv->fork_worker('lvmthincreate', $name, $user, $worker);
    }});

1;
