package PVE::API2::Disks::LVM;

use strict;
use warnings;

use PVE::Storage::LVMPlugin;
use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::API2::Storage::Config;
use PVE::Tools qw(lock_file);

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
    description => "List LVM Volume Groups",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'object',
	properties => {
	    leaf => {
		type => 'boolean',
	    },
	    children => {
		type => 'array',
		items => {
		    type => "object",
		    properties => {
			leaf => {
			    type => 'boolean',
			},
			name => {
			    type => 'string',
			    description => 'The name of the volume group',
			},
			size => {
			    type => 'integer',
			    description => 'The size of the volume group in bytes',
			},
			free => {
			    type => 'integer',
			    description => 'The free bytes in the volume group',
			},
			children => {
			    optional => 1,
			    type => 'array',
			    description => 'The underlying physical volumes',
			    items =>  {
				type => 'object',
				properties => {
				    leaf => {
					type => 'boolean',
				    },
				    name => {
					type => 'string',
					description => 'The name of the physical volume',
				    },
				    size => {
					type => 'integer',
					description => 'The size of the physical volume in bytes',
				    },
				    free => {
					type => 'integer',
					description => 'The free bytes in the physical volume',
				    },
				},
			    },
			},
		    },
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $result = [];

	my $vgs = PVE::Storage::LVMPlugin::lvm_vgs(1);

	foreach my $vg_name (sort keys %$vgs) {
	    my $vg = $vgs->{$vg_name};
	    $vg->{name} = $vg_name;
	    $vg->{leaf} = 0;
	    foreach my $pv (@{$vg->{pvs}}) {
		$pv->{leaf} = 1;
	    }
	    $vg->{children} = delete $vg->{pvs};
	    push @$result, $vg;
	}

	return {
	    leaf => 0,
	    children => $result,
	};
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
    description => "Create an LVM Volume Group",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    name => get_standard_option('pve-storage-id'),
	    device => {
		type => 'string',
		description => 'The block device you want to create the volume group on',
	    },
	    add_storage => {
		description => "Configure storage using the Volume Group",
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

	my $worker = sub {
	    PVE::Diskmanage::locked_disk_action(sub {
		PVE::Storage::LVMPlugin::lvm_create_volume_group($dev, $name);

		if ($param->{add_storage}) {
		    my $storage_params = {
			type => 'lvm',
			vgname => $name,
			storage => $name,
			content => 'rootdir,images',
			shared => 0,
			nodes => $node,
		    };

		    PVE::API2::Storage::Config->create($storage_params);
		}
	    });
	};

	return $rpcenv->fork_worker('lvmcreate', $name, $user, $worker);
    }});

1;
