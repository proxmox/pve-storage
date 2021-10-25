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
		vg => {
		    type => 'string',
		    description => 'The associated volume group.',
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
	PVE::Diskmanage::assert_disk_unused($dev);
	PVE::Storage::assert_sid_unused($name) if $param->{add_storage};

	my $worker = sub {
	    PVE::Diskmanage::locked_disk_action(sub {
		PVE::Diskmanage::assert_disk_unused($dev);

		if (PVE::Diskmanage::is_partition($dev)) {
		    eval { PVE::Diskmanage::change_parttype($dev, '8E00'); };
		    warn $@ if $@;
		}

		PVE::Storage::LVMPlugin::lvm_create_volume_group($dev, $name);
		my $pv = PVE::Storage::LVMPlugin::lvm_pv_info($dev);
		# keep some free space just in case
		my $datasize = $pv->{size} - 128*1024;
		# default to 1% for metadata
		my $metadatasize = $datasize/100;
		# but at least 1G, as recommended in lvmthin man
		$metadatasize = 1024*1024 if $metadatasize < 1024*1024;
		# but at most 16G, which is the current lvm max
		$metadatasize = 16*1024*1024 if $metadatasize > 16*1024*1024;
		# shrink data by needed amount for metadata
		$datasize -= 2*$metadatasize;

		run_command([
		    '/sbin/lvcreate',
		    '--type', 'thin-pool',
		    "-L${datasize}K",
		    '--poolmetadatasize', "${metadatasize}K",
		    '-n', $name,
		    $name
		]);

		PVE::Diskmanage::udevadm_trigger($dev);

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

__PACKAGE__->register_method ({
    name => 'delete',
    path => '{name}',
    method => 'DELETE',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Modify', 'Datastore.Allocate']],
    },
    description => "Remove an LVM thin pool.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    name => get_standard_option('pve-storage-id'),
	    'volume-group' => get_standard_option('pve-storage-id'),
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $user = $rpcenv->get_user();

	my $vg = $param->{'volume-group'};
	my $lv = $param->{name};

	my $worker = sub {
	    PVE::Diskmanage::locked_disk_action(sub {
		my $thinpools = PVE::Storage::LvmThinPlugin::list_thinpools();

		die "no such thin pool ${vg}/${lv}\n"
		    if !grep { $_->{lv} eq $lv && $_->{vg} eq $vg } $thinpools->@*;

		run_command(['lvremove', '-y', "${vg}/${lv}"]);
	    });
	};

	return $rpcenv->fork_worker('lvmthinremove', "${vg}-${lv}", $user, $worker);
    }});

1;
