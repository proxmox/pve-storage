package PVE::API2::Disks::LVM;

use strict;
use warnings;

use PVE::Storage::LVMPlugin;
use PVE::Storage::SAN;
use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::API2::Storage::Config;

use PVE::RPCEnvironment;
use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
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
                            items => {
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
                                        description =>
                                            'The size of the physical volume in bytes',
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
            foreach my $pv (@{ $vg->{pvs} }) {
                $pv->{leaf} = 1;
            }
            $vg->{children} = delete $vg->{pvs};
            push @$result, $vg;
        }

        return {
            leaf => 0,
            children => $result,
        };
    },
});

__PACKAGE__->register_method({
    name => 'create',
    path => '',
    method => 'POST',
    proxyto => 'node',
    protected => 1,
    permissions => {
        description =>
            "Requires additionally 'Datastore.Allocate' on /storage when setting 'add_storage'",
        check => ['perm', '/', ['Sys.Modify']],
    },
    description => "Create an LVM Volume Group",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            device => {
                type => 'string',
                description => 'The block device you want to create the volume group on.'
                    . ' Multipath mapped devices are supported.',
            },
            add_storage => {
                description => "Configure storage using the Volume Group. On a SAN LUN, this"
                    . " adds a new shared storage for all nodes. On a local disk, the storage is"
                    . " for this node, or this node joins a matching storage of that ID.",
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
        my $san = PVE::Storage::SAN::is_san_device($dev);
        PVE::Diskmanage::assert_disk_unused($dev) if !$san;

        my $storage_params = {
            type => 'lvm',
            vgname => $name,
            storage => $name,
            content => 'rootdir,images',
            shared => $san ? 1 : 0,
            $san ? () : (nodes => $node),
        };
        my $verify_params = [qw(vgname)];

        if ($param->{add_storage}) {
            $rpcenv->check($user, "/storage", ['Datastore.Allocate']);

            # only local volume groups of the same name may share a storage that exists already
            die "storage ID '$name' already defined\n"
                if $san && PVE::Storage::storage_config(PVE::Storage::config(), $name, 1);

            # a dry run that checks the parameters, the storage is added after the volume group
            PVE::API2::Storage::Config->create_or_update(
                $name, $node, $storage_params, $verify_params, 1,
            );
        }

        my $create_group = sub {
            PVE::Diskmanage::assert_disk_unused($dev) if !$san;
            die "volume group with name '${name}' already exists on node '${node}'\n"
                if PVE::Storage::LVMPlugin::lvm_vgs()->{$name};

            if (PVE::Diskmanage::is_partition($dev)) {
                eval { PVE::Diskmanage::change_parttype($dev, '8E00'); };
                warn $@ if $@;
            }

            # the given name, so that the LUN lookup notices if it passed to another device during
            # its scan
            PVE::Storage::LVMPlugin::lvm_create_volume_group(
                $san ? $param->{device} : $dev,
                $name,
                $san,
            );

            PVE::Diskmanage::udevadm_trigger($dev);

            if ($param->{add_storage} && $san) {
                # must not join a storage that someone added with this ID in the meantime
                PVE::API2::Storage::Config->create({%$storage_params});
            } elsif ($param->{add_storage}) {
                PVE::API2::Storage::Config->create_or_update(
                    $name, $node, $storage_params, $verify_params,
                );
            }
        };
        my $worker = sub {
            $san ? $create_group->() : PVE::Diskmanage::locked_disk_action($create_group);
        };

        return $rpcenv->fork_worker('lvmcreate', $name, $user, $worker);
    },
});

__PACKAGE__->register_method({
    name => 'delete',
    path => '{name}',
    method => 'DELETE',
    proxyto => 'node',
    protected => 1,
    permissions => {
        description =>
            "Requires additionally 'Datastore.Allocate' on /storage when setting 'cleanup-config'",
        check => ['perm', '/', ['Sys.Modify']],
    },
    description => "Remove an LVM Volume Group.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            name => get_standard_option('pve-storage-id'),
            'cleanup-config' => {
                description =>
                    "Marks associated storage(s) as not available on this node anymore "
                    . "or removes them from the configuration (if configured for this node only).",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            'cleanup-disks' => {
                description => "Also wipe disks so they can be repurposed afterwards.",
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

        $rpcenv->check($user, "/storage", ['Datastore.Allocate']) if $param->{'cleanup-config'};

        my $name = $param->{name};
        my $node = $param->{node};

        my $worker = sub {
            PVE::Diskmanage::locked_disk_action(sub {
                my $vgs = PVE::Storage::LVMPlugin::lvm_vgs(1);
                die "no such volume group '$name'\n" if !$vgs->{$name};

                PVE::Storage::LVMPlugin::lvm_destroy_volume_group($name);

                my $config_err;
                if ($param->{'cleanup-config'}) {
                    my $match = sub {
                        my ($scfg) = @_;
                        return $scfg->{type} eq 'lvm' && $scfg->{vgname} eq $name;
                    };
                    eval {
                        PVE::API2::Storage::Config->cleanup_storages_for_node($match, $node);
                    };
                    warn $config_err = $@ if $@;
                }

                if ($param->{'cleanup-disks'}) {
                    my $wiped = [];
                    eval {
                        for my $pv ($vgs->{$name}->{pvs}->@*) {
                            my $dev = PVE::Diskmanage::verify_blockdev_path($pv->{name});
                            PVE::Diskmanage::wipe_blockdev($dev);
                            push $wiped->@*, $dev;
                        }
                    };
                    my $err = $@;
                    PVE::Diskmanage::udevadm_trigger($wiped->@*);
                    die "cleanup failed - $err" if $err;
                }

                die "config cleanup failed - $config_err" if $config_err;
            });
        };

        return $rpcenv->fork_worker('lvmremove', $name, $user, $worker);
    },
});

1;
