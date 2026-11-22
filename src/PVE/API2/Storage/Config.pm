package PVE::API2::Storage::Config;

use strict;
use warnings;

use PVE::SafeSyslog;
use PVE::Tools qw(extract_param extract_sensitive_params);
use PVE::Cluster qw(cfs_read_file cfs_write_file);
use PVE::Storage;
use PVE::Storage::Common;
use PVE::Storage::Plugin;
use PVE::Storage::LVMPlugin;
use PVE::Storage::CIFSPlugin;
use HTTP::Status qw(:constants);
use Storable qw(dclone);
use PVE::JSONSchema qw(get_standard_option);
use PVE::RPCEnvironment;

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

my @ctypes = qw(images vztmpl iso backup);

my $storage_type_enum = PVE::Storage::Plugin->lookup_types();

# Older nodes reject a content list with a type they do not know and fall back to the defaults of
# the storage type, which may allow more than intended. A type that the storage has already is not
# checked again, so that the storage can still be changed while a node is offline.
# FIXME: MAJOR VERSION: This check can be dropped at the next major version bump.
my sub assert_cluster_knows_content_types {
    my ($content, $current_content) = @_;

    return if !defined($content);

    my @new_vtypes =
        grep { defined(PVE::Storage::Common::generic_vtype($_)) && !($current_content // {})->{$_} }
        keys $content->%*;

    PVE::Cluster::assert_min_cluster_version(9, 2, 29) if scalar(@new_vtypes);
}

my $api_storage_config = sub {
    my ($cfg, $storeid) = @_;

    my $scfg = dclone(PVE::Storage::storage_config($cfg, $storeid));
    $scfg->{storage} = $storeid;
    $scfg->{digest} = $cfg->{digest};
    $scfg->{content} =
        PVE::Storage::Plugin->encode_value($scfg->{type}, 'content', $scfg->{content});

    if ($scfg->{nodes}) {
        $scfg->{nodes} =
            PVE::Storage::Plugin->encode_value($scfg->{type}, 'nodes', $scfg->{nodes});
    }

    if (my $content_dirs = $scfg->{'content-dirs'}) {
        $scfg->{'content-dirs'} =
            PVE::Storage::Plugin->encode_value($scfg->{type}, 'content-dirs', $content_dirs);
    }

    return $scfg;
};

# For storages that $match->($scfg), update node restrictions to not include $node anymore and
# in case no node remains, remove the storage altogether.
sub cleanup_storages_for_node {
    my ($self, $match, $node) = @_;

    my $config = PVE::Storage::config();
    my $cluster_nodes = PVE::Cluster::get_nodelist();

    for my $storeid (keys $config->{ids}->%*) {
        my $scfg = PVE::Storage::storage_config($config, $storeid);
        next if !$match->($scfg);

        my $nodes = $scfg->{nodes} || { map { $_ => 1 } $cluster_nodes->@* };
        next if !$nodes->{$node}; # not configured on $node, so nothing to do
        delete $nodes->{$node};

        if (scalar(keys $nodes->%*) > 0) {
            $self->update({
                nodes => join(',', sort keys $nodes->%*),
                storage => $storeid,
            });
        } else {
            $self->delete({ storage => $storeid });
        }
    }
}

# Decides if a storage needs to be created or updated. An update is needed, if
# the storage has a node list configured, then the current node will be added.
# The verify_params parameter is an array of parameter names that need to match
# if there already is a storage config of the same name present.  This is
# mainly intended for local storage types as certain parameters need to be the
# same.  For exmaple 'pool' for ZFS, 'vg_name' for LVM, ...
# Set the dryrun parameter, to only verify the parameters without updating or
# creating the storage.
sub create_or_update {
    my ($self, $sid, $node, $storage_params, $verify_params, $dryrun) = @_;

    my $cfg = PVE::Storage::config();
    my $scfg = PVE::Storage::storage_config($cfg, $sid, 1);

    if ($scfg) {
        die "storage config for '${sid}' exists but no parameters to verify were provided\n"
            if !$verify_params;

        $node = PVE::INotify::nodename() if !$node || ($node eq 'localhost');
        die "Storage ID '${sid}' already exists on node ${node}\n"
            if !defined($scfg->{nodes}) || $scfg->{nodes}->{$node};

        # check for type mismatch first to get a clear error
        for my $key ('type', $verify_params->@*) {
            if (!defined($scfg->{$key})) {
                die "Option '${key}' is not configured for storage '$sid', "
                    . "expected it to be '$storage_params->{$key}'";
            }
            if ($storage_params->{$key} ne $scfg->{$key}) {
                die "Option '${key}' ($storage_params->{$key}) does not match "
                    . "existing storage configuration '$scfg->{$key}'\n";
            }
        }
    }

    if (!$dryrun) {
        if ($scfg) {
            if ($scfg->{nodes}) {
                $scfg->{nodes}->{$node} = 1;
                $self->update({
                    nodes => join(',', sort keys $scfg->{nodes}->%*),
                    storage => $sid,
                });
                print "Added '${node}' to nodes for storage '${sid}'\n";
            }
        } else {
            $self->create($storage_params);
        }
    }
}

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => "Storage index.",
    permissions => {
        description =>
            "Only list entries where you have 'Datastore.Audit' or 'Datastore.AllocateSpace' permissions on '/storage/<storage>'",
        user => 'all',
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            type => {
                description => "Only list storage of specific type",
                type => 'string',
                enum => $storage_type_enum,
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
        links => [{ rel => 'child', href => "{storage}" }],
    },
    code => sub {
        my ($param) = @_;

        my $rpcenv = PVE::RPCEnvironment::get();
        my $authuser = $rpcenv->get_user();

        my $cfg = PVE::Storage::config();

        my @sids = PVE::Storage::storage_ids($cfg);

        my $res = [];
        foreach my $storeid (@sids) {
            my $privs = ['Datastore.Audit', 'Datastore.AllocateSpace'];
            next if !$rpcenv->check_any($authuser, "/storage/$storeid", $privs, 1);

            my $scfg = &$api_storage_config($cfg, $storeid);
            next if $param->{type} && $param->{type} ne $scfg->{type};
            push @$res, $scfg;
        }

        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'read',
    path => '{storage}',
    method => 'GET',
    description => "Read storage configuration.",
    permissions => {
        check => ['perm', '/storage/{storage}', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            storage => get_standard_option('pve-storage-id'),
        },
    },
    returns => { type => 'object' },
    code => sub {
        my ($param) = @_;

        my $cfg = PVE::Storage::config();

        return &$api_storage_config($cfg, $param->{storage});
    },
});

__PACKAGE__->register_method({
    name => 'create',
    protected => 1,
    path => '',
    method => 'POST',
    description => "Create a new storage.",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => PVE::Storage::Plugin::add_sensitive_properties_to_schema(
        PVE::Storage::Plugin->createSchema()
    ),
    returns => {
        type => 'object',
        properties => {
            storage => {
                description => "The ID of the created storage.",
                type => 'string',
            },
            type => {
                description => "The type of the created storage.",
                type => 'string',
                enum => $storage_type_enum,
            },
            config => {
                description => "Partial, possibly server generated, configuration properties.",
                type => 'object',
                optional => 1,
                additionalProperties => 1,
                properties => {
                    'encryption-key' => {
                        description => "The, possibly auto-generated, encryption-key.",
                        optional => 1,
                        type => 'string',
                    },
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $type = extract_param($param, 'type');
        my $storeid = extract_param($param, 'storage');

        # revent an empty nodelist.
        # fix me in section config create never need an empty entity.
        delete $param->{nodes} if !$param->{nodes};

        my $sensitive_params = PVE::Storage::Plugin::sensitive_properties($type);
        my $sensitive = extract_sensitive_params($param, $sensitive_params, []);

        my $plugin = PVE::Storage::Plugin->lookup($type);
        my $opts = $plugin->check_config($storeid, $param, 1, 1);

        assert_cluster_knows_content_types($opts->{content});

        # An LVM storage on a SAN base activates the base in its add hook. Connecting it can wait
        # for the network, which must not happen while the configuration is locked, so activate it
        # before. With all paths answering, the hook then finds it active.
        if ($type eq 'lvm' && (my $base = $opts->{base})) {
            my ($baseid) = PVE::Storage::parse_volume_id($base, 1);
            my $cfg = PVE::Storage::config();
            my $basecfg =
                defined($baseid) ? PVE::Storage::storage_config($cfg, $baseid, 1) : undef;
            if ($basecfg && ($basecfg->{type} eq 'iscsi' || $basecfg->{type} eq 'nvmeof')) {
                PVE::Storage::activate_storage($cfg, $baseid);
                PVE::Storage::activate_volumes($cfg, [$base]);
            }
        }

        my ($returned_config, $written, $added);
        PVE::Storage::lock_storage_config(
            sub {
                my $cfg = PVE::Storage::config();

                if (my $scfg = PVE::Storage::storage_config($cfg, $storeid, 1)) {
                    die "storage ID '$storeid' already defined\n";
                }

                # A node without the type drops its sections when it writes the configuration.
                # FIXME: MAJOR VERSION: This check can be dropped at the next major version bump.
                PVE::Cluster::assert_min_cluster_version(9, 2, 38)
                    if $type eq 'nvmeof' && !grep { $_->{type} eq 'nvmeof' } values $cfg->{ids}->%*;

                $cfg->{ids}->{$storeid} = $opts;

                $returned_config = $plugin->on_add_hook($storeid, $opts, %$sensitive);

                if (defined($opts->{mkdir})) { # TODO: remove complete option in Proxmox VE 9
                    warn
                        "NOTE: The 'mkdir' option set for '${storeid}' is deprecated and will be removed"
                        . " in Proxmox VE 9. Use 'create-base-path' or 'create-subdirs' instead.\n";
                }

                PVE::Storage::write_config($cfg);
                $written = $cfg;
                $added = PVE::Storage::config()->{ids}->{$storeid};
            },
            "create storage failed",
        );

        # The activation only reports a problem early. It can wait for the network, so it runs
        # without the lock. Another activation, like the one of pvestatd, can run at the same time,
        # and a mount or connect that lost that race fails, so a failure is retried once. If it
        # still fails, the storage is removed again, unless it changed or got used meanwhile.
        if (PVE::Storage::storage_check_enabled($written, $storeid, undef, 1)) {
            eval { PVE::Storage::activate_storage($written, $storeid) };
            if (my $err = $@) {
                sleep(1);
                eval { PVE::Storage::activate_storage($written, $storeid) };
                if ($@) {
                    my $removed;
                    eval {
                        local $Storable::canonical = 1;
                        PVE::Storage::lock_storage_config(
                            sub {
                                my $cfg = PVE::Storage::config();
                                my $scfg = $cfg->{ids}->{$storeid} // return;
                                return if Storable::freeze($scfg) ne Storable::freeze($added);
                                return if PVE::Storage::storage_is_used($cfg, $storeid);
                                delete $cfg->{ids}->{$storeid};
                                PVE::Storage::write_config($cfg);
                                $removed = 1;
                                eval { $plugin->on_delete_hook($storeid, $opts) };
                                warn "$@\n" if $@;
                            },
                            "removing it failed",
                        );
                    };
                    die "create storage failed: $err" if $removed;
                    chomp(my $kept = $@);
                    chomp($err);
                    die "storage '$storeid' was added, but its activation failed - $err"
                        . ($kept ? " ($kept)" : '') . "\n";
                }
            }
        }

        my $res = {
            storage => $storeid,
            type => $type,
        };
        $res->{config} = $returned_config if $returned_config;
        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'update',
    protected => 1,
    path => '{storage}',
    method => 'PUT',
    description => "Update storage configuration.",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => PVE::Storage::Plugin::add_sensitive_properties_to_schema(
        PVE::Storage::Plugin->updateSchema(), 1,
    ),
    resolve_type => sub {
        my ($param) = @_;

        # the schema only has a type once a plugin isolates its properties
        return undef if !PVE::Storage::Plugin->has_isolated_properties();

        my $cfg = PVE::Storage::config();
        my $scfg = PVE::Storage::storage_config($cfg, $param->{storage});
        return $scfg->{type};
    },
    returns => {
        type => 'object',
        properties => {
            storage => {
                description => "The ID of the created storage.",
                type => 'string',
            },
            type => {
                description => "The type of the created storage.",
                type => 'string',
                enum => $storage_type_enum,
            },
            config => {
                description => "Partial, possibly server generated, configuration properties.",
                type => 'object',
                optional => 1,
                additionalProperties => 1,
                properties => {
                    'encryption-key' => {
                        description => "The, possibly auto-generated, encryption-key.",
                        optional => 1,
                        type => 'string',
                    },
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $storeid = extract_param($param, 'storage');
        my $digest = extract_param($param, 'digest');
        my $delete = extract_param($param, 'delete');
        my $type = extract_param($param, 'type');

        if ($delete) {
            $delete = [PVE::Tools::split_list($delete)];
        }

        my $returned_config;
        PVE::Storage::lock_storage_config(
            sub {
                my $cfg = PVE::Storage::config();

                PVE::SectionConfig::assert_if_modified($cfg, $digest);

                my $scfg = PVE::Storage::storage_config($cfg, $storeid);
                if (!defined($type)) {
                    $type = $scfg->{type};
                } elsif ($type ne $scfg->{type}) {
                    die "cannot change storage type (expected '$scfg->{type}', got '$type')\n";
                }

                my $sensitive_params = PVE::Storage::Plugin::sensitive_properties($type);
                my $sensitive = extract_sensitive_params($param, $sensitive_params, $delete);

                my $plugin = PVE::Storage::Plugin->lookup($type);
                my $opts = $plugin->check_config($storeid, $param, 0, 1);

                assert_cluster_knows_content_types($opts->{content}, $scfg->{content});

                # Do checks for deletion up-front, but defer actual deletion until after the
                # on_update_hook(_full) call. This makes it possible to pass the unmodified current
                # storage configuration to the method.
                if ($delete) {
                    my $options = $plugin->private()->{options}->{$type};
                    foreach my $k (@$delete) {
                        my $d = $options->{$k} || die "no such option '$k'\n";
                        die "unable to delete required option '$k'\n" if !$d->{optional};
                        die "unable to delete fixed option '$k'\n" if $d->{fixed};
                        die "cannot set and delete property '$k' at the same time!\n"
                            if defined($opts->{$k});
                    }
                }

                if ($plugin->can('api') && $plugin->api() < 13) {
                    $returned_config = $plugin->on_update_hook($storeid, $opts, %$sensitive);
                } else {
                    $returned_config =
                        $plugin->on_update_hook_full($storeid, $scfg, $opts, $delete, $sensitive);
                }

                if ($delete) {
                    for my $k ($delete->@*) {
                        delete $scfg->{$k};
                    }
                }

                for my $k (keys %$opts) {
                    $scfg->{$k} = $opts->{$k};
                }

                if (defined($scfg->{mkdir})) { # TODO: remove complete option in Proxmox VE 9
                    warn
                        "NOTE: The 'mkdir' option set for '${storeid}' is deprecated and will be removed"
                        . " in Proxmox VE 9. Use 'create-base-path' or 'create-subdirs' instead.\n";
                }

                PVE::Storage::write_config($cfg);

            },
            "update storage failed",
        );

        my $res = {
            storage => $storeid,
            type => $type,
        };
        $res->{config} = $returned_config if $returned_config;
        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'delete',
    protected => 1,
    path => '{storage}', # /storage/config/{storage}
    method => 'DELETE',
    description => "Delete storage configuration.",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            storage => get_standard_option(
                'pve-storage-id',
                {
                    completion => \&PVE::Storage::complete_storage,
                },
            ),
            digest => get_standard_option('pve-config-digest'),
        },
    },
    returns => { type => 'null' },
    code => sub {
        my ($param) = @_;

        my $storeid = extract_param($param, 'storage');
        my $digest = extract_param($param, 'digest');

        PVE::Storage::lock_storage_config(
            sub {
                my $cfg = PVE::Storage::config();

                PVE::SectionConfig::assert_if_modified($cfg, $digest);

                my $scfg = PVE::Storage::storage_config($cfg, $storeid);

                die "can't remove storage - storage is used as base of another storage\n"
                    if PVE::Storage::storage_is_used($cfg, $storeid);

                my $plugin = PVE::Storage::Plugin->lookup($scfg->{type});

                $plugin->on_delete_hook($storeid, $scfg);

                delete $cfg->{ids}->{$storeid};

                PVE::Storage::write_config($cfg);

            },
            "delete storage failed",
        );

        PVE::AccessControl::remove_storage_access($storeid);

        return undef;
    },
});

1;
