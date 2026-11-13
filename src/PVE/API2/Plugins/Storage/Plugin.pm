package PVE::API2::Plugins::Storage::Plugin;

use v5.36;

use Storable qw(dclone);

use PVE::Exception qw(raise);
use PVE::RESTHandler;
use PVE::Tools qw(extract_param);

use PVE::Storage; # pre-load plugins
use PVE::Storage::Plugin;

use base qw(PVE::RESTHandler);

my $ALL_PLUGIN_TYPES = PVE::Storage::Plugin->lookup_types();

my $PLUGIN_ORIGINS = ['builtin', 'custom'];

my sub plugin_origin($plugin) {
    return $plugin =~ m/^PVE::Storage::Custom::/ ? 'custom' : 'builtin';
}

my $PLUGIN_METADATA_SCHEMA = {
    type => 'object',
    additionalProperties => 0,
    properties => {
        content => {
            description => "The content types of the plugin.",
            type => 'object',
            optional => 0,
            properties => {
                supported => {
                    description => "The content types the plugin supports.",
                    type => 'array',
                    optional => 0,
                    items => {
                        type => 'string',
                    },
                },
                default => {
                    description => "The content types enabled by default.",
                    type => 'array',
                    optional => 0,
                    items => {
                        type => 'string',
                    },
                },
            },
        },
        'display-name' => {
            description => "The name of the storage type, if the plugin provides one.",
            type => 'string',
            optional => 1,
        },
        origin => {
            description => "Whether the plugin is shipped with Proxmox VE ('builtin') or"
                . " installed separately ('custom').",
            type => 'string',
            enum => $PLUGIN_ORIGINS,
            optional => 0,
        },
        schema => {
            type => 'object',
            optional => 0,
        },
        type => {
            description => "The storage type of the plugin.",
            type => 'string',
            optional => 0,
            enum => $ALL_PLUGIN_TYPES,
        },
    },
};

my $PLUGIN_SCHEMAS = {};

my sub get_schema_for_plugin($plugin) {
    my $type = $plugin->type();

    return $PLUGIN_SCHEMAS->{$type} if defined($PLUGIN_SCHEMAS->{$type});

    # Derive options from the initialized data instead of using `options()`,
    # since `init_isolated()` automatically adds properties to options that
    # were not specified explicitly
    my $pdata = $plugin->private();
    my $options = $pdata->{options}->{$type};
    my $plugindata = $plugin->plugindata();

    # also covers the fallback for plugins without 'sensitive-properties' in their plugindata()
    my $sensitive = { map { $_ => 1 } PVE::Storage::Plugin::sensitive_properties($type)->@* };
    my $advanced = $plugindata->{'advanced-properties'} // {};
    my $hidden = $plugindata->{'hidden-properties'} // {};

    my $schema = {};

    for my $option (keys $options->%*) {
        my $prop_schema = PVE::RESTHandler::api_dump_remove_refs(
            PVE::Storage::Plugin->get_property_schema($type, $option));

        # Deep clone to prevent any accidental Perl-esque side effects, because
        # we are memoizing each schema through $PLUGIN_SCHEMAS
        my $property = dclone($prop_schema);

        # A value from 'sensitive-properties' goes to the plugin's hooks instead of the storage
        # config, so a client cannot read it back, whatever the property schema itself declares.
        $property->{sensitive} = ($property->{sensitive} || $sensitive->{$option}) ? 1 : 0;

        $property->{'x-advanced'} = $advanced->{$option} ? 1 : 0;
        $property->{'x-hidden'} = $hidden->{$option} ? 1 : 0;

        $schema->{$option} = $property;

        for my $opt_key (keys $options->{$option}->%*) {
            $property->{$opt_key} = $options->{$option}->{$opt_key};
        }

        # NOTE: The global property list in SectionConfig marks every plugin
        # property as optional. This means that we cannot rely on the
        # `optional` key that the `get_property_schema` above returns for
        # validation in the front end; it would mark required values as
        # optional.
        #
        # Note that we also cannot rely on `createSchema()` / `updateSchema()`
        # here for the same reason.
        #
        # Therefore, we override this key below with what the finalized set of
        # options (from the private data) contains directly. This makes it so that the
        # front end validation matches the back end's.
        #
        # See the docstring for PVE::SectionConfig::properties() for more
        # information.
        my $is_optional = $options->{$option}->{optional};
        $property->{optional} = $is_optional ? 1 : 0;
    }

    $PLUGIN_SCHEMAS->{$type} = $schema;

    return $schema;
}

my sub build_plugin_metadata($type) {
    my $plugin = PVE::Storage::Plugin->lookup($type);
    my $plugindata = $plugin->plugindata();

    my $metadata = {
        content => {
            supported => [sort keys $plugindata->{content}->[0]->%*],
            default => [sort keys $plugindata->{content}->[1]->%*],
        },
        origin => plugin_origin($plugin),
        schema => get_schema_for_plugin($plugin),
        type => $type,
    };

    my $display_name = $plugindata->{meta}->{'display-name'};
    if (defined($display_name) && !ref($display_name) && length($display_name)) {
        $metadata->{'display-name'} = $display_name;
    }

    return $metadata;
}

# plugins/storage/plugin

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => 'List all available storage plugins and their metadata.',
    permissions => {
        user => 'all',
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            origin => {
                description => "Only list plugins of this origin.",
                type => 'string',
                enum => $PLUGIN_ORIGINS,
                optional => 1,
            },
        },
    },
    returns => {
        type => 'array',
        items => $PLUGIN_METADATA_SCHEMA,
        links => [{ rel => 'child', href => '{type}' }],
    },
    code => sub($param) {
        my $origin = extract_param($param, 'origin');

        my $result = [];

        for my $type ($ALL_PLUGIN_TYPES->@*) {
            my $plugin = PVE::Storage::Plugin->lookup($type);

            if (defined($origin)) {
                next if plugin_origin($plugin) ne $origin;
            }

            my $metadata = eval { build_plugin_metadata($type) };
            if (my $err = $@) {
                warn "skipping storage plugin '$type' - cannot build its metadata - $err";
                next;
            }

            push($result->@*, $metadata);
        }

        return $result;
    },
});

# plugins/storage/plugin/{type}

__PACKAGE__->register_method({
    name => 'info',
    path => '{type}',
    method => 'GET',
    description => 'Fetch metadata of a storage plugin.',
    permissions => {
        user => 'all',
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            type => {
                description => 'The type of the storage plugin.',
                type => 'string',
                enum => $ALL_PLUGIN_TYPES,
            },
        },
    },
    returns => $PLUGIN_METADATA_SCHEMA,
    code => sub($param) {
        my $param_type = extract_param($param, 'type');

        my $metadata = eval { build_plugin_metadata($param_type) };
        if (my $err = $@) {
            raise("Cannot build metadata of storage plugin '$param_type' - $err");
        }

        return $metadata;
    },
});

1;
