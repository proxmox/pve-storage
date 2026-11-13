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

    my $schema = {};

    for my $option (keys $options->%*) {
        my $prop_schema = PVE::RESTHandler::api_dump_remove_refs(
            PVE::Storage::Plugin->get_property_schema($type, $option));

        # Deep clone to prevent any accidental Perl-esque side effects, because
        # we are memoizing each schema through $PLUGIN_SCHEMAS
        my $property = dclone($prop_schema);
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

            my $schema = eval { get_schema_for_plugin($plugin) };
            if (my $err = $@) {
                warn "skipping storage plugin '$type' - cannot build its schema: $err\n";
                next;
            }

            my $item = {
                origin => plugin_origin($plugin),
                schema => $schema,
                type => $type,
            };

            push($result->@*, $item);
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

        # Does not fail since `type` is validated first
        my $plugin = PVE::Storage::Plugin->lookup($param_type);

        my $schema = eval { get_schema_for_plugin($plugin) };
        if (my $err = $@) {
            raise("Cannot build metadata of storage plugin '$param_type' - $err");
        }

        my $result = {
            origin => plugin_origin($plugin),
            schema => $schema,
            type => $param_type,
        };

        return $result;
    },
});

1;
