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
        type => {
            description => "The storage type of the plugin.",
            type => 'string',
            optional => 0,
            enum => $ALL_PLUGIN_TYPES,
        },
    },
};

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

            my $item = {
                origin => plugin_origin($plugin),
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

        my $result = {
            origin => plugin_origin($plugin),
            type => $param_type,
        };

        return $result;
    },
});

1;
