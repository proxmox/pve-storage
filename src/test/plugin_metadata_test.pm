package PVE::Storage::TestPluginMetadata;

use v5.36;

use lib qw(..);

use Test::More;

use PVE::Storage::Plugin;

package PVE::Storage::Custom::FixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'fixture';
    }

    sub plugindata {
        return {
            content => [{ images => 1, none => 1 }, { none => 1 }],
            meta => { 'display-name' => 'Fixture Storage' },
            'sensitive-properties' => { 'fixture-token' => 1 },
        };
    }

    sub properties {
        return {
            'fixture-token' => { type => 'string', sensitive => 0 },
            'fixture-secret' => { type => 'string', sensitive => 1 },
        };
    }

    sub options {
        return {
            'fixture-token' => { optional => 1 },
            'fixture-secret' => { optional => 1 },
        };
    }
}

package PVE::Storage::Custom::LegacyFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'legacyfixture';
    }

    sub plugindata {
        return { content => [{ none => 1 }, { none => 1 }] };
    }

    sub options {
        return { 'master-pubkey' => { optional => 1 } };
    }
}

# has to happen before loading PVE::Storage, which initializes all registered plugins
PVE::Storage::Custom::FixturePlugin->register();
PVE::Storage::Custom::LegacyFixturePlugin->register();

require PVE::Storage;
require PVE::API2::Plugins::Storage::Plugin;

my $api = 'PVE::API2::Plugins::Storage::Plugin';

# each test is comprised of the following array keys:
# [0] => storage type
# [1] => property
# [2] => expected keys in the schema of the property
my $property_tests = [
    # a property shared by several plugins follows the options of each plugin
    ['rbd', 'pool', { optional => 1, fixed => undef }],
    ['zfspool', 'pool', { optional => 0, fixed => 1 }],
    ['pbs', 'password', { sensitive => 1 }],
    ['pbs', 'datastore', { sensitive => 0 }],
    ['rbd', 'namespace', { 'x-advanced' => 1, 'x-hidden' => 0 }],
    ['pbs', 'namespace', { 'x-advanced' => 0, 'x-hidden' => 0 }],
    ['dir', 'content-dirs', { 'x-advanced' => 0, 'x-hidden' => 1 }],
    # kept out of the storage config, so sensitive whatever its schema declares
    ['fixture', 'fixture-token', { sensitive => 1 }],
    ['fixture', 'fixture-secret', { sensitive => 1 }],
    # without 'sensitive-properties' the properties that were kept out before it existed apply
    ['legacyfixture', 'master-pubkey', { sensitive => 1 }],
];

for my $test ($property_tests->@*) {
    my ($type, $property, $expected) = $test->@*;

    my $schema = $api->info({ type => $type })->{schema}->{$property};
    is_deeply({ $schema->%{ keys $expected->%* } }, $expected, "$type: schema of $property");
}

# each test is comprised of the following array keys:
# [0] => storage type
# [1] => expected metadata of the plugin, without its schema
my $metadata_tests = [
    [
        'pbs',
        {
            type => 'pbs',
            origin => 'builtin',
            content => { supported => ['backup', 'none'], default => ['backup'] },
        },
    ],
    [
        'fixture',
        {
            type => 'fixture',
            origin => 'custom',
            'display-name' => 'Fixture Storage',
            content => { supported => ['images', 'none'], default => ['none'] },
        },
    ],
];

for my $test ($metadata_tests->@*) {
    my ($type, $expected) = $test->@*;

    my $metadata = $api->info({ type => $type });
    my ($listed) = grep { $_->{type} eq $type } $api->index({})->@*;
    is_deeply($listed, $metadata, "$type: listed like queried");

    delete($metadata->{schema});
    is_deeply($metadata, $expected, "$type: metadata");
}

# each test is comprised of the following array keys:
# [0] => origin to filter by, if any
# [1] => storage types that are expected in the list
# [2] => storage types that must not be in the list
my $origin_tests = [
    [undef, ['dir', 'fixture'], []],
    ['builtin', ['dir'], ['fixture']],
    ['custom', ['fixture'], ['dir']],
];

for my $test ($origin_tests->@*) {
    my ($origin, $expected, $unexpected) = $test->@*;

    my $list = $api->index({ defined($origin) ? (origin => $origin) : () });
    my $listed = { map { $_->{type} => 1 } $list->@* };
    my $name = 'plugin list' . (defined($origin) ? " filtered by origin $origin" : '');

    is_deeply([grep { $listed->{$_} } $expected->@*, $unexpected->@*], $expected, $name);
    is(scalar(grep { $_->{origin} ne $origin } $list->@*), 0, "$name: origins") if $origin;
}

done_testing();

1;
