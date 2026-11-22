package PVE::Storage::TestPluginIsolation;

use v5.36;

use lib qw(..);

use Test::MockModule;
use Test::More;

use PVE::Storage::Plugin;

# declares 'is_mountpoint' itself, with another schema than the built-in plugins use for it, and
# uses properties of the base plugin
package PVE::Storage::Custom::IsolatedFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'isolated';
    }

    sub plugindata {
        return {
            content => [{ iso => 1, none => 1 }, { none => 1 }],
            'isolate-properties' => 1,
        };
    }

    sub properties {
        return {
            is_mountpoint => { type => 'string', enum => ['always', 'never'], optional => 1 },
            'fixture-id' => { type => 'string' },
        };
    }

    sub options {
        return {
            path => { fixed => 1 },
            bwlimit => { optional => 1 },
            content => { optional => 1 },
            disable => { optional => 1 },
        };
    }
}

# does not isolate its properties and derives a required one when a storage gets added
package PVE::Storage::Custom::DerivingFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'deriving';
    }

    sub plugindata {
        return { content => [{ none => 1 }, { none => 1 }] };
    }

    sub properties {
        return { 'derived-id' => { type => 'string' } };
    }

    sub options {
        return { 'derived-id' => { fixed => 1 }, content => { optional => 1 } };
    }

    sub check_config($class, $storeid, $config, $create, $skip_schema_check) {
        $config->{'derived-id'} //= "id-of-$storeid" if $create;
        return $class->SUPER::check_config($storeid, $config, $create, $skip_schema_check);
    }
}

my $hook_params;

# does not isolate its properties and has no 'sensitive-properties', so it gets the fallback ones,
# without listing them in its options
package PVE::Storage::Custom::FallbackFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'fallback';
    }

    sub plugindata {
        return { content => [{ none => 1 }, { none => 1 }] };
    }

    sub options {
        return { content => { optional => 1 }, disable => { optional => 1 } };
    }

    sub on_add_hook($class, $storeid, $scfg, %sensitive) {
        $hook_params = {%sensitive};
        return;
    }

    sub on_update_hook($class, $storeid, $scfg, %sensitive) {
        $hook_params = {%sensitive};
        return;
    }
}

# lists its own sensitive property and password, which other types can change, as fixed options,
# and declares another one that it does not list
package PVE::Storage::Custom::FixedSecretFixturePlugin {
    our @ISA = ('PVE::Storage::Custom::FallbackFixturePlugin');

    sub type {
        return 'fixedsecret';
    }

    sub plugindata {
        return {
            content => [{ none => 1 }, { none => 1 }],
            'sensitive-properties' =>
                { 'fixed-secret' => 1, password => 1, 'unlisted-secret' => 1 },
        };
    }

    sub properties {
        return {
            'fixed-secret' => { type => 'string', optional => 1 },
            'unlisted-secret' => { type => 'string', optional => 1 },
        };
    }

    sub options {
        return {
            'fixed-secret' => { optional => 1, fixed => 1 },
            password => { optional => 1, fixed => 1 },
            content => { optional => 1 },
        };
    }
}

# has to happen before loading PVE::Storage, which initializes all registered plugins
PVE::Storage::Custom::IsolatedFixturePlugin->register();
PVE::Storage::Custom::DerivingFixturePlugin->register();
PVE::Storage::Custom::FallbackFixturePlugin->register();
PVE::Storage::Custom::FixedSecretFixturePlugin->register();

require PVE::Storage;
require PVE::API2::Storage::Config;

my $raw = <<'EOF';
dir: local
	path /var/lib/vz
	content iso

isolated: fixture
	path /mnt/fixture
	fixture-id one
	content iso

fallback: secrets
	content none

fixedsecret: fixed
	content none
EOF

my $written;
my $mock_storage = Test::MockModule->new('PVE::Storage', no_auto => 1);
$mock_storage->redefine(
    lock_storage_config => sub { $_[0]->(); },
    config => sub { return PVE::Storage::Plugin->parse_config('storage.cfg', $raw); },
    write_config => sub { $written = $_[0]->{ids}; },
    activate_storage => sub { },
);

ok(PVE::Storage::Plugin->has_isolated_properties(), "one isolating plugin isolates the schema");

# each test is comprised of the following array keys:
# [0] => description
# [1] => API method
# [2] => parameters
# [3] => expected error, or the property and its value that have to end up in the config
my $tests = [
    [
        'built-in storage, no type',
        'update',
        { storage => 'local', disable => 1 },
        [disable => 1],
    ],
    [
        'built-in storage, its type',
        'update',
        { storage => 'local', type => 'dir', disable => 1 },
        [disable => 1],
    ],
    [
        'built-in storage, other type',
        'update',
        { storage => 'local', type => 'nfs', disable => 1 },
        qr/cannot change storage type \(expected 'dir', got 'nfs'\)/,
    ],
    [
        'built-in storage, property of the isolating plugin',
        'update',
        { storage => 'local', 'fixture-id' => 'two' },
        qr/fixture-id: property is not defined in schema/,
    ],
    [
        'built-in schema of a property both declare',
        'update',
        { storage => 'local', is_mountpoint => 'yes' },
        [is_mountpoint => 'yes'],
    ],
    [
        'isolated schema of that property',
        'update',
        { storage => 'fixture', is_mountpoint => 'always' },
        [is_mountpoint => 'always'],
    ],
    [
        'isolated storage, value for the built-in schema',
        'update',
        { storage => 'fixture', is_mountpoint => 'yes' },
        qr/is_mountpoint: value 'yes' does not have a value in the enumeration/,
    ],
    [
        'isolated storage, property of a built-in plugin',
        'update',
        { storage => 'fixture', nocow => 1 },
        qr/nocow: property is not defined in schema/,
    ],
    [
        'isolated storage, property of the base plugin',
        'update',
        { storage => 'fixture', bwlimit => 'migration=100' },
        [bwlimit => 'migration=100'],
    ],
    [
        'isolated storage, property of the base plugin it does not use',
        'update',
        { storage => 'fixture', shared => 1 },
        qr/shared: property is not defined in schema/,
    ],
    [
        'isolated storage, delete an own required property',
        'update',
        { storage => 'fixture', delete => 'fixture-id' },
        qr/unable to delete required option 'fixture-id'/,
    ],
    [
        'unknown storage',
        'update',
        { storage => 'missing', disable => 1 },
        qr/storage 'missing' does not exist/,
    ],
    [
        'create isolated storage',
        'create',
        { storage => 'new', type => 'isolated', path => '/mnt/new', 'fixture-id' => 'two' },
        [path => '/mnt/new'],
    ],
    [
        'create isolated storage without an own required property',
        'create',
        { storage => 'new', type => 'isolated', path => '/mnt/new' },
        qr/fixture-id: property is missing and it is not optional/,
    ],
    [
        'create isolated storage without a required property of the base plugin',
        'create',
        { storage => 'new', type => 'isolated', 'fixture-id' => 'two' },
        qr/path: property is missing and it is not optional/,
    ],
    [
        'create built-in storage whose path is derived from its ID',
        'create',
        { storage => 'new', type => 'nfs', server => '192.0.2.1', export => '/export' },
        [path => '/mnt/pve/new'],
    ],
    [
        'create storage of a plugin that derives a required property',
        'create',
        { storage => 'new', type => 'deriving' },
        ['derived-id' => 'id-of-new'],
    ],
    [
        'create built-in storage with a property of the isolating plugin',
        'create',
        { storage => 'new', type => 'dir', path => '/srv/new', 'fixture-id' => 'two' },
        qr/fixture-id: property is not defined in schema/,
    ],
];

for my $test ($tests->@*) {
    my ($description, $method, $param, $expected) = $test->@*;

    $written = undef;
    eval { PVE::API2::Storage::Config->$method({ $param->%* }) };
    my $err = $@;

    if (ref($expected) eq 'Regexp') {
        like("$err", $expected, "$description: refused");
        is($written, undef, "$description: nothing written");
    } else {
        my ($property, $value) = $expected->@*;
        is("$err", '', "$description: accepted");
        is($written->{ $param->{storage} }->{$property}, $value, "$description: written");
    }
}

# A sensitive property goes to the hooks, not to the config, so the API never checked it against
# the options of the type. Plugins that do not isolate their properties keep relying on that.
my $sensitive_tests = [
    [
        'create, fallback property',
        'create',
        { storage => 'new', type => 'fallback', password => 'secret' },
        { password => 'secret' },
    ],
    [
        'update, fallback property',
        'update',
        { storage => 'secrets', 'encryption-key' => 'key' },
        { 'encryption-key' => 'key' },
    ],
    [
        'delete a fallback property that is no option',
        'update',
        { storage => 'secrets', delete => 'password' },
        qr/no such option 'password'/,
    ],
    [
        'create, fixed sensitive property',
        'create',
        { storage => 'new', type => 'fixedsecret', 'fixed-secret' => 'secret' },
        { 'fixed-secret' => 'secret' },
    ],
    [
        'update, fixed sensitive property',
        'update',
        { storage => 'fixed', 'fixed-secret' => 'secret' },
        qr/fixed-secret: property is not defined in schema/,
    ],
    [
        'create, fixed sensitive property another type can change',
        'create',
        { storage => 'new', type => 'fixedsecret', password => 'secret' },
        { password => 'secret' },
    ],
    [
        'update, fixed sensitive property another type can change',
        'update',
        { storage => 'fixed', password => 'secret' },
        qr/password: property is not defined in schema/,
    ],
    [
        'create, sensitive property no plugin lists',
        'create',
        { storage => 'new', type => 'fixedsecret', 'unlisted-secret' => 'secret' },
        { 'unlisted-secret' => 'secret' },
    ],
    [
        'update, sensitive property no plugin lists',
        'update',
        { storage => 'fixed', 'unlisted-secret' => 'secret' },
        qr/unlisted-secret: property is not defined in schema/,
    ],
    [
        'isolated storage, fallback property',
        'update',
        { storage => 'fixture', password => 'secret' },
        qr/password: property is not defined in schema/,
    ],
    [
        'built-in storage, sensitive property of another type',
        'update',
        { storage => 'local', password => 'secret' },
        qr/password: property is not defined in schema/,
    ],
];

for my $test ($sensitive_tests->@*) {
    my ($description, $method, $param, $expected) = $test->@*;

    ($written, $hook_params) = (undef, undef);
    eval { PVE::API2::Storage::Config->$method({ $param->%* }) };
    my $err = $@;

    if (ref($expected) eq 'Regexp') {
        like("$err", $expected, "$description: refused");
        is($written, undef, "$description: nothing written");
    } else {
        is("$err", '', "$description: accepted");
        is_deeply($hook_params, $expected, "$description: passed to the hook");
        my $config = $written->{ $param->{storage} };
        ok(!grep({ exists($config->{$_}) } keys $expected->%*), "$description: not in the config");
    }
}

# The loader checks a third-party plugin that isolates its properties before registering it, here
# with variations of the fixture that are never registered.
my $variant_count = 0;
my sub subclass_with($base, %methods) {
    my $class = 'PVE::Storage::Custom::IsolatedFixtureVariant' . $variant_count++;

    no strict 'refs'; ## no critic (ProhibitNoStrict)
    @{"${class}::ISA"} = ($base);
    for my $name (keys %methods) {
        my $value = $methods{$name};
        *{"${class}::${name}"} = sub { return $value; };
    }
    return $class;
}

my sub fixture_with(%methods) {
    return subclass_with('PVE::Storage::Custom::IsolatedFixturePlugin', %methods);
}

my $isolating = { content => [{ none => 1 }, { none => 1 }], 'isolate-properties' => 1 };

# each test is comprised of the following array keys:
# [0] => description
# [1] => plugin class
# [2] => API version of the plugin
# [3] => expected error, if any
my $loader_tests = [
    ['isolating plugin', 'PVE::Storage::Custom::IsolatedFixturePlugin', 17],
    [
        'plugin derived from the directory plugin',
        subclass_with('PVE::Storage::DirPlugin', plugindata => $isolating),
        17,
    ],
    [
        'API version without support for isolation',
        'PVE::Storage::Custom::IsolatedFixturePlugin',
        16,
        qr/needs API version 17 or newer \(has 16\)/,
    ],
    [
        'property of the base plugin declared again',
        fixture_with(properties => { content => { type => 'integer' } }),
        17,
        qr/declares the property 'content' of the base plugin/,
    ],
    [
        'parameter for the config digest declared',
        fixture_with(properties => { digest => { type => 'string', optional => 1 } }),
        17,
        qr/declares the property 'digest', which is a parameter of the API/,
    ],
    [
        'parameter for deleting properties declared',
        fixture_with(properties => { delete => { type => 'boolean', optional => 1 } }),
        17,
        qr/declares the property 'delete', which is a parameter of the API/,
    ],
    [
        'properties exposed to other plugins',
        fixture_with(plugindata => { $isolating->%*, 'expose-properties' => 1 }),
        17,
        qr/exposes its isolated properties/,
    ],
];

for my $test ($loader_tests->@*) {
    my ($description, $class, $api_version, $expected) = $test->@*;

    eval { PVE::Storage::assert_isolating_plugin_is_loadable($class, $api_version) };
    if ($expected) {
        like($@, $expected, "loader refuses: $description");
    } else {
        is($@, '', "loader accepts: $description");
    }
}

done_testing();
