package PVE::Storage::TestPluginIsolation;

use v5.36;

use lib qw(..);

use Test::MockModule;
use Test::More;

use PVE::Storage::Plugin;

# declares 'is_mountpoint' itself, with another schema than the built-in plugins use for it
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
            content => { optional => 1 },
            disable => { optional => 1 },
        };
    }
}

# has to happen before loading PVE::Storage, which initializes all registered plugins
PVE::Storage::Custom::IsolatedFixturePlugin->register();

require PVE::Storage;
require PVE::API2::Storage::Config;

my $raw = <<'EOF';
dir: local
	path /var/lib/vz
	content iso

isolated: fixture
	fixture-id one
	content iso
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
        { storage => 'new', type => 'isolated', 'fixture-id' => 'two' },
        ['fixture-id' => 'two'],
    ],
    [
        'create isolated storage without an own required property',
        'create',
        { storage => 'new', type => 'isolated', is_mountpoint => 'never' },
        qr/fixture-id: property is missing and it is not optional/,
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

done_testing();
