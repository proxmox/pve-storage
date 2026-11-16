package PVE::Storage::TestPluginLoad;

use v5.36;

use lib qw(..);

use Test::More;

use PVE::Storage;

my sub plugin($type, %methods) {
    my $class = "PVE::Storage::Custom::LoadFixture" . ucfirst($type);

    no strict 'refs'; ## no critic (ProhibitNoStrict)
    @{"${class}::ISA"} = ('PVE::Storage::Plugin');
    $methods{type} = $type;
    $methods{plugindata} //= { content => [{ none => 1 }, { none => 1 }] };
    for my $name (keys %methods) {
        my $value = $methods{$name};
        *{"${class}::${name}"} = sub { return $value; };
    }
    $class->register();
    return $class;
}

my $string = { type => 'string', optional => 1 };

my $isolating = { content => [{ none => 1 }, { none => 1 }], 'isolate-properties' => 1 };

my $plugins = [
    plugin('duplicate', properties => { server => $string }),
    plugin('undeclared', options => { 'load-missing' => { optional => 1 } }),
    plugin('consumer', options => { 'load-shared' => { optional => 1 } }),
    plugin('provider', properties => { 'load-shared' => $string }),
    plugin('second', properties => { 'load-shared' => $string }),
    plugin('foreign', plugindata => $isolating, options => { export => { optional => 1 } }),
    plugin('isolated', plugindata => $isolating, properties => { server => $string }),
];

my @warnings;
{
    local $SIG{__WARN__} = sub { push @warnings, $_[0]; };
    PVE::Storage::init_plugins($plugins);
}

my $loaded = { map { $_ => 1 } PVE::Storage::Plugin->lookup_types()->@* };

ok($loaded->{$_}, "plugin '$_' is loaded") for qw(dir nfs consumer provider isolated);
ok(!$loaded->{$_}, "plugin '$_' is left out") for qw(duplicate undeclared second foreign);

my $warned = join('', sort @warnings);
like($warned, qr/LoadFixtureDuplicate": duplicate property 'server'/,
    "duplicate of an in-tree one");
like($warned, qr/LoadFixtureUndeclared": undefined property 'load-missing'/, "undeclared option");
like($warned, qr/LoadFixtureSecond": duplicate property 'load-shared'/, "duplicate of another one");
like(
    $warned,
    qr/LoadFixtureForeign": isolated plugin cannot use property 'export'/,
    "foreign option",
);
is(scalar(@warnings), 4, "no other plugin is reported");

ok(PVE::Storage::Plugin->get_property_schema('nfs', 'server'), "the in-tree plugins work");

done_testing();
