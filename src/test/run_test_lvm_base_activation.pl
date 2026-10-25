#!/usr/bin/perl

use strict;
use warnings;

use lib '..';
use Test::More;
use Test::MockModule;

use PVE::Storage;
use PVE::Storage::LVMPlugin;

my $CLASS = 'PVE::Storage::LVMPlugin';

# the volume groups that vgs lists with its device hints, and those that only a listing without
# hints finds
my ($vgs, $unhinted, @calls) = ([], []);

my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm->mock(
    lvm_vgs => sub {
        my ($includepvs, $nohints) = @_;
        push @calls, $nohints ? 'vgs --nohints' : 'vgs';
        return { map { $_ => {} } $vgs->@*, ($nohints ? $unhinted->@* : ()) };
    },
    run_command => sub {
        my ($cmd) = @_;
        push @calls, join(' ', $cmd->[0] =~ s|^/sbin/||r, $cmd->@[1 .. $#$cmd]);
    },
);

my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(config => sub { return { ids => {} } });
$storage->mock(storage_config => sub { return { type => 'fakebase' } });

my $base_fails;
my $plugin = Test::MockModule->new('PVE::Storage::Plugin');
$plugin->mock(lookup => sub { return 'FakeBasePlugin' });

{
    no warnings 'once';
    *FakeBasePlugin::activate_volume = sub {
        my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
        push @calls, "base $storeid:$volname" . ($cache->{marker} ? ' shared-cache' : '');
        die "volume '$volname' is not available on this node\n" if $base_fails;
        # the device appeared while waiting, and with it the volume group
        push $vgs->@*, $volname =~ s/^vol-//r;
    };
}

my sub activate {
    my ($scfg, $cache, $vgs_now, $unhinted_now) = @_;

    ($vgs, $unhinted, @calls) = ($vgs_now // [], $unhinted_now // []);
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    $CLASS->activate_storage('lvm', $scfg, $cache);
    return (join(', ', @calls), \@warnings);
}

my $with_base = { vgname => 'vg1', base => 'san:vol-vg1' };

my ($calls, $warnings) = activate($with_base, { vgs => { vg1 => {} } });
is($calls, '', 'present volume group: no base activation, no scan');

($calls) = activate($with_base, { vgs => {} }, ['vg1']);
is($calls, 'vgs', 'volume group appeared meanwhile: a fresh list, no base activation');

my $cache = { marker => 1, vgs => {}, lvs => {} };
($calls) = activate($with_base, $cache);
is(
    $calls,
    'vgs, base san:vol-vg1 shared-cache, vgs',
    'missing volume group: base activated with the shared cache, the device brings it into the list',
);
ok($cache->{vgs}->{vg1}, 'and the volume group list of the cache is refreshed');
ok(!exists($cache->{lvs}), 'and its logical volume list is dropped');
$cache = { vgs => { vgold => {} } };
activate($with_base, $cache, ['vg1']);
ok($cache->{vgs}->{vgold}, 'a volume group found before stays in the list');

# another node created the volume group after this node probed the device, which it had before
$cache = { vgs => {}, vgscaned => 1 };
{
    no warnings 'redefine';
    local *FakeBasePlugin::activate_volume = sub { push @calls, 'base present' };
    ($calls) = activate($with_base, $cache, [], ['vg1']);
}
is(
    $calls,
    'vgs, base present, vgs, vgs --nohints, pvscan --cache',
    'volume group missing from the hints: found without them despite an earlier probe',
);
ok($cache->{vgs}->{vg1}, 'and added to the volume group list of the cache');

$cache = { vgs => {} };
activate($with_base, $cache);
($calls) = activate({ vgname => 'vg2', base => 'san:vol-vg2' }, $cache, ['vg1']);
is($calls, 'vgs, base san:vol-vg2, vgs', 'a second base in the same cache is activated');

$base_fails = 1;
$cache = { vgs => {} };
($calls, $warnings) = activate($with_base, $cache);
is(
    $calls,
    'vgs, base san:vol-vg1, vgs --nohints',
    'failed base activation: only the listing without hints of the cache follows',
);
like(
    $warnings->[0] // '',
    qr/^storage 'lvm': activating base volume 'san:vol-vg1' failed - volume/,
    'and warns about it',
);
($calls) = activate({ vgname => 'vg2', base => 'san:vol-vg2' }, $cache);
is(
    $calls,
    'vgs, base san:vol-vg2',
    'second failed base activation in the cache: no listing without hints again',
);
$base_fails = 0;

$cache = { vgs => {} };
($calls) = activate({ vgname => 'vg1' }, $cache);
is($calls, 'vgs --nohints', 'without base: one listing without hints per cache');
($calls) = activate({ vgname => 'vg3' }, $cache);
is($calls, '', 'and not again for the next storage');

done_testing();
