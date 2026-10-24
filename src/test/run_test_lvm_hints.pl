#!/usr/bin/perl

# Unit test for the activation of an LVM storage whose volume group is missing from the volume
# groups LVM lists through its device hints, as after another node created it on a device this node
# already had: a listing without the hints finds it, and only then the hints are rebuilt. vgs and
# pvscan are mocked, so the test needs neither root nor a real LVM setup.

use strict;
use warnings;

use lib '..';
use Test::More;
use Test::MockModule;

use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;

my $CLASS = 'PVE::Storage::LVMPlugin';

# the volume groups vgs lists with and without the hints, and the commands run
my ($hinted, $all, $vgs_fails, $pvscan_fails, @calls);

my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm->mock(
    run_command => sub {
        my ($cmd, %param) = @_;
        if ($cmd->[0] eq '/sbin/vgs') {
            my $nohints = grep { $_ eq '--nohints' } @$cmd;
            push @calls, $nohints ? 'vgs --nohints' : 'vgs';
            die "vgs failed\n" if $nohints && $vgs_fails;
            $param{outfunc}->("$_:1024:512:0") for ($nohints ? $all : $hinted)->@*;
        } elsif ($cmd->[0] eq '/sbin/pvscan') {
            push @calls, 'pvscan --cache';
            die "pvscan failed\n" if $pvscan_fails;
        } else {
            push @calls, join(' ', @$cmd);
        }
    },
);

my sub activate {
    my ($vgname, $cache) = @_;

    @calls = ();
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    $CLASS->activate_storage('lvm', { vgname => $vgname }, $cache);
    return (join(', ', @calls), \@warnings);
}

($hinted, $all) = (['vg1'], ['vg1']);
my ($calls) = activate('vg1', {});
is($calls, 'vgs', 'listed volume group: no listing without the hints');

($hinted, $all) = (['vg1'], ['vg1', 'vg2']);
my $cache = { lvs => { vg1 => {} } };
($calls) = activate('vg2', $cache);
is($calls, 'vgs, vgs --nohints, pvscan --cache', 'unhinted volume group: hints rebuilt');
ok($cache->{vgs}->{vg2} && $cache->{vgs}->{vg1}, 'and added to the volume groups of the cache');
ok(!exists($cache->{lvs}), 'and the logical volume list of the cache is dropped');
is_deeply(
    [$CLASS->status('lvm', { vgname => 'vg2' }, $cache)],
    [1024, 512, 512, 1],
    'the status of the same cache sees the volume group',
);

($calls) = activate('vg3', $cache);
is($calls, '', 'once per cache: no second listing for the next missing volume group');

($hinted, $all) = (['vg1'], ['vg1']);
($calls) = activate('vg2', {});
is($calls, 'vgs, vgs --nohints', 'volume group missing everywhere: hints left alone');

($hinted, $all, $vgs_fails) = (['vg1'], [], 1);
$cache = {};
my $warnings;
($calls, $warnings) = activate('vg2', $cache);
is($calls, 'vgs, vgs --nohints', 'failed listing without the hints: hints left alone');
ok($cache->{vgs}->{vg1}, 'and the volume groups found before stay');
like($warnings->[0] // '', qr/vgs failed/, 'and it warns');
$vgs_fails = 0;

($hinted, $all, $pvscan_fails) = (['vg1'], ['vg1', 'vg2'], 1);
$cache = {};
($calls, $warnings) = activate('vg2', $cache);
ok($cache->{vgs}->{vg2}, 'failed rebuild of the hints: the volume group still counts');
like(
    $warnings->[0] // '',
    qr/^rebuilding the LVM device hints failed - pvscan failed/,
    'and it warns about the rebuild',
);
$pvscan_fails = 0;

# a thin pool storage activates through the LVM activation and reads the logical volume list
my $listed;
$lvm->mock(
    lvm_list_volumes => sub {
        $listed++;
        return { vg2 => { data => { lv_type => 't', lv_state => 'a' } } };
    },
);
($hinted, $all) = (['vg1'], ['vg1', 'vg2']);
$cache = { lvs => { vg1 => {} } };
PVE::Storage::LvmThinPlugin->activate_storage(
    'thin', { vgname => 'vg2', thinpool => 'data' }, $cache,
);
ok($listed, 'thin pool: the logical volumes are listed again after the volume group was found');

done_testing();
