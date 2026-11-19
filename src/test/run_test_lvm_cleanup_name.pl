#!/usr/bin/perl

# Unit test for freeing a volume while an earlier one with the same name is still pending removal.

use strict;
use warnings;

use lib '..';
use Test::More;
use Test::MockModule;

use PVE::Storage::LVMPlugin;

my $CLASS = 'PVE::Storage::LVMPlugin';
my $vg = 'test-vg';
my $scfg = { type => 'lvm', vgname => $vg, saferemove => 1 };
my $volname = 'vm-100-disk-0.qcow2';
my $snapvolname = 'snap_vm-100-disk-0_snap1.qcow2';

my $lvs; # names of the existing LVs
my @renames;
my @activated;
my @removed;
my $workers;

my $lvm_mock = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm_mock->redefine(
    cluster_lock_storage => sub {
        my ($class, $storeid, $shared, $timeout, $func, @param) = @_;
        return $func->(@param);
    },
    lvm_vgs => sub {
        return { $vg => {} };
    },
    lvm_list_volumes => sub {
        return { $vg => { map { $_ => {} } keys %$lvs } };
    },
    run_command => sub {
        my ($cmd) = @_;
        if ($cmd->[0] eq '/sbin/lvrename') {
            my (undef, undef, $from, $to) = @$cmd;
            die "Logical Volume \"$from\" not found in volume group \"$vg\"\n"
                if !$lvs->{$from};
            die "Logical Volume \"$to\" already exists in volume group \"$vg\"\n"
                if $lvs->{$to};
            push @renames, "$from -> $to";
            delete $lvs->{$from};
            $lvs->{$to} = 1;
        } elsif ($cmd->[0] eq '/sbin/lvremove') {
            my $name = $cmd->[-1] =~ s|^\Q$vg\E/||r;
            die "Failed to find logical volume \"$vg/$name\"\n" if !$lvs->{$name};
            push @removed, $name;
            delete $lvs->{$name};
        } elsif ($cmd->[0] eq '/sbin/lvchange' && $cmd->[1] eq '-aly') {
            push @activated, $cmd->[2];
        }
        return 0;
    },
    activate_volume => sub { },
    # no volume has a device mapper link, so the cleanup worker cannot zero out
    abs_path => sub { return undef },
    volume_snapshot_info => sub {
        return {
            snap1 => { file => '/dev/null', volname => $snapvolname, child => 'current' },
            current => { file => '/dev/null', volname => $volname, parent => 'snap1' },
        };
    },
);

# makes allocating the new current volume of a rollback fail
my $plugin_mock = Test::MockModule->new('PVE::Storage::Plugin');
$plugin_mock->redefine(file_size_info => sub { return undef });

my $rpcenv_mock = Test::MockModule->new('PVE::RPCEnvironment');
$rpcenv_mock->redefine(
    get => sub { return bless({}, 'PVE::RPCEnvironment') },
    get_user => sub { return 'root@pam' },
    fork_worker => sub { $workers++; return 'UPID:test' },
);

my sub setup {
    my (@names) = @_;
    $lvs = { map { $_ => 1 } @names };
    @renames = ();
    @activated = ();
    @removed = ();
    $workers = 0;
}

# keep plugin progress and errors out of the test output
my sub quietly {
    my ($code) = @_;
    local *STDOUT;
    local *STDERR;
    open(STDOUT, '>', '/dev/null') or die "cannot open /dev/null - $!\n";
    open(STDERR, '>', '/dev/null') or die "cannot open /dev/null - $!\n";
    my $ok = eval { $code->(); 1 };
    my $err = $@;
    return ($ok, $err);
}

# the previous current volume replaced by a rollback is still pending removal
setup($volname, $snapvolname, "del-$volname");
my ($ok, $err) =
    quietly(sub { $CLASS->volume_snapshot_delete($scfg, 'mystore', $volname, 'snap1', 0) });
ok($ok, 'deleting the snapshot right after a rollback succeeds') or diag($err);
is_deeply(
    \@renames,
    ["$volname -> del-1-$volname", "$snapvolname -> $volname"],
    'frees the current volume under a numbered name',
);
ok($lvs->{"del-$volname"}, 'volume replaced by the rollback is left alone');
is($workers, 1, 'started a cleanup worker');

setup($volname, $snapvolname, "del-$volname");
($ok, $err) =
    quietly(sub { $CLASS->volume_snapshot_rollback($scfg, 'mystore', $volname, 'snap1') });
ok(!$ok, 'second rollback fails when the new volume cannot be allocated');
like($err, qr/original volume restored/, 'reports the original volume as restored');
is_deeply(
    \@renames,
    ["$volname -> del-1-$volname", "del-1-$volname -> $volname"],
    'restores the volume it renamed, not the one of the earlier rollback',
);
ok($lvs->{"del-$volname"}, 'volume replaced by the earlier rollback is left alone');
is($workers, 0, 'started no cleanup worker');

my $rawvolname = 'vm-100-disk-1';

# only a numbered volume is still pending removal, so del-<name> is free
setup($rawvolname, "del-1-$rawvolname");
my $discard_scfg = { type => 'lvm', vgname => $vg, 'on-volume-remove' => 'discard=1' };
my $worker = $CLASS->free_image('mystore', $discard_scfg, $rawvolname, 0, 'raw');
is_deeply(\@renames, ["$rawvolname -> del-$rawvolname"], 'renamed to del-<name> if that is free');
($ok, $err) = quietly($worker);
ok($ok, 'cleanup worker succeeds') or diag($err);
is_deeply(\@activated, ["/dev/$vg/del-$rawvolname"], 'cleanup worker activates its own volume');
is_deeply(\@removed, ["del-$rawvolname"], 'cleanup worker removes its own volume');
ok($lvs->{"del-1-$rawvolname"}, 'volume pending removal is left alone');

# zeroing out fails without a device mapper link, so the cleanup worker renames its volume
setup($rawvolname, "del-$rawvolname", "del-1-$rawvolname");
$worker = $CLASS->free_image('mystore', $scfg, $rawvolname, 0, 'raw');
is_deeply(
    \@renames,
    ["$rawvolname -> del-2-$rawvolname"],
    'renamed to the first free numbered name',
);
@renames = ();
($ok, $err) = quietly($worker);
ok(!$ok, 'cleanup worker fails');
like($err, qr/cleanup failed for 1 out of 1 volumes/, 'cleanup worker reports the failed cleanup');
is_deeply(\@activated, ["/dev/$vg/del-2-$rawvolname"], 'cleanup worker activates its own volume');
is_deeply(
    \@renames,
    ["del-2-$rawvolname -> failed-0-del-2-$rawvolname"],
    'failed cleanup renames its own volume',
);
is_deeply(\@removed, [], 'failed cleanup removes no volume');
ok(
    $lvs->{"del-$rawvolname"} && $lvs->{"del-1-$rawvolname"},
    'volumes pending removal are left alone',
);

done_testing();
