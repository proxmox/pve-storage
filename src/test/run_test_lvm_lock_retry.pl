#!/usr/bin/perl

# Unit test for the storage-lock acquisition retry used by LVMPlugin during snapshot and volume
# removal cleanup (lock_storage_with_acquire_retry). It is driven through the public
# volume_snapshot_delete; cluster_lock_storage, run_command and sleep are mocked, so the test needs
# neither root nor a real LVM setup. The invariant under test: only lock *acquisition* is retried,
# the (non-idempotent) cleanup is entered at most once and never repeated.

use strict;
use warnings;

# Make the retry backoff instant; must be installed before LVMPlugin is compiled so its bareword
# sleep() binds to this override. Also record that a backoff happened.
my $slept = 0;

BEGIN {
    *CORE::GLOBAL::sleep = sub { $slept += ($_[0] // 0); return 0; };
}

use lib '..';
use Test::More;

use PVE::Storage::LVMPlugin;

my $CLASS = 'PVE::Storage::LVMPlugin';

# scripted mocks: one @lock_script entry is consumed per cluster_lock_storage call
my @lock_script;
my $lock_calls;
my $cleanup_runs;
my $cleanup_dies;

{
    no warnings qw(redefine once);
    # mimic cluster_lock_storage/cfs_lock: on an acquisition timeout the locked code is never
    # entered, on success the passed code runs exactly once
    *PVE::Storage::LVMPlugin::cluster_lock_storage = sub {
        my ($class, $storeid, $shared, $timeout, $func, @param) = @_;
        $lock_calls++;
        my $behavior = shift(@lock_script) // 'timeout';
        die "cfs-lock 'storage-$storeid' error: got lock request timeout\n"
            if $behavior eq 'timeout';
        return $func->(@param);
    };
    # the only run_command on this path is the lvremove of the snapshot volume
    *PVE::Storage::LVMPlugin::run_command = sub {
        $cleanup_runs++;
        die "lvremove failed\n" if $cleanup_dies;
        return 0;
    };
}

# keep the expected retry warnings out of the test output, but surface anything unexpected
$SIG{__WARN__} = sub {
    my ($msg) = @_;
    warn $msg if $msg !~ /could not acquire storage lock, retrying/;
};

my $scfg = { type => 'lvm', vgname => 'test-vg' }; # no saferemove, so no forked worker

my sub reset_mocks {
    my (@script) = @_;
    @lock_script = @script;
    $lock_calls = 0;
    $cleanup_runs = 0;
    $cleanup_dies = 0;
    $slept = 0;
}

my sub delete_snapshot {
    return $CLASS->volume_snapshot_delete($scfg, 'mystore', 'vm-100-disk-0.qcow2', 'snap1', 1);
}

# acquired immediately: no retry, cleanup runs once
reset_mocks('run');
ok(eval { delete_snapshot(); 1 }, 'succeeds when the lock is acquired on the first try')
    or diag($@);
is($lock_calls, 1, 'acquired exactly once');
is($cleanup_runs, 1, 'cleanup ran exactly once');

# contention then success: acquisition is retried, cleanup still runs only once
reset_mocks('timeout', 'timeout', 'run');
ok(eval { delete_snapshot(); 1 }, 'retries acquisition and then succeeds') or diag($@);
is($lock_calls, 3, 'retried until the third acquisition');
is($cleanup_runs, 1, 'cleanup ran exactly once, not repeated across retries');
ok($slept > 0, 'backed off between acquisition attempts');

# failure from inside the locked cleanup must not be retried
reset_mocks('run');
$cleanup_dies = 1;
ok(!eval { delete_snapshot(); 1 }, 'propagates a failure raised inside the locked cleanup');
is($lock_calls, 1, 'did not retry once the cleanup had started');
is($cleanup_runs, 1, 'cleanup was entered exactly once');

# persistent contention: give up after the capped number of attempts, cleanup never runs
reset_mocks(('timeout') x 10);
ok(!eval { delete_snapshot(); 1 }, 'gives up after exhausting acquisition retries');
is($lock_calls, 5, 'tried exactly the capped number of times');
is($cleanup_runs, 0, 'cleanup never ran because the lock was never acquired');

done_testing();
