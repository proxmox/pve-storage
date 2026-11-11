#!/usr/bin/perl

use strict;
use warnings;

our $ignore_command_kill = 0;

BEGIN {
    *CORE::GLOBAL::kill = sub {
        # Model an unkillable command without preventing run_fork_with_timeout from killing Perl.
        return scalar(@_) - 1 if $ignore_command_kill && caller eq 'PVE::Cmd';
        return CORE::kill(@_);
    };
}

use lib '..';
use Cwd qw(abs_path);
use Fcntl qw(:flock O_CREAT O_RDWR);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use PVE::Storage;
use PVE::Syscall;

my $dir = tempdir('lvm-status-XXXXXX', DIR => '.', CLEANUP => 1);
local $PVE::Storage::LVMPlugin::LVM_STATUS_LOCK = "$dir/lock";
local $PVE::Storage::LVMPlugin::LVM_STATUS_TIMEOUT = 10;

my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(uevent_seqnum => sub { return 1 });
my $directory = Test::MockModule->new('PVE::Storage::DirPlugin');
$directory->mock(
    activate_storage => sub { },
    status => sub { return (1024, 512, 512, 1) },
);

my $cfg = {
    ids => {
        lvm1 => { type => 'lvm', vgname => 'vg1', content => { images => 1 } },
        lvm2 => { type => 'lvm', vgname => 'vg2', content => { images => 1 } },
        thin => {
            type => 'lvmthin',
            vgname => 'vg1',
            thinpool => 'pool',
            content => { images => 1 },
        },
        local => { type => 'dir', path => $dir, content => { iso => 1 } },
    },
};
my $thin_cfg = { ids => { thin => $cfg->{ids}->{thin}, local => $cfg->{ids}->{local} } };
my $vgs = ['vg1:1024:512:1', 'vg2:2048:1024:0'];
my $lvs = ['vg1:pool:1048576:twi---tz--::10.00:5.00::uuid::4096:0'];
my $script;
my $real_run = \&PVE::Storage::LVMPlugin::run_command;
$lvm->mock(
    run_command => sub {
        my ($cmd, %param) = @_;
        die "status command without a timeout\n" if !$param{timeout};
        my $name = $cmd->[0] =~ s|.*/||r;
        $name .= ' --nohints' if grep { $_ eq '--nohints' } @$cmd;
        open(my $log, '>>', "$dir/calls") or die $!;
        print {$log} "$name\n";
        close($log);
        my $action = $script->{$name} // [];
        my $replacement = ref($action) eq 'HASH' ? $action->{cmd} : [
            $^X, '-e', 'print "$_\n" for @ARGV', @$action,
        ];
        # only the slow command gets a short limit, the fast ones before it keep a wide margin
        $param{timeout} = $action->{timeout} if ref($action) eq 'HASH' && $action->{timeout};
        return $real_run->($replacement, %param);
    },
);
my $thin = Test::MockModule->new('PVE::Storage::LvmThinPlugin');
$thin->mock(
    run_command => sub {
        my ($cmd, %param) = @_;
        die "thin pool activation with a timeout\n" if $param{timeout};
        open(my $log, '>>', "$dir/calls") or die $!;
        print {$log} "$cmd->[0]\n";
        close($log);
        return 0;
    },
);

my sub calls {
    return [split(/\n/, PVE::Tools::file_get_contents("$dir/calls"))];
}

my sub pass {
    my ($commands, $config) = @_;
    $script = $commands;
    PVE::Tools::file_set_contents("$dir/calls", '');
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $info = PVE::Storage::storage_info($config // $cfg);
    return ($info, \@warnings);
}

my ($info, $warnings) = pass({ vgs => $vgs, lvs => $lvs });
is_deeply(
    [map { $info->{$_}->{active} } qw(lvm1 lvm2 thin local)],
    [1, 1, 1, 1],
    'normal status activates an inactive thin pool and reports all storages',
);
is_deeply([sort @{ calls() }], [qw(lvchange lvs vgs)], 'listings are cached across storages');
is_deeply($warnings, [], 'normal status has no warnings');
is($info->{lvm1}->{total}, 1024, 'VG sizes cross the fork');
is($info->{thin}->{used}, 104857, 'LV usage crosses the fork');

($info, $warnings) = pass({
    vgs => { cmd => [$^X, '-e', 'print "$_\n" for @ARGV; exit 5', @$vgs] },
    lvs => $lvs,
});
is_deeply(
    [map { $info->{$_}->{active} } qw(lvm1 lvm2 thin)],
    [1, 1, 1],
    'ordinary vgs failure keeps the partial listing and permits later commands',
);
like(join('', @$warnings), qr/exit code 5/, 'ordinary vgs failure still warns');

($info, $warnings) = pass({
    vgs => [],
    'vgs --nohints' => $vgs,
    lvs => $lvs,
});
is($info->{thin}->{active}, 1, 'a VG missing from hints is found and activated');
is_deeply(
    [sort @{ calls() }],
    ['lvchange', 'lvs', 'pvscan', 'vgs', 'vgs --nohints'],
    'hint recovery runs bounded vgs and pvscan',
);

($info, $warnings) = pass({
    vgs => [],
    'vgs --nohints' => $vgs,
    lvs => $lvs,
    pvscan => { cmd => [$^X, '-e', 'exit 5'] },
});
is($info->{thin}->{active}, 1, 'ordinary pvscan failure keeps the discovered VG');
like(join('', @$warnings), qr/rebuilding the LVM device hints failed/, 'pvscan failure warns');

($info, $warnings) =
    pass({ vgs => { cmd => [$^X, '-e', 'kill("KILL", getppid())'] }, lvs => $lvs });
is($info->{lvm1}->{active}, 0, 'a child that dies without a result is no empty listing');
like(join('', @$warnings), qr/no result from/, 'the missing result is reported');

{
    local $SIG{TERM} = sub { PVE::Tools::file_set_contents("$dir/handler", $$) };
    ($info, $warnings) =
        pass({ vgs => { cmd => [$^X, '-e', 'kill("TERM", getppid())'] }, lvs => $lvs });
    ok(!-e "$dir/handler", 'the forked child does not run the signal handlers of the caller');
    like(join('', @$warnings), qr/no result from/, 'the terminated child is reported');
}

{
    my $slow = { cmd => [$^X, '-e', 'sleep 60'], timeout => 1 };
    for my $command ('vgs', 'vgs --nohints', 'pvscan', 'lvs') {
        my $commands = { vgs => $vgs, lvs => $lvs, $command => $slow };
        if ($command eq 'vgs --nohints' || $command eq 'pvscan') {
            $commands->{vgs} = [];
            $commands->{'vgs --nohints'} //= $vgs;
        }
        ($info, $warnings) = pass($commands, $thin_cfg);
        is($info->{thin}->{active}, 0, "$command timeout makes the storage inactive");
        is($info->{local}->{active}, 1, "$command timeout does not affect other plugins");
        like(join('', @$warnings), qr/(?:got timeout|timed out)/, "$command timeout is reported");
        is(calls()->[-1], $command, "no command follows the $command timeout");
    }

    ($info, $warnings) = pass({ vgs => $slow, lvs => $lvs });
    is_deeply(
        [map { $info->{$_}->{active} } qw(lvm1 lvm2 thin)],
        [0, 0, 0],
        'a timed out first listing makes all LVM storages inactive',
    );
    is_deeply(calls(), ['vgs'], 'the failed listing is not retried within the pass');

    local $PVE::Storage::LVMPlugin::LVM_STATUS_TIMEOUT = 2;
    sysopen(my $lock, "$dir/lock", O_RDWR | O_CREAT, 0600) or die $!;
    flock($lock, LOCK_EX) or die $!;
    for (1 .. 2) {
        ($info, $warnings) = pass({ vgs => $vgs, lvs => $lvs });
        is_deeply(
            [map { $info->{$_}->{active} } qw(lvm1 lvm2 thin local)],
            [0, 0, 0, 1],
            'a held lock makes only LVM storages inactive',
        );
        like(
            join('', @$warnings),
            qr/(?:another LVM status command is still running|timed out|got timeout)/,
            'lock contention is reported',
        );
        is_deeply(calls(), [], 'a pass waiting for the lock starts no commands');
    }
    close($lock);
}

($info) = pass({ vgs => $vgs, lvs => $lvs });
is($info->{thin}->{active}, 1, 'a new pass recovers after the lock is released');

# Adopt the simulated stuck command for deterministic cleanup, as init does for a daemon.
syscall(PVE::Syscall::prctl(), 36, 1, 0, 0, 0) == 0
    or die "cannot become a child subreaper: $!";
{
    local $ignore_command_kill = 1;
    my $cache = { status_only => 1 };
    $script = {
        vgs => {
            cmd => [
                $^X,
                '-e',
                'open(my $fh, ">", $ARGV[0]) or die $!; print {$fh} "$$\n"; close($fh); sleep 60',
                "$dir/pid",
            ],
        },
    };
    eval { PVE::Storage::LVMPlugin::lvm_vgs(undef, undef, $cache) };
    like($@, qr/timed out/, 'the outer timeout returns even when the command cannot be killed');
    my $pid = PVE::Tools::file_read_firstline("$dir/pid");
    ok($pid && kill(0, $pid), 'the command outlives the Perl child');
    sysopen(my $lock, "$dir/lock", O_RDWR) or die $!;
    ok(!flock($lock, LOCK_EX | LOCK_NB), 'the execed command retains the lock');
    is(readlink("/proc/$pid/fd/0"), abs_path("$dir/lock"), 'stdin holds the lock');
    local $PVE::Storage::LVMPlugin::LVM_STATUS_TIMEOUT = 2;

    for (1 .. 2) {
        ($info, $warnings) = pass({ vgs => $vgs, lvs => $lvs });
        is($info->{lvm1}->{active}, 0, 'later passes report inactive while the command survives');
        is_deeply(calls(), [], 'later passes start no additional command');
    }
    CORE::kill('KILL', $pid);
    is(waitpid($pid, 0), $pid, 'the orphan was adopted and is reaped by the test');
    ok(flock($lock, LOCK_EX | LOCK_NB), 'the lock is released only after the command exits');
}

my %plain;
$lvm->mock(run_command => sub { my ($cmd, %param) = @_; %plain = %param; return 0 });
PVE::Storage::LVMPlugin::lvm_vgs(undef, undef, {});
ok(!exists($plain{timeout}) && !exists($plain{input}), 'non-status callers keep the plain runner');

done_testing();
