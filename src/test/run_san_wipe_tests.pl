#!/usr/bin/perl

use strict;
use warnings;

our (@open_modes, $claim_other);

BEGIN {
    *CORE::GLOBAL::sysopen = sub (*$$;$) {
        push @open_modes, $_[2];
        # lets a name mean another device by the time it is opened exclusively
        my $exclusive = $_[2] & Fcntl::O_EXCL();
        my $name = $exclusive && defined($claim_other) ? $claim_other : $_[1];
        return @_ == 4
            ? CORE::sysopen($_[0], $name, $_[2], $_[3])
            : CORE::sysopen($_[0], $name, $_[2]);
    };
}

use lib qw(..);
use Cwd qw(abs_path);
use Fcntl qw(O_RDWR O_EXCL);
use File::Temp qw(tempfile);
use Test::MockModule;
use Test::More;
use PVE::Storage::SAN;

{
    my ($fh, $name) = tempfile(UNLINK => 1);
    my ($tmp, $read_name) = tempfile(UNLINK => 1);
    open(my $read_fh, '<', $read_name) or die "cannot open '$read_name' - $!\n";
    close($tmp);
    my $pid = fork() // die "fork failed - $!\n";
    if (!$pid) {
        sleep(60);
        exit(0);
    }
    like(PVE::Diskmanage::device_opener($name), qr/^$pid \(/, 'a process with the file open');
    my $all = PVE::Diskmanage::device_openers([$name, $read_name, "$name-none"]);
    is_deeply(
        [sort keys %$all],
        [sort map { abs_path($_) } $name, $read_name],
        'each open file has a process, by its resolved path',
    );
    my $writers = PVE::Diskmanage::device_openers([$name, $read_name], writable => 1);
    is_deeply([keys %$writers], [abs_path($name)], 'a file open for reading can be left out');
    kill('TERM', $pid);
    waitpid($pid, 0);
    is(PVE::Diskmanage::device_opener($name), undef, 'the current process does not count');
}

my ($file, $name) = tempfile('san-wipe-XXXX', DIR => '.', UNLINK => 1);
close($file);
my $lun = { devpath => $name, 'by-id' => $name, wwid => '3600a' };
my $san = Test::MockModule->new('PVE::Storage::SAN');
my $disk = Test::MockModule->new('PVE::Diskmanage');
my $tools = Test::MockModule->new('PVE::Tools');
my (@commands, @parts, @parttypes);
my $sectors = 1024 * 1024;
my $run = sub {
    my ($cmd, %opts) = @_;
    push @commands, [$cmd, \%opts];
    if ($cmd->[0] eq 'dd' || ($cmd->[0] eq 'wipefs' && grep { $_ eq '--force' } @$cmd)) {
        my ($path) = map { /^of=(.*)$/ ? $1 : m{^(/proc/\d+/fd/\d+)$} ? $1 : () } @$cmd;
        ok(defined($path) && -e $path, 'whole-device command uses a live descriptor');
        is((stat($path))[1], (stat($name))[1], 'descriptor still names the opened device');
        ok($open_modes[-1] & O_EXCL, 'exclusive descriptor held for whole-device commands');
    }
};
$san->mock(run_command => $run);
my $referenced = 0;
my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(config => sub { { ids => {} } });
my @probes;
$san->mock(assert_lun_unreferenced =>
    sub { push @probes, $_[1]->{devpath}; die "referenced\n" if $referenced });
my $path_used = 0;
$san->mock(assert_lun_paths_unused =>
    sub { push @probes, $_[0]->{devpath}; die "path in use\n" if $path_used });
$san->mock(lun_partitions => sub { push @probes, $_[0]->{devpath}; [@parts] });
$san->mock(dir_glob_foreach => sub { });
my $canonical = abs_path($name);
$disk->mock(verify_blockdev_path => sub { $canonical });
$disk->mock(is_mounted => sub { 0 });
$disk->mock(has_holder => sub { 0 });
my $opener;
$disk->mock(device_opener => sub { $opener });
$disk->mock(run_command => $run);
$disk->mock(dir_glob_foreach => sub { die "partition enumeration after exclusive open\n" });
$disk->mock(is_partition => sub { die "partition check after exclusive open\n" });
$tools->mock(file_get_contents => sub { $sectors });

my $held = 'the LUN is held during the checks, and neither claimed nor written';
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm->mock(assert_running_path => sub { die "multipath map has no running path\n" });
@commands = @open_modes = @probes = ();
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr/no running path/, 'a map without a running path is refused');
is_deeply([\@probes, \@commands], [[], []], 'before a probe could stall on it');
$lvm->unmock('assert_running_path');
$referenced = 1;
@commands = @open_modes = ();
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr/referenced/, 'a referenced LUN is refused');
is_deeply([\@commands, \@open_modes], [[], [O_RDWR]], $held);
$referenced = 0;
$path_used = 1;
@open_modes = ();
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr/path in use/, 'a LUN in use through another path is refused');
is_deeply([\@commands, \@open_modes], [[], [O_RDWR]], $held);
$path_used = 0;
$opener = '4711 (kvm)';
@open_modes = ();
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr/process 4711 \(kvm\) has it open/, 'a LUN that a process has open is refused');
is_deeply([\@commands, \@open_modes], [[], [O_RDWR]], $held);
$opener = undef;

for my $count (0, 2, 10) {
    @parts = map { "/dev/dm-$_" } 1 .. $count;
    @commands = @open_modes = @probes = ();
    PVE::Storage::SAN::wipe_lun($lun);
    is_deeply(
        \@probes,
        [($canonical) x (3 + 2 * $count)],
        'every probe uses the verified kernel path',
    );
    is_deeply(\@open_modes, [O_RDWR, O_RDWR | O_EXCL], 'one exclusive whole-device open');
    my $fdpath = $commands[-2]->[0]->[-1];
    is_deeply(
        $commands[-2]->[0],
        ['wipefs', '--all', '--force', $fdpath],
        'force avoids a second exclusive claim',
    );
    is($commands[-1]->[1]->{timeout}, 60, 'dd has 60 seconds for fdatasync');
    ok(grep({ $_ eq 'count=209715200' } $commands[-1]->[0]->@*), 'wipe capped at 200 MiB');

    for my $i (0 .. $#parts) {
        is_deeply(
            $commands[2 * $i]->[0],
            ['wipefs', '--all', $parts[$i]],
            'partition signatures first',
        );
        is_deeply(
            $commands[2 * $i + 1]->[0],
            ['dmsetup', 'remove', '--retry', $parts[$i]],
            'remove partition map with retry',
        );
    }
    is($_->[1]->{timeout}, 10, 'partition command has a fixed timeout')
        for @commands[0 .. $#commands - 2];
    ok(!-e $fdpath, 'descriptor released on return');
}

@parts = ();
@commands = ();
$sectors = 1;
PVE::Storage::SAN::wipe_lun($lun);
ok(grep({ $_ eq 'count=512' } $commands[-1]->[0]->@*), 'small device uses a byte count');

@parts = ('/dev/dm-1');
@commands = @probes = ();
eval { PVE::Storage::SAN::wipe_lun({ %$lun, devpath => '/dev/null' }) };
like($@, qr{the device behind '/dev/null' changed}, 'a path that means another device refuses');
is_deeply(\@probes, [], 'before the reference check');
is_deeply(\@commands, [], 'before a partition or the device is wiped');
my $listed = 0;
$san->mock(lun_partitions => sub { $listed++ ? [] : ['/dev/dm-1'] });
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr{'/dev/dm-1' is no longer one of its partitions}, 'a partition name that passed on');
is_deeply(\@commands, [], 'is not wiped');
$san->mock(lun_partitions => sub { [@parts] });
@parts = ();
my $err = do {
    local $claim_other = '/dev/zero';
    eval { PVE::Storage::SAN::wipe_lun($lun) };
    $@;
};
like(
    $err,
    qr{the device behind '\Q$name\E' changed},
    'a by-id name that passed on before the claim',
);
is_deeply(\@commands, [], 'is not wiped either');
$canonical = '/dev/null';
eval { PVE::Storage::SAN::wipe_lun($lun) };
like($@, qr{the device behind '/dev/null' changed}, 'a resolved name that no longer means the LUN');
is_deeply(\@commands, [], 'refuses before a write as well');
$canonical = $name;

$disk->mock(dir_glob_foreach => sub { });
$disk->mock(is_partition => sub { 1 });
$disk->mock(change_parttype => sub { push @parttypes, [@_] });
$disk->mock(run_command => sub { my ($cmd, %opts) = @_; push @commands, [$cmd, \%opts] });
@commands = ();
$sectors = 204800;
PVE::Diskmanage::wipe_blockdev('/dev/sda1');
is_deeply(
    [map { $_->[0] } @commands],
    [
        ['wipefs', '--all', '/dev/sda1'],
        ['dd', 'if=/dev/zero', 'of=/dev/sda1', 'bs=1M', 'conv=fdatasync', 'count=100'],
    ],
    'local disk command lines unchanged',
);
ok(!grep({ exists($_->[1]->{timeout}) } @commands), 'local disk timeout behavior unchanged');
is_deeply(\@parttypes, [['/dev/sda1', '8300']], 'local partition type still updated');

my $identity = Test::MockModule->new('PVE::Multipath::Identity');
$identity->mock(
    collect => sub {
        {
            paths => {
                sdb => { wwid => '3600a' },
                sdc => { wwid => '3600b' },
                sdd => { wwid => '3600a' },
            },
        };
    },
);
is_deeply(
    PVE::Storage::SAN::lun_paths($lun),
    ['/dev/sdb', '/dev/sdd'],
    'all paths of the LUN are refreshed after a wipe',
);

# the other paths of a LUN: sdd beside sdb without a map, both below the map dm-3
$san->unmock('assert_lun_paths_unused');
my ($mounted, %holders, $writers, $asked) = (undef, ());
$disk->mock(is_mounted => sub { $_[0] eq '/dev/sdd' ? $mounted : undef });
$san->mock(
    dir_glob_foreach => sub {
        my ($dir, $regex, $code) = @_;
        $code->($_) for ($holders{$dir} // [])->@*;
    },
);
$disk->mock(
    device_openers => sub {
        my ($devices, %param) = @_;
        $asked = [$devices, $param{writable}];
        return $writers // {};
    },
);
$san->mock(abs_path => sub { $_[0] });
my $path_lun = { %$lun, devpath => '/dev/sdb' };
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
is($@, '', 'unused paths pass');
is_deeply($asked, [['/dev/sdd'], 1], 'writers on the other paths, multipathd only reads them');
$mounted = '/dev/sdd1';
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
like($@, qr{the LUN is mounted through '/dev/sdd1'}, 'a mounted path or partition refuses');
$mounted = undef;
$holders{'/sys/class/block/sdd/holders'} = ['dm-3'];
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
like($@, qr{another block device uses the LUN through '/dev/sdd'}, 'a held path refuses');
$holders{'/sys/class/block/sdd/holders'} = [];
$holders{'/sys/class/block/sdd'} = ['sdd1'];
$holders{'/sys/class/block/sdd1/holders'} = ['dm-7'];
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
like($@, qr{uses the LUN through '/dev/sdd1'}, 'so does a held partition of a path');
$holders{'/sys/class/block/sdd1/holders'} = [];
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
is_deeply($asked->[0], ['/dev/sdd', '/dev/sdd1'], 'writers on the partitions of a path count');
$holders{'/sys/class/block/sdb/holders'} = ['dm-3'];
eval { PVE::Storage::SAN::assert_lun_paths_unused({ %$lun, devpath => '/dev/dm-3' }) };
is($@, '', 'the map of the LUN holds its paths');
%holders = ();
$writers = { '/dev/sdd' => { pid => 4711, command => 'kvm' } };
eval { PVE::Storage::SAN::assert_lun_paths_unused($path_lun) };
like($@, qr{process 4711 \(kvm\) has the LUN open through '/dev/sdd'},
    'a writer on a path refuses');

# without multipathd, udev can know a member of the map by another identity than the map
my $map_lun = { %$lun, devpath => '/dev/dm-3' };
$holders{'/sys/class/block/dm-3/slaves'} = ['sdc'];
is_deeply(
    PVE::Storage::SAN::lun_paths($map_lun),
    ['/dev/sdb', '/dev/sdc', '/dev/sdd'],
    'the members of the map are paths of the LUN, whatever their identity',
);
$writers = { '/dev/sdc' => { pid => 4712, command => 'kvm' } };
eval { PVE::Storage::SAN::assert_lun_paths_unused($map_lun) };
like(
    $@,
    qr{process 4712 \(kvm\) has the LUN open through '/dev/sdc'},
    'a writer on such a member refuses',
);

done_testing();
