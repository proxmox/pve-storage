#!/usr/bin/perl

use strict;
use warnings;

use FindBin qw($Bin);
use JSON;
use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::LVMPlugin;

my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $disk = Test::MockModule->new('PVE::Diskmanage');
my $tools = Test::MockModule->new('PVE::Tools');
my $inotify = Test::MockModule->new('PVE::INotify');
$inotify->mock(nodename => sub { 'testnode' });
my ($mounted, $held, $map, $running, $size, $signatures, $label, $pv, $fail, $bad_json);
my ($holder_uuid, $pv_alias, $partitioned);
my @writes;
my @probes;
$disk->mock(
    verify_blockdev_path => sub { '/dev/sda' },
    is_mounted => sub { $mounted },
    has_holder => sub { $held },
);
# the sysfs directory of a disk also holds files, which the partition count must not open
$tools->mock(
    dir_glob_foreach => sub {
        my ($dir, $regex, $code) = @_;
        my @entries =
            $dir =~ m{/holders$} ? ('dm-5')
            : $dir =~ m{/slaves$} ? ('sdb')
            : ('uevent', 'size', 'sda1');
        $code->($_) for grep { m/^(?:$regex)$/ } @entries;
    },
);
$lvm->mock(
    file_read_firstline => sub {
        return $size if $_[0] =~ m{/size$};
        die "file '$_[0]' exists but open for reading failed - Not a directory\n"
            if $_[0] =~ m{/(?:uevent|size)/partition$};
        return $partitioned ? '1' : undef if $_[0] =~ m{/partition$};
        return $holder_uuid if $_[0] =~ m{/dm-5/dm/uuid$};
        return $map ? 'mpath-3600abc' : undef if $_[0] =~ m{/uuid$};
        return $running ? 'running' : 'offline' if $_[0] =~ m{/state$};
        die "unexpected read $_[0]";
    },
    clear_first_sector => sub { push @writes, 'clear' },
    # pvs lists a multipath map by its /dev/mapper link
    abs_path => sub { $_[0] eq $pv_alias ? '/dev/sda' : $_[0] },
    run_command => sub {
        my ($cmd, %opt) = @_;
        my $tool = $cmd->[0];
        if ($tool =~ m{(?:file|pvs|wipefs)$}) {
            push @probes, $tool;
            is($opt{timeout}, 10, "$tool has a timeout");
            die "$tool failed or timed out\n" if ($fail // '') eq $tool;
            if ($tool eq 'wipefs') {
                $opt{outfunc}->($bad_json // encode_json({ signatures => $signatures }));
            } elsif ($tool eq '/usr/bin/file') {
                $opt{outfunc}->($label);
            } else {
                is_deeply(
                    [@$cmd[1 .. 6]],
                    [
                        '--nohints',
                        '--readonly',
                        '--devices',
                        '/dev/sda',
                        '--binary',
                        '--separator',
                    ],
                    'PV lookup reads the candidate without hints or activation',
                );
                ok(
                    grep($_ eq 'pv_name,pv_size,vg_name,pv_uuid,pv_in_use', @$cmd),
                    'PV use requested',
                );
                $opt{outfunc}->($pv) if defined($pv);
            }
        } else {
            push @writes, $tool;
        }
    },
);

my sub reset_case {
    ($mounted, $held, $map, $running, $size) = (undef, undef, 0, 1, 1024);
    $partitioned = 0;
    $signatures = [];
    $label = '/dev/sda: data';
    $pv = undef;
    $pv_alias = '/dev/mapper/3600abc';
    $fail = undef;
    $bad_json = undef;
    @writes = ();
    @probes = ();
}

my sub create {
    eval { PVE::Storage::LVMPlugin::lvm_create_volume_group('/dev/disk/by-id/test', 'test', 1) };
    return $@;
}

reset_case();
is(create(), '', 'blank device accepted');
is_deeply(\@writes, ['clear', '/sbin/pvcreate', '/sbin/vgcreate'], 'writes only after probes');

for my $case (
    ['mount', sub { $mounted = '/dev/sda1' }, qr/is mounted/],
    ['unavailable', sub { $size = 0 }, qr/unavailable/],
    ['lost paths', sub { $map = 1; $running = 0 }, qr/no running path/],
) {
    reset_case();
    $case->[1]->();
    like(create(), $case->[2], "$case->[0] refused");
    is_deeply(\@probes, [], "$case->[0]: no device reads");
    is_deeply(\@writes, [], "$case->[0]: no writes");
}

reset_case();
$held = '/dev/sda';
like(create(), qr{another block device uses '/dev/sda'}, 'holder refused');
is_deeply(\@writes, [], 'holder: no writes');

reset_case();
$map = 1;
is(create(), '', 'map with a running slave accepted');

reset_case();
$signatures = [{ type => 'gpt' }, { type => 'ext4' }];
is(
    create(),
    "device '/dev/sda' contains data (ext4, gpt) - wipe it first if it is no longer needed\n",
    'refusal names the signatures',
);
is_deeply(\@writes, [], 'signatures never overwritten');

reset_case();
$signatures = [{ type => 'PMBR' }, { type => 'gpt' }];
is(create(), '', 'an empty partition table of a disk is no data');
is_deeply(
    \@writes,
    ['clear', '/sbin/pvcreate', '/sbin/vgcreate'],
    'empty partition table: initialized',
);
reset_case();
($signatures, $partitioned) = ([{ type => 'PMBR' }, { type => 'gpt' }], 1);
like(create(), qr/contains data \(PMBR, gpt\)/, 'a partition table with partitions is data');
reset_case();
($signatures, $map) = ([{ type => 'gpt' }], 1);
like(
    create(),
    qr/contains data \(gpt\)/,
    'a partition table of a map is data, kpartx may not map it',
);

my $uuid = 'aaaaaa-bbbb-cccc-dddd-eeee-ffff-gggggg';
for my $vg ('test', 'other', 'unknown', '') {
    reset_case();
    $signatures = [{ type => 'LVM2_member' }];
    $label = '/dev/sda: LVM2 PV';
    $pv = "/dev/sda:1024:$vg:$uuid:" . ($vg ? '1' : '0');
    if ($vg && $vg ne 'test') {
        like(create(), qr/already used by volume group/, 'foreign VG refused');
    } else {
        is(create(), '', $vg ? 'same VG accepted' : 'orphan PV accepted');
    }
    is(scalar(@writes), $vg ? 0 : 3, 'only the orphan PV is initialized');
}

for my $vg ('', '[unknown]') {
    for my $used ('1', '', 'unknown', $vg ? ('0') : ()) {
        reset_case();
        $signatures = [{ type => 'LVM2_member' }];
        $pv = "/dev/mapper/3600abc:1024:$vg:$uuid:$used";
        like(
            create(),
            qr/belongs to a volume group whose metadata is not visible/,
            "VG '$vg' with use flag '$used' refused before named-VG handling",
        );
        is_deeply(\@writes, [], 'uncertain orphan never initialized');
    }
}
reset_case();
$signatures = [{ type => 'LVM2_member' }];
$pv = "/dev/sda:1024:[unknown]:$uuid:1";
eval { PVE::Storage::LVMPlugin::lvm_create_volume_group('/dev/sda', '[unknown]', 1) };
like($@, qr/metadata is not visible/, 'sentinel cannot match the requested VG as already created');
is_deeply(\@writes, [], 'sentinel match causes no writes');

subtest 'derived colon-format no-metadata PV and orphan probes' => sub {
    my $fixture = PVE::Tools::file_get_contents(
        "$Bin/lvm-fixtures/lab1-nomda-orphan-derived.colon",
    );
    my @lines = split('\n', $fixture);
    is(scalar(@lines), 2, 'derived fixture contains one probe for each captured PV');
    for my $line (@lines) {
        reset_case();
        $signatures = [{ type => 'LVM2_member' }];
        $label = '/dev/sda: LVM2 PV';
        $pv = $line;
        my @fields = split(':', $line, -1);
        $pv_alias = $fields[0];
        if ($fields[2] eq '[unknown]') {
            like(
                create(),
                qr/belongs to a volume group whose metadata is not visible/,
                'derived no-metadata probe receives the specific refusal',
            );
            is_deeply(\@writes, [], 'derived no-metadata probe causes no writes');
        } else {
            is(create(), '', 'derived genuine orphan probe permits creation');
            is_deeply(
                \@writes,
                ['clear', '/sbin/pvcreate', '/sbin/vgcreate'],
                'only the derived genuine orphan reaches the mocked writes',
            );
        }
    }
};

# kpartx maps the partitions of a map as its holders, which indicates a partition table
reset_case();
($map, $held, $holder_uuid) = (1, '/dev/sda', 'part1-mpath-3600abc');
like(
    create(),
    qr{contains data \(partitions\) - wipe it first if it is no longer needed},
    'partition maps of the map refused as data',
);
is_deeply(\@writes, [], 'partition maps: no writes');
($map, $held, $holder_uuid) = (1, '/dev/sda', 'LVM-otherlv');
like(create(), qr{another block device uses '/dev/sda'}, 'another holder refused as a user');

reset_case();
$signatures = [{ type => 'LVM2_member' }];
$label = '/dev/sda: LVM2 PV';
$pv = "/dev/mapper/3600abc:1024:test:$uuid:1";
is(create(), '', 'same VG on a map listed by its mapper name accepted');

reset_case();
$signatures = [{ type => 'LVM2_member' }];
$label = '/dev/sda: LVM2 PV';
$pv = "/dev/sda:1024:test:$uuid:1";
$held = '/dev/sda';
is(create(), '', 'same VG with active logical volumes counts as created');
is_deeply(\@writes, [], 'existing VG with active logical volumes: no writes');
is(scalar(@writes), 0, 'same VG on a map: no writes');

for my $case (
    ['missing PV', sub { $pv = undef }, qr/no PV found/],
    ['wrong PV', sub { $pv = "/dev/sdb:1024::$uuid:0" }, qr/malformed pvs/],
    ['bad PV size', sub { $pv = "/dev/sda:nan::$uuid:0" }, qr/malformed pvs/],
    ['missing PV use', sub { $pv = "/dev/sda:1024::$uuid" }, qr/malformed pvs/],
    ['invalid PV use', sub { $pv = "/dev/sda:1024::$uuid:maybe" }, qr/malformed pvs/],
    ['sentinel missing use', sub { $pv = "/dev/sda:1024:[unknown]:$uuid" }, qr/malformed pvs/],
    [
        'sentinel invalid use',
        sub { $pv = "/dev/sda:1024:[unknown]:$uuid:maybe" },
        qr/malformed pvs/,
    ],
    ['invalid VG sentinel', sub { $pv = "/dev/sda:1024:[missing]:$uuid:0" }, qr/malformed pvs/],
    ['missing device', sub { $pv = "[unknown]:1024:[unknown]:$uuid:1" }, qr/malformed pvs/],
    [
        'extra signature',
        sub { push @$signatures, { type => 'ext4' } },
        qr/contains data \(ext4\)/,
    ],
    ['bad file', sub { $label = 'garbage' }, qr/malformed file/],
    ['bad JSON', sub { $bad_json = '{}' }, qr/malformed wipefs/],
    ['bad signature', sub { $signatures = [{}] }, qr/malformed wipefs/],
    map {
        my $tool = $_;
        ["failed $tool", sub { $fail = $tool }, qr/failed or timed out/]
    } ('wipefs', '/usr/bin/file', '/sbin/pvs'),
) {
    reset_case();
    $signatures = [{ type => 'LVM2_member' }];
    $pv = "/dev/sda:1024::$uuid:0";
    $case->[1]->();
    like(create(), $case->[2], "$case->[0] refused");
    is_deeply(\@writes, [], "$case->[0]: no writes");
}

done_testing();
