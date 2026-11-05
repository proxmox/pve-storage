#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

# Activating a volume requires a block device, but the mocks below answer all questions about it.
my ($blockdev) = grep { -b $_ } map { s!^/sys/block/!/dev/!r } glob('/sys/block/*');
plan skip_all => 'block device required for volume activation' if !$blockdev;

my $pci = '/devices/pci0000:00/0000:00:03.0/0000:05:00.0';

# What udev and sysfs report for the virtual devices of the cases below and the disks they use.
my $udev_paths = {
    '/dev/sda' => "$pci/ata1/host0/target0:0:0/0:0:0:0/block/sda",
    '/dev/sdb' => '/devices/platform/host3/session1/target3:0:0/3:0:0:1/block/sdb',
    '/dev/sdc' => '/devices/platform/host4/session2/target4:0:0/4:0:0:1/block/sdc',
    '/dev/dm-3' => '/devices/virtual/block/dm-3',
};
my $dm_uuids = {
    'dm-3' => 'mpath-3600a098038323536',
    'dm-4' => 'part1-mpath-3600a098038323536',
    'dm-5' => '',
    'dm-6' => 'LVM-0hRrw7Rb3cd2WVzQ0ZjOhyAZB9KDGUj5x3wNPtbWZLQfx3m5lhIsm0dw7Js5Gs7v',
    'dm-7' => 'mpath-3600a098038323537',
};
my $slaves = {
    'dm-3' => ['sdb', 'sdc'],
    'dm-4' => ['dm-3'],
    'dm-5' => ['sda', 'sdb'],
    'dm-6' => ['dm-3'],
    'dm-7' => ['sdc'],
    'md0' => ['sda', 'sdb'],
};

my ($device_path, $queried);
my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
$plugin->mock(
    filesystem_path => sub { $blockdev },
    run_command => sub {
        my ($cmd, %options) = @_;
        my $path = $queried++ ? $udev_paths->{ $cmd->[-1] } : $device_path;
        die "no device path for '$cmd->[-1]'\n" if !defined($path);
        $options{outfunc}->($path);
    },
    dir_glob_foreach => sub {
        my ($dir, $regex, $func) = @_;
        my ($name) = $dir =~ m!/([^/]+)/slaves$!;
        $func->($_) for ($slaves->{$name} // [])->@*;
    },
    file_read_firstline => sub {
        my ($file) = @_;
        return $dm_uuids->{$1} if $file =~ m!/([^/]+)/dm/uuid$!;
        return $file =~ m!/session1/targetname$! ? 'iqn.test' : 'iqn.other';
    },
);

my $rejected = "volume '0.0.0.scsi-test' not part of target 'iqn.test'";
my $not_multipath =
    sub { "$rejected - '$_[0]' is neither a multipath map nor a partition of one\n" };

for my $case (
    ['software iSCSI', '/devices/platform/host3/session1/target3:0:0/3:0:0:1/block/sdb', ''],
    ['iSER or offload', "$pci/host7/session1/target7:0:0/7:0:0:1/block/sdc", ''],
    [
        'session of another target',
        '/devices/platform/host3/session2/target3:0:0/3:0:0:1/block/sdb',
        "$rejected\n",
    ],
    [
        'offload session of another target',
        "$pci/host7/session2/target7:0:0/7:0:0:1/block/sdc",
        "$rejected\n",
    ],
    ['local SATA disk', "$pci/ata1/host0/target0:0:0/0:0:0:0/block/sda", "$rejected\n"],
    ['local NVMe disk', "$pci/nvme/nvme0/nvme0n1", "$rejected\n"],
    ['multipath map with paths through two target names', '/devices/virtual/block/dm-3', ''],
    ['partition of a multipath map', '/devices/virtual/block/dm-4', ''],
    ['multipath map of another target', '/devices/virtual/block/dm-7', "$rejected\n"],
    [
        'linear map with a disk of the target',
        '/devices/virtual/block/dm-5',
        $not_multipath->('dm-5'),
    ],
    [
        'logical volume on a multipath map',
        '/devices/virtual/block/dm-6',
        $not_multipath->('dm-6'),
    ],
    [
        'software RAID with a disk of the target',
        '/devices/virtual/block/md0',
        $not_multipath->('md0'),
    ],
) {
    my ($name, $path, $error) = $case->@*;
    $device_path = $path;
    $queried = 0;

    eval {
        PVE::Storage::ISCSIPlugin->activate_volume(
            'san',
            { target => 'iqn.test' },
            '0.0.0.scsi-test',
        );
    };
    is($@, $error, "$name: " . ($error eq '' ? 'accepted' : 'rejected'));
}

done_testing();
