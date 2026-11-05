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

my $device_path;
my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
$plugin->mock(
    filesystem_path => sub { $blockdev },
    run_command => sub {
        my ($cmd, %options) = @_;
        $options{outfunc}->($device_path);
    },
    file_read_firstline => sub {
        my ($file) = @_;
        return $file =~ m!/session1/targetname$! ? 'iqn.test' : 'iqn.other';
    },
);

my $pci = '/devices/pci0000:00/0000:00:03.0/0000:05:00.0';
for my $case (
    ['software iSCSI', '/devices/platform/host3/session1/target3:0:0/3:0:0:1/block/sdb', 1],
    ['iSER or offload', "$pci/host7/session1/target7:0:0/7:0:0:1/block/sdc", 1],
    [
        'session of another target',
        '/devices/platform/host3/session2/target3:0:0/3:0:0:1/block/sdb',
        0,
    ],
    [
        'offload session of another target',
        "$pci/host7/session2/target7:0:0/7:0:0:1/block/sdc",
        0,
    ],
    ['local SATA disk', "$pci/ata1/host0/target0:0:0/0:0:0:0/block/sda", 0],
    ['local NVMe disk', "$pci/nvme/nvme0/nvme0n1", 0],
) {
    my ($name, $path, $accepted) = $case->@*;
    $device_path = $path;

    eval {
        PVE::Storage::ISCSIPlugin->activate_volume(
            'san',
            { target => 'iqn.test' },
            '0.0.0.scsi-test',
        );
    };
    is(
        $@,
        $accepted ? '' : "volume '0.0.0.scsi-test' not part of target 'iqn.test'\n",
        "$name: " . ($accepted ? 'accepted' : 'rejected'),
    );
}

done_testing();
