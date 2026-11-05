package PVE::Storage::TestZfsListZvol;

use strict;
use warnings;

use lib qw(..);

use Test::MockModule;
use Test::More;

use PVE::Storage;
use PVE::Storage::ZFSPlugin;
use PVE::Storage::ZFSPoolPlugin;

# named like a volume on purpose, the pool itself is part of the listing and must not end up in it
my $pool = 'tank/vm-100-pool';

my sub zfs_list_line {
    my ($name, $volsize, $origin, $type, $refquota) = @_;
    return join("\t", $name, $volsize, $origin, $type, $refquota) . "\n";
}

my $zfs_list_output = join(
    '',
    zfs_list_line($pool, '-', '-', 'filesystem', 'none'),
    zfs_list_line("$pool/vm-100-disk-0", 1024, '-', 'volume', '-'),
    zfs_list_line(
        "$pool/vm-100-disk-1",
        2048,
        "$pool/base-100-disk-0\@__base__",
        'volume',
        '-',
    ),
    zfs_list_line("$pool/subvol-101-disk-0", '-', '-', 'filesystem', 4096),
    zfs_list_line("$pool/subvol-101-disk-1", '-', '-', 'filesystem', 'none'),
    zfs_list_line("$pool/vm-102-with:colon", 1024, '-', 'volume', '-'),
    zfs_list_line("$pool/vm-103-disk-0%recv", 1024, '-', 'volume', '-'),
    zfs_list_line("$pool/unrelated", 1024, '-', 'volume', '-'),
    zfs_list_line("tank/other/vm-104-disk-0", 1024, '-', 'volume', '-'),
);

my sub expected_volume {
    my ($name, $vmid, $size, $format, $parent) = @_;
    return (
        $name => {
            name => $name,
            vmid => $vmid,
            size => $size,
            format => $format,
            parent => $parent,
        },
    );
}

my $common_volumes = {
    expected_volume('vm-100-disk-0', 100, 1024, 'raw', undef),
    expected_volume('vm-100-disk-1', 100, 2048, 'raw', 'base-100-disk-0@__base__'),
    expected_volume('subvol-101-disk-0', 101, 4096, 'subvol', undef),
    expected_volume('subvol-101-disk-1', 101, 0, 'subvol', undef),
};

my $tests = [
    {
        plugin => 'PVE::Storage::ZFSPoolPlugin',
        expected => {
            $common_volumes->%*, expected_volume('vm-102-with:colon', 102, 1024, 'raw', undef),
        },
    },
    {
        plugin => 'PVE::Storage::ZFSPlugin',
        expected => $common_volumes,
    },
];

for my $test ($tests->@*) {
    my ($plugin, $expected) = $test->@{qw(plugin expected)};

    my $output;
    my $mock = Test::MockModule->new($plugin);
    $mock->mock(zfs_request => sub { return $output; });

    $output = $zfs_list_output;
    is_deeply($plugin->zfs_list_zvol({ pool => $pool }), $expected, "$plugin - volumes");

    $output = '';
    is_deeply($plugin->zfs_list_zvol({ pool => $pool }), {}, "$plugin - no volumes");
}

done_testing();

1;
