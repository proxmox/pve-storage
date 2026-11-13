package PVE::Storage::TestFilesystemPath;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use Test::More;

my $path = '/some/path';

{

    package PVE::Storage::TestFilesystemPath::RootdirPlugin;

    use base qw(PVE::Storage::Plugin);

    sub parse_volname {
        my ($class, $volname) = @_;
        return ('rootdir', $volname, 1234, undef, undef, undef, 'subvol');
    }
}

# each array entry is a test that consists of the following keys:
# plugin   => the plugin class to use instead of the base plugin
# volname  => image name that is passed to parse_volname
# snapname => to test the die condition
# expected => the array of return values; or the die message
my $tests = [
    {
        volname => '1234/vm-1234-disk-0.raw',
        snapname => undef,
        expected => [
            "$path/images/1234/vm-1234-disk-0.raw", '1234', 'vm-vol',
        ],
    },
    {
        volname => '1234/vm-1234-disk-0.raw',
        snapname => 'my_snap',
        expected => "can't snapshot this image format\n",
    },
    {
        volname => '1234/vm-1234-disk-0.qcow2',
        snapname => undef,
        expected => [
            "$path/images/1234/vm-1234-disk-0.qcow2", '1234', 'vm-vol',
        ],
    },
    {
        volname => '1234/vm-1234-disk-0.qcow2',
        snapname => 'my_snap',
        expected => [
            "$path/images/1234/vm-1234-disk-0.qcow2", '1234', 'vm-vol',
        ],
    },
    {
        volname => '1234/vm-1234-state-my_snap.raw',
        snapname => undef,
        expected => [
            "$path/images/1234/vm-1234-state-my_snap.raw", '1234', 'vm-state',
        ],
    },
    {
        volname => '1234/subvol-1234-disk-0.subvol',
        snapname => undef,
        expected => [
            "$path/images/1234/subvol-1234-disk-0.subvol", '1234', 'ct-vol',
        ],
    },
    {
        volname => 'iso/my-awesome-proxmox.iso',
        snapname => undef,
        expected => [
            "$path/template/iso/my-awesome-proxmox.iso", undef, 'iso',
        ],
    },
    {
        volname => "backup/vzdump-qemu-1234-2020_03_30-21_12_40.vma",
        snapname => undef,
        expected => [
            "$path/dump/vzdump-qemu-1234-2020_03_30-21_12_40.vma", 1234, 'backup',
        ],
    },
    # an external plugin may return 'rootdir', whose volumes are in 'private' instead of 'images'
    {
        plugin => 'PVE::Storage::TestFilesystemPath::RootdirPlugin',
        volname => 'subvol-1234-disk-0',
        snapname => undef,
        expected => [
            "$path/private/subvol-1234-disk-0", 1234, 'rootdir',
        ],
    },
];

plan tests => scalar @$tests;

foreach my $tt (@$tests) {
    my $volname = $tt->{volname};
    my $snapname = $tt->{snapname};
    my $expected = $tt->{expected};
    my $plugin = $tt->{plugin} // 'PVE::Storage::Plugin';
    my $scfg = { path => $path };
    my $got;

    eval { $got = [$plugin->filesystem_path($scfg, $volname, $snapname)]; };
    $got = $@ if $@;

    is_deeply($got, $expected, "wantarray: filesystem_path for $volname")
        || diag(explain($got));

}

done_testing();

1;
