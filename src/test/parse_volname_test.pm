package PVE::Storage::TestParseVolname;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use Test::More;

my $vmid = 1234;

{

    package PVE::Storage::TestParseVolname::OldApiPlugin;

    use base qw(PVE::Storage::Plugin);

    sub api { return 16; }
}

# an array of test cases, each test is comprised of the following keys:
# description => to identify a single test
# volname     => the input for parse_volname
# expected    => the array that parse_volname returns
my $tests = [
    #
    # VM images
    #
    {
        description => 'VM disk image, linked, qcow2, vm- as base-',
        volname => "$vmid/vm-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2",
        expected => [
            'vm-vol',
            "vm-$vmid-disk-0.qcow2",
            "$vmid",
            "vm-$vmid-disk-0.qcow2",
            "$vmid",
            undef,
            'qcow2',
        ],
    },
    #
    # iso
    #
    {
        description => 'ISO image, iso',
        volname => 'iso/some-installation-disk.iso',
        expected => ['iso', 'some-installation-disk.iso', undef, undef, undef, undef, 'raw'],
    },
    {
        description => 'ISO image, img',
        volname => 'iso/some-other-installation-disk.img',
        expected =>
            ['iso', 'some-other-installation-disk.img', undef, undef, undef, undef, 'raw'],
    },
    #
    # container templates
    #
    {
        description => 'Container template tar',
        volname => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar',
        expected => [
            'vztmpl', 'debian-10.0-standard_10.0-1_amd64.tar', undef, undef, undef, undef,
            'raw',
        ],
    },
    {
        description => 'Container template tar.gz',
        volname => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.gz',
        expected => [
            'vztmpl',
            'debian-10.0-standard_10.0-1_amd64.tar.gz',
            undef,
            undef,
            undef,
            undef,
            'raw',
        ],
    },
    {
        description => 'Container template tar.xz',
        volname => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.xz',
        expected => [
            'vztmpl',
            'debian-10.0-standard_10.0-1_amd64.tar.xz',
            undef,
            undef,
            undef,
            undef,
            'raw',
        ],
    },
    {
        description => 'Container template tar.bz2',
        volname => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.bz2',
        expected => [
            'vztmpl',
            'debian-10.0-standard_10.0-1_amd64.tar.bz2',
            undef,
            undef,
            undef,
            undef,
            'raw',
        ],
    },
    #
    # container rootdir
    #
    {
        description => 'Container rootdir, subvol',
        volname => "$vmid/subvol-$vmid-disk-0.subvol",
        expected =>
            ['ct-vol', "subvol-$vmid-disk-0.subvol", "$vmid", undef, undef, undef, 'subvol'],
    },
    #
    # VM state
    #
    {
        description => 'VM state of a snapshot',
        volname => "$vmid/vm-$vmid-state-snap1.raw",
        expected =>
            ['vm-state', "vm-$vmid-state-snap1.raw", "$vmid", undef, undef, undef, 'raw'],
    },
    {
        description => 'VM disk image with state in its name only',
        volname => "$vmid/vm-$vmid-disk-state-1.raw",
        expected =>
            ['vm-vol', "vm-$vmid-disk-state-1.raw", "$vmid", undef, undef, undef, 'raw'],
    },
    {
        description => 'Plugin for an older API version, VM state',
        volname => "$vmid/vm-$vmid-state-snap1.raw",
        plugin => 'PVE::Storage::TestParseVolname::OldApiPlugin',
        expected => ['images', "vm-$vmid-state-snap1.raw", "$vmid", undef, undef, undef, 'raw'],
    },
    {
        description => 'Plugin for an older API version, container subvol',
        volname => "$vmid/subvol-$vmid-disk-0.subvol",
        plugin => 'PVE::Storage::TestParseVolname::OldApiPlugin',
        expected =>
            ['images', "subvol-$vmid-disk-0.subvol", "$vmid", undef, undef, undef, 'subvol'],
    },
    {
        description => 'Backup archive, no virtualization type',
        volname => "backup/vzdump-none-$vmid-2020_03_30-21_39_30.tar",
        expected => [
            'backup',
            "vzdump-none-$vmid-2020_03_30-21_39_30.tar",
            undef,
            undef,
            undef,
            undef,
            'raw',
        ],
    },
    #
    # Snippets
    #
    {
        description => 'Snippets, yaml',
        volname => 'snippets/userconfig.yaml',
        expected => ['snippets', 'userconfig.yaml', undef, undef, undef, undef, 'raw'],
    },
    {
        description => 'Snippets, perl',
        volname => 'snippets/hookscript.pl',
        expected => ['snippets', 'hookscript.pl', undef, undef, undef, undef, 'raw'],
    },
    #
    # Import
    #
    {
        description => "Import, ova",
        volname => 'import/import.ova',
        expected => ['import', 'import.ova', undef, undef, undef, undef, 'ova'],
    },
    {
        description => "Import, ovf",
        volname => 'import/import.ovf',
        expected => ['import', 'import.ovf', undef, undef, undef, undef, 'ovf'],
    },
    {
        description => "Import, innner file of ova",
        volname => 'import/import.ova/disk.qcow2',
        expected =>
            ['import', 'import.ova/disk.qcow2', undef, undef, undef, undef, 'ova+qcow2'],
    },
    {
        description => "Import, innner file of ova",
        volname => 'import/import.ova/disk.vmdk',
        expected => ['import', 'import.ova/disk.vmdk', undef, undef, undef, undef, 'ova+vmdk'],
    },
    {
        description => "Import, innner file of ova with whitespace in name",
        volname => 'import/import.ova/OS disk.vmdk',
        expected =>
            ['import', 'import.ova/OS disk.vmdk', undef, undef, undef, undef, 'ova+vmdk'],
    },
    {
        description => "Import, innner file of ova",
        volname => 'import/import.ova/disk.raw',
        expected => ['import', 'import.ova/disk.raw', undef, undef, undef, undef, 'ova+raw'],
    },
    #
    # failed matches
    #
    {
        description => "Failed match: VM disk image, base, raw",
        volname => "ssss/base-$vmid-disk-0.raw",
        expected => "unable to parse directory volume name 'ssss/base-$vmid-disk-0.raw'\n",
    },
    {
        description => 'Failed match: ISO image, dvd',
        volname => 'iso/yet-again-a-installation-disk.dvd',
        expected =>
            "unable to parse directory volume name 'iso/yet-again-a-installation-disk.dvd'\n",
    },
    {
        description => 'Failed match: Container template, zip.gz',
        volname => 'vztmpl/debian-10.0-standard_10.0-1_amd64.zip.gz',
        expected =>
            "unable to parse directory volume name 'vztmpl/debian-10.0-standard_10.0-1_amd64.zip.gz'\n",
    },
    {
        description => 'Failed match: VM disk image, linked, vhdx',
        volname => "$vmid/base-$vmid-disk-0.vhdx/$vmid/vm-$vmid-disk-0.vhdx",
        expected => "unable to parse volume filename 'base-$vmid-disk-0.vhdx'\n",
    },
    {
        description => 'Failed match: VM disk image, linked, qcow2, first vmid',
        volname => "ssss/base-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2",
        expected =>
            "unable to parse directory volume name 'ssss/base-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2'\n",
    },
    {
        description => 'Failed match: VM disk image, linked, qcow2, second vmid',
        volname => "$vmid/base-$vmid-disk-0.qcow2/ssss/vm-$vmid-disk-0.qcow2",
        expected =>
            "unable to parse volume filename 'base-$vmid-disk-0.qcow2/ssss/vm-$vmid-disk-0.qcow2'\n",
    },
    {
        description => "Failed match: import dir but no ova/ovf/disk image",
        volname => "import/test.foo",
        expected => "unable to parse directory volume name 'import/test.foo'\n",
    },
];

# create more test cases for VM disk images matches
my $disk_suffix = ['raw', 'qcow2', 'vmdk'];
foreach my $s (@$disk_suffix) {
    my @arr = (
        {
            description => "VM disk image, $s",
            volname => "$vmid/vm-$vmid-disk-1.$s",
            expected => [
                'vm-vol', "vm-$vmid-disk-1.$s", "$vmid", undef, undef, undef, "$s",
            ],
        },
        {
            description => "VM disk image, linked, $s",
            volname => "$vmid/base-$vmid-disk-0.$s/$vmid/vm-$vmid-disk-0.$s",
            expected => [
                'vm-vol',
                "vm-$vmid-disk-0.$s",
                "$vmid",
                "base-$vmid-disk-0.$s",
                "$vmid",
                undef,
                "$s",
            ],
        },
        {
            description => "VM disk image, base, $s",
            volname => "$vmid/base-$vmid-disk-0.$s",
            expected => [
                'vm-vol', "base-$vmid-disk-0.$s", "$vmid", undef, undef, 'base-', "$s",
            ],
        },
    );

    push @$tests, @arr;
}

# create more test cases for backup files matches
my $bkp_suffix = {
    qemu => ['vma', 'vma.gz', 'vma.lzo', 'vma.zst'],
    lxc => ['tar', 'tgz', 'tar.gz', 'tar.lzo', 'tar.zst', 'tar.bz2'],
    openvz => ['tar', 'tgz', 'tar.gz', 'tar.lzo', 'tar.zst'],
};

foreach my $virt (keys %$bkp_suffix) {
    my $suffix = $bkp_suffix->{$virt};
    foreach my $s (@$suffix) {
        my @arr = (
            {
                description => "Backup archive, $virt, $s",
                volname => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
                expected => [
                    'backup',
                    "vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
                    "$vmid",
                    undef,
                    undef,
                    undef,
                    'raw',
                ],
            },
        );

        push @$tests, @arr;
    }
}

# create more test cases for failed backup files matches
my $non_bkp_suffix = {
    qemu => ['vms.gz', 'vma.xz'],
    lxc => ['zip.gz', 'tgz.lzo'],
};
foreach my $virt (keys %$non_bkp_suffix) {
    my $suffix = $non_bkp_suffix->{$virt};
    foreach my $s (@$suffix) {
        my @arr = (
            {
                description => "Failed match: Backup archive, $virt, $s",
                volname => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
                expected =>
                    "unable to parse directory volume name 'backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s'\n",
            },
        );

        push @$tests, @arr;
    }
}

my sub map_is_base {
    my ($plugin_name, $is_base) = @_;

    if ($is_base) {
        return 1;
    } else {
        return undef if $plugin_name eq 'LVMPlugin';
        return '';
    }
}

my sub map_error_message {
    my ($plugin_name, $volume_name, $disallowed_char) = @_;

    my $plugin = "PVE::Storage::$plugin_name";
    my $type_in_msg = $plugin->type();
    $type_in_msg = 'zfs' if $type_in_msg eq 'zfspool';
    $type_in_msg = 'lvm' if $type_in_msg eq 'lvmthin';

    if (($disallowed_char || $volume_name =~ m/^-/) && $type_in_msg eq 'lvm') {
        return "lvm name '$volume_name' contains illegal characters\n";
    }

    return "unable to parse $type_in_msg volume name '$volume_name'\n";
}

my sub map_disallowed_chars {
    my ($plugin_name) = @_;

    return [] if $plugin_name eq 'RBDPlugin';

    my $disallowed_chars = [];

    for (my $i = 0; $i < 128; $i++) {
        # Do not simply use PVE::Storage::SAFE_CHAR_CLASS_RE, but hard-code for proper testing
        next if $i == ord('-') || $i == ord('.');
        next if $i >= ord('0') && $i <= ord('9');
        next if $i >= ord('A') && $i <= ord('Z');
        next if $i == ord('_');
        next if $i >= ord('a') && $i <= ord('z');

        # FIXME: MAJOR VERSION: get rid of this exception
        next if $i == ord(':') && $plugin_name eq 'ZFSPoolPlugin';

        push $disallowed_chars->@*, chr($i);
    }

    push $disallowed_chars->@*, 'ä', '此';

    return $disallowed_chars;
}

# common tests for some block-based plugins
for my $plugin_name (qw(LVMPlugin LvmThinPlugin RBDPlugin ZFSPlugin ZFSPoolPlugin)) {
    my $is_base = sub { return map_is_base($plugin_name, $_[0]); };

    push $tests->@*,
        (
            {
                description => "$plugin_name - regular disk name",
                volname => "vm-123-disk-0",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-vol', 'vm-123-disk-0', 123, undef, undef, $is_base->(0), 'raw',
                ],
            },
            {
                description => "$plugin_name - regular disk name with big numbers",
                volname => "vm-999999999-disk-999",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-vol', 'vm-999999999-disk-999', 999999999, undef, undef, $is_base->(0),
                    'raw',
                ],
            },
            {
                description => "$plugin_name - custom disk name",
                volname => "vm-123-something-else.foo",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-vol', 'vm-123-something-else.foo', 123, undef, undef, $is_base->(0), 'raw',
                ],
            },
            {
                description => "$plugin_name - VM state name",
                volname => "vm-123-state-snap1",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-state', 'vm-123-state-snap1', 123, undef, undef, $is_base->(0), 'raw',
                ],
            },
            {
                description => "$plugin_name - invalid prefix",
                volname => "rm-123-disk-0",
                plugin => "PVE::Storage::$plugin_name",
                expected => map_error_message($plugin_name, 'rm-123-disk-0'),
            },
            {
                description => "$plugin_name - missing prefix",
                volname => "-123-disk-0",
                plugin => "PVE::Storage::$plugin_name",
                expected => map_error_message($plugin_name, '-123-disk-0'),
            },
            {
                description => "$plugin_name - trailing newline",
                volname => "vm-123-disk-0\n",
                plugin => "PVE::Storage::$plugin_name",
                expected => map_error_message($plugin_name, "vm-123-disk-0\n", 1),
            },
        );

    # Test disallowed chars
    my $disallowed_chars = map_disallowed_chars($plugin_name) // [];
    for my $chr ($disallowed_chars->@*) {
        push $tests->@*,
            (
                {
                    description => "$plugin_name - $chr character",
                    volname => "vm-123-invalid${chr}char",
                    plugin => "PVE::Storage::$plugin_name",
                    expected => map_error_message($plugin_name, "vm-123-invalid${chr}char", 1),
                },
            );
    }

    # LVMPlugin does not support creating base images at the moment.
    next if grep { $plugin_name eq $_ } (qw(LVMPlugin));
    push $tests->@*,
        (
            {
                description => "$plugin_name - base disk name",
                volname => "base-123-disk-0",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-vol', 'base-123-disk-0', 123, undef, undef, $is_base->(1), 'raw',
                ],
            },
        );

    # All clones are independent for LvmThin, so no linked clones.
    next if grep { $plugin_name eq $_ } (qw(LvmThinPlugin));
    push $tests->@*,
        (
            {
                description => "$plugin_name - linked clone disk name",
                volname => "base-1000-disk-0/vm-112-disk-0",
                plugin => "PVE::Storage::$plugin_name",
                expected => [
                    'vm-vol', 'vm-112-disk-0', 112, 'base-1000-disk-0', 1000, $is_base->(0), 'raw',
                ],
            },
        );
}

# container subvolumes
push $tests->@*,
    (
        {
            description => "ZFSPoolPlugin - subvol name",
            volname => "subvol-123-disk-0",
            plugin => "PVE::Storage::ZFSPoolPlugin",
            expected => ['ct-vol', 'subvol-123-disk-0', 123, undef, undef, '', 'subvol'],
        },
        {
            description => "ZFSPoolPlugin - base subvol name",
            volname => "basevol-123-disk-0",
            plugin => "PVE::Storage::ZFSPoolPlugin",
            expected => ['ct-vol', 'basevol-123-disk-0', 123, undef, undef, 1, 'subvol'],
        },
        {
            description => "ZFSPoolPlugin - linked clone subvol name",
            volname => "basevol-1000-disk-0/subvol-112-disk-0",
            plugin => "PVE::Storage::ZFSPoolPlugin",
            expected =>
            ['ct-vol', 'subvol-112-disk-0', 112, 'basevol-1000-disk-0', 1000, '', 'subvol'],
        },
    );

# volumes that are not named like the guest volumes of the other plugins
push $tests->@*,
    (
        {
            description => "ESXiPlugin - disk in a datacenter named like a VM state volume",
            volname => "vm-100-state-prod/datastore/debian/debian.vmdk",
            plugin => 'PVE::Storage::ESXiPlugin',
            expected => [
                'vm-vol',
                'vm-100-state-prod/datastore/debian/debian.vmdk',
                0,
                undef,
                undef,
                undef,
                'vmdk',
            ],
        },
        {
            description => "ISCSIPlugin - LUN",
            volname => "0.0.0.scsi-36001405b8fdeb9d5d28444482f76d55f",
            plugin => 'PVE::Storage::ISCSIPlugin',
            expected => [
                'vm-vol', 'scsi-36001405b8fdeb9d5d28444482f76d55f', undef, undef, undef, undef,
                'raw',
            ],
        },
        {
            description => "ISCSIDirectPlugin - LUN",
            volname => "lun1",
            plugin => 'PVE::Storage::ISCSIDirectPlugin',
            expected => ['vm-vol', '1', undef, undef, undef, undef, 'raw'],
        },
    );

# tests for PBS backup archives
push $tests->@*,
    (
        {
            description => "PBS backup archive, vm/117",
            volname => "backup/vm/117/2025-08-06T14:27:07Z",
            plugin => 'PVE::Storage::PBSPlugin',
            expected => [
                'backup', 'vm/117/2025-08-06T14:27:07Z', 117, undef, undef, undef, 'pbs-vm',
            ],
        },
        {
            description => "PBS backup archive, ct/123455",
            volname => "backup/ct/123455/2025-08-06T14:27:07Z",
            plugin => 'PVE::Storage::PBSPlugin',
            expected => [
                'backup', 'ct/123455/2025-08-06T14:27:07Z', 123455, undef, undef, undef, 'pbs-ct',
            ],
        },
        {
            description => "PBS backup archive, host/foobar",
            volname => "backup/host/foobar/2025-08-06T14:27:07Z",
            plugin => 'PVE::Storage::PBSPlugin',
            expected => [
                'backup', 'host/foobar/2025-08-06T14:27:07Z', undef, undef, undef, undef,
                'pbs-host',
            ],
        },
        {
            description => "PBS backup archive, host/999",
            volname => "backup/host/999/2025-08-06T14:27:07Z",
            plugin => 'PVE::Storage::PBSPlugin',
            expected => [
                'backup', 'host/999/2025-08-06T14:27:07Z', undef, undef, undef, undef, 'pbs-host',
            ],
        },
        {
            description => "Failed match: PBS backup archive, non-backup content",
            volname => "images/host/999/2025-08-06T14:27:07Z",
            plugin => 'PVE::Storage::PBSPlugin',
            expected => "unable to parse PBS volume name 'images/host/999/2025-08-06T14:27:07Z'\n",
        },
    );

#
# run through test case array
#
plan tests => scalar @$tests + 1;

my $seen_vtype;
my $vtype_subdirs = { map { $_ => 1 } keys %{ PVE::Storage::Plugin::get_vtype_subdirs() } };

foreach my $t (@$tests) {
    my $description = $t->{description};
    my $volname = $t->{volname};
    my $expected = $t->{expected};

    my $got;
    if (my $plugin = $t->{plugin}) {
        eval { $got = [$plugin->parse_volname($volname)] };
    } else {
        eval { $got = [PVE::Storage::Plugin->parse_volname($volname)] };
    }
    $got = $@ if $@;

    is_deeply($got, $expected, $description);

    $seen_vtype->{ @$expected[0] } = 1 if ref $expected eq 'ARRAY';
}

# every directory type needs a test: the volumes of VMs and containers all use the directory of the
# 'images' type, and the in-tree plugins return 'rootdir' for no volume
my $seen_subdir_vtype = {};
for my $vtype (keys $seen_vtype->%*) {
    $vtype = 'images' if PVE::Storage::Common::is_guest_vtype($vtype);
    $seen_subdir_vtype->{$vtype} = 1;
}
delete $vtype_subdirs->{rootdir};
is_deeply($seen_subdir_vtype, $vtype_subdirs, "vtype_subdir check");

done_testing();

1;
