package PVE::Storage::TestParseVolname;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use Test::More;

my $vmid = 1234;

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
	volname     => "$vmid/vm-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2",
	expected    => [ 'images', "vm-$vmid-disk-0.qcow2", "$vmid", "vm-$vmid-disk-0.qcow2", "$vmid", undef, 'qcow2', ],
    },
    #
    # iso
    #
    {
	description => 'ISO image, iso',
	volname     => 'iso/some-installation-disk.iso',
	expected    => ['iso', 'some-installation-disk.iso'],
    },
    {
	description => 'ISO image, img',
	volname     => 'iso/some-other-installation-disk.img',
	expected    => ['iso', 'some-other-installation-disk.img'],
    },
    #
    # container templates
    #
    {
	description => 'Container template tar.gz',
	volname     => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.gz',
	expected    => ['vztmpl', 'debian-10.0-standard_10.0-1_amd64.tar.gz'],
    },
    {
	description => 'Container template tar.xz',
	volname     => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.xz',
	expected    => ['vztmpl', 'debian-10.0-standard_10.0-1_amd64.tar.xz'],
    },
    {
	description => 'Container template tar.bz2',
	volname     => 'vztmpl/debian-10.0-standard_10.0-1_amd64.tar.bz2',
	expected    => ['vztmpl', 'debian-10.0-standard_10.0-1_amd64.tar.bz2'],
    },
    #
    # container rootdir
    #
    {
	description => 'Container rootdir, sub directory',
	volname     => "rootdir/$vmid",
	expected    => ['rootdir', "$vmid", "$vmid"],
    },
    {
	description => 'Container rootdir, subvol',
	volname     => "$vmid/subvol-$vmid-disk-0.subvol",
	expected    => [ 'images', "subvol-$vmid-disk-0.subvol", "$vmid", undef, undef, undef, 'subvol' ],
    },
    {
	description => 'Backup archive, no virtualization type',
	volname     => "backup/vzdump-none-$vmid-2020_03_30-21_39_30.tar",
	expected    => ['backup', "vzdump-none-$vmid-2020_03_30-21_39_30.tar"],
    },
    #
    # Snippets
    #
    {
	description => 'Snippets, yaml',
	volname     => 'snippets/userconfig.yaml',
	expected    => ['snippets', 'userconfig.yaml'],
    },
    {
	description => 'Snippets, perl',
	volname     => 'snippets/hookscript.pl',
	expected    => ['snippets', 'hookscript.pl'],
    },
    #
    # Import
    #
    {
	description => "Import, ova",
	volname     => 'import/import.ova',
	expected    => ['import', 'import.ova', undef, undef, undef ,undef, 'ova'],
    },
    {
	description => "Import, ovf",
	volname     => 'import/import.ovf',
	expected    => ['import', 'import.ovf', undef, undef, undef ,undef, 'ovf'],
    },
    {
	description => "Import, innner file of ova",
	volname     => 'import/import.ova/disk.qcow2',
	expected    => ['import', 'import.ova/disk.qcow2', undef, undef, undef, undef, 'ova+qcow2'],
    },
    {
	description => "Import, innner file of ova",
	volname     => 'import/import.ova/disk.vmdk',
	expected    => ['import', 'import.ova/disk.vmdk', undef, undef, undef, undef, 'ova+vmdk'],
    },
    {
	description => "Import, innner file of ova with whitespace in name",
	volname     => 'import/import.ova/OS disk.vmdk',
	expected    => ['import', 'import.ova/OS disk.vmdk', undef, undef, undef, undef, 'ova+vmdk'],
    },
    {
	description => "Import, innner file of ova",
	volname     => 'import/import.ova/disk.raw',
	expected    => ['import', 'import.ova/disk.raw', undef, undef, undef, undef, 'ova+raw'],
    },
    #
    # failed matches
    #
    {
	description => "Failed match: VM disk image, base, raw",
	volname     => "ssss/base-$vmid-disk-0.raw",
	expected    => "unable to parse directory volume name 'ssss/base-$vmid-disk-0.raw'\n",
    },
    {
	description => 'Failed match: ISO image, dvd',
	volname     => 'iso/yet-again-a-installation-disk.dvd',
	expected    => "unable to parse directory volume name 'iso/yet-again-a-installation-disk.dvd'\n",
    },
    {
	description => 'Failed match: Container template, zip.gz',
	volname     => 'vztmpl/debian-10.0-standard_10.0-1_amd64.zip.gz',
	expected    => "unable to parse directory volume name 'vztmpl/debian-10.0-standard_10.0-1_amd64.zip.gz'\n",
    },
    {
	description => 'Failed match: Container rootdir, subvol',
	volname     => "rootdir/subvol-$vmid-disk-0",
	expected    => "unable to parse directory volume name 'rootdir/subvol-$vmid-disk-0'\n",
    },
    {
	description => 'Failed match: VM disk image, linked, vhdx',
	volname     => "$vmid/base-$vmid-disk-0.vhdx/$vmid/vm-$vmid-disk-0.vhdx",
	expected    => "unable to parse volume filename 'base-$vmid-disk-0.vhdx'\n",
    },
    {
	description => 'Failed match: VM disk image, linked, qcow2, first vmid',
	volname     => "ssss/base-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2",
	expected    => "unable to parse directory volume name 'ssss/base-$vmid-disk-0.qcow2/$vmid/vm-$vmid-disk-0.qcow2'\n",
    },
    {
	description => 'Failed match: VM disk image, linked, qcow2, second vmid',
	volname     => "$vmid/base-$vmid-disk-0.qcow2/ssss/vm-$vmid-disk-0.qcow2",
	expected    => "unable to parse volume filename 'base-$vmid-disk-0.qcow2/ssss/vm-$vmid-disk-0.qcow2'\n",
    },
    {
	description => "Failed match: import dir but no ova/ovf/disk image",
	volname	    => "import/test.foo",
	expected    => "unable to parse directory volume name 'import/test.foo'\n",
    },
];

# create more test cases for VM disk images matches
my $disk_suffix = [ 'raw', 'qcow2', 'vmdk' ];
foreach my $s (@$disk_suffix) {
    my @arr = (
	{
	    description => "VM disk image, $s",
	    volname     => "$vmid/vm-$vmid-disk-1.$s",
	    expected    => [
		'images',
		"vm-$vmid-disk-1.$s",
		"$vmid",
		undef,
		undef,
		undef,
		"$s",
	    ],
	},
	{
	    description => "VM disk image, linked, $s",
	    volname     => "$vmid/base-$vmid-disk-0.$s/$vmid/vm-$vmid-disk-0.$s",
	    expected    => [
		'images',
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
	    volname     => "$vmid/base-$vmid-disk-0.$s",
	    expected    => [
		'images',
		"base-$vmid-disk-0.$s",
		"$vmid",
		undef,
		undef,
		'base-',
		"$s"
	    ],
	},
    );

    push @$tests, @arr;
}


# create more test cases for backup files matches
my $bkp_suffix = {
    qemu   => [ 'vma', 'vma.gz', 'vma.lzo', 'vma.zst' ],
    lxc    => [ 'tar', 'tgz', 'tar.gz', 'tar.lzo', 'tar.zst', 'tar.bz2' ],
    openvz => [ 'tar', 'tgz', 'tar.gz', 'tar.lzo', 'tar.zst' ],
};

foreach my $virt (keys %$bkp_suffix) {
    my $suffix = $bkp_suffix->{$virt};
    foreach my $s (@$suffix) {
	my @arr = (
	    {
		description => "Backup archive, $virt, $s",
		volname     => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
		expected    => [
		    'backup',
		    "vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
		    "$vmid"
		],
	    },
	);

	push @$tests, @arr;
    }
}


# create more test cases for failed backup files matches
my $non_bkp_suffix = {
    qemu   => [ 'vms.gz', 'vma.xz' ],
    lxc    => [ 'zip.gz', 'tgz.lzo' ],
};
foreach my $virt (keys %$non_bkp_suffix) {
    my $suffix = $non_bkp_suffix->{$virt};
    foreach my $s (@$suffix) {
	my @arr = (
	    {
		description => "Failed match: Backup archive, $virt, $s",
		volname     => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
		expected    => "unable to parse directory volume name 'backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s'\n",
	    },
	);

	push @$tests, @arr;
    }
}


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
    eval { $got = [ PVE::Storage::Plugin->parse_volname($volname) ] };
    $got = $@ if $@;

    is_deeply($got, $expected, $description);

    $seen_vtype->{@$expected[0]} = 1 if ref $expected eq 'ARRAY';
}

# to check if all $vtype_subdirs are defined in path_to_volume_id
# or have a test
is_deeply($seen_vtype, $vtype_subdirs, "vtype_subdir check");

done_testing();

1;
