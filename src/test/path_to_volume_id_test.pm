package PVE::Storage::TestPathToVolumeId;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;

use Test::More;

use Cwd;
use File::Basename;
use File::Path qw(make_path remove_tree);
use File::Temp;

my $storage_dir = File::Temp->newdir();
my $scfg = {
    'digest' => 'd29306346b8b25b90a4a96165f1e8f52d1af1eda',
    'ids'    => {
	'local' => {
	    'shared'   => 0,
	    'path'     => "$storage_dir",
	    'type'     => 'dir',
	    'maxfiles' => 0,
	    'content'  => {
		'snippets' => 1,
		'rootdir'  => 1,
		'images'   => 1,
		'iso'      => 1,
		'backup'   => 1,
		'vztmpl'   => 1,
	    },
	},
    },
    'order' => {
	'local' => 1,
    },
};

# the tests array consists of hashes with the following keys:
# description => to identify the test case
# volname     => to create the test file
# expected    => the result that path_to_volume_id should return
my @tests = (
    {
	description => 'Image, qcow2',
	volname     => "$storage_dir/images/16110/vm-16110-disk-0.qcow2",
	expected    => [
	    'images',
	    'local:16110/vm-16110-disk-0.qcow2',
	],
    },
    {
	description => 'Image, raw',
	volname     => "$storage_dir/images/16112/vm-16112-disk-0.raw",
	expected    => [
	    'images',
	    'local:16112/vm-16112-disk-0.raw',
	],
    },
    {
	description => 'Image template, qcow2',
	volname     => "$storage_dir/images/9004/base-9004-disk-0.qcow2",
	expected    => [
	    'images',
	    'local:9004/base-9004-disk-0.qcow2',
	],
    },

    {
	description => 'Backup, vma.gz',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_11_40.vma.gz",
	expected    => [
	    'backup',
	    'local:backup/vzdump-qemu-16110-2020_03_30-21_11_40.vma.gz',
	],
    },
    {
	description => 'Backup, vma.lzo',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_45.vma.lzo",
	expected    => [
	    'backup',
	    'local:backup/vzdump-qemu-16110-2020_03_30-21_12_45.vma.lzo',
	],
    },
    {
	description => 'Backup, vma',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_13_55.vma",
	expected    => [
	    'backup',
	    'local:backup/vzdump-qemu-16110-2020_03_30-21_13_55.vma',
	],
    },
    {
	description => 'Backup, tar.lzo',
	volname     => "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_39_30.tar.lzo",
	expected    => [
	    'backup',
	    'local:backup/vzdump-lxc-16112-2020_03_30-21_39_30.tar.lzo',
	],
    },
    {
	description => 'Backup, vma.zst',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_13_55.vma.zst",
	expected    => [
	    'backup',
	    'local:backup/vzdump-qemu-16110-2020_03_30-21_13_55.vma.zst'
	],
    },
    {
	description => 'Backup, tar.zst',
	volname     => "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_39_30.tar.zst",
	expected    => [
	    'backup',
	    'local:backup/vzdump-lxc-16112-2020_03_30-21_39_30.tar.zst'
	],
    },
    {
	description => 'Backup, tar.bz2',
	volname     => "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.tar.bz2",
	expected    => [
	    'backup',
	    'local:backup/vzdump-openvz-16112-2020_03_30-21_39_30.tar.bz2',
	],
    },

    {
	description => 'ISO file',
	volname     => "$storage_dir/template/iso/yet-again-a-installation-disk.iso",
	expected    => [
	    'iso',
	    'local:iso/yet-again-a-installation-disk.iso',
	],
    },
    {
	description => 'CT template, tar.gz',
	volname     => "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.tar.gz",
	expected    => [
	    'vztmpl',
	    'local:vztmpl/debian-10.0-standard_10.0-1_amd64.tar.gz',
	],
    },
    {
	description => 'CT template, wrong ending, tar bz2',
	volname     => "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.tar.bz2",
	expected    => [
	    'vztmpl',
	    'local:vztmpl/debian-10.0-standard_10.0-1_amd64.tar.bz2',
	],
    },

    {
	description => 'Rootdir',
	volname     => "$storage_dir/private/1234/", # fileparse needs / at the end
	expected    => [
	    'rootdir',
	    'local:rootdir/1234',
	],
    },
    {
	description => 'Rootdir, folder subvol',
	volname     => "$storage_dir/images/1234/subvol-1234-disk-0.subvol/", # fileparse needs / at the end
	expected    => [
	    'images',
	    'local:1234/subvol-1234-disk-0.subvol'
	],
    },
    {
	description => 'Snippets, yaml',
	volname => "$storage_dir/snippets/userconfig.yaml",
	expected => [
	    'snippets',
	    'local:snippets/userconfig.yaml',
	],
    },
    {
	description => 'Snippets, hookscript',
	volname     => "$storage_dir/snippets/hookscript.pl",
	expected    => [
	    'snippets',
	    'local:snippets/hookscript.pl',
	],
    },
    {
	description => 'CT template, tar.xz',
	volname     => "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.tar.xz",
	expected    => [
	    'vztmpl',
	    'local:vztmpl/debian-10.0-standard_10.0-1_amd64.tar.xz',
	],
    },

    # no matches, path or files with failures
    {
	description => 'Base template, string as vmid in folder name',
	volname     => "$storage_dir/images/ssss/base-4321-disk-0.raw",
	expected    => [''],
    },
    {
	description => 'ISO file, wrong ending',
	volname     => "$storage_dir/template/iso/yet-again-a-installation-disk.dvd",
	expected    => [''],
    },
    {
	description => 'CT template, wrong ending, zip.gz',
	volname     => "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.zip.gz",
	expected    => [''],
    },
    {
	description => 'Rootdir as subvol, wrong path',
	volname     => "$storage_dir/private/subvol-19254-disk-0/",
	expected    => [''],
    },
    {
	description => 'Backup, wrong format, openvz, zip.gz',
	volname     => "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.zip.gz",
	expected    => [''],
    },
    {
	description => 'Backup, wrong format, openvz, tgz.lzo',
	volname     => "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.tgz.lzo",
	expected    => [''],
    },
    {
	description => 'Backup, wrong ending, qemu, vma.xz',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_40.vma.xz",
	expected    => [''],
    },
    {
	description => 'Backup, wrong format, qemu, vms.gz',
	volname     => "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_40.vms.gz",
	expected    => [''],
    },
    {
	description => 'Image, string as vmid in folder name',
	volname     => "$storage_dir/images/ssss/vm-1234-disk-0.qcow2",
	expected    => [''],
    },
);

plan tests => scalar @tests + 1;

my $seen_vtype;
my $vtype_subdirs = { map { $_ => 1 } keys %{ PVE::Storage::Plugin::get_vtype_subdirs() } };

foreach my $tt (@tests) {
    my $file = $tt->{volname};
    my $expected = $tt->{expected};
    my $description = $tt->{description};

    # prepare environment
    my ($name, $dir, $suffix) = fileparse($file);
    make_path($dir, { verbose => 1, mode => 0755 });

    if ($name) {
	open(my $fh, ">>", "$file") || die "Error open file: $!";
	close($fh);
    }

    # run tests
    my $got;
    eval { $got = [ PVE::Storage::path_to_volume_id($scfg, $file) ] };
    $got = $@ if $@;

    is_deeply($got, $expected, $description) || diag(explain($got));

    $seen_vtype->{@$expected[0]} = 1
	if ( @$expected[0] ne '' && scalar @$expected > 1);
}

# to check if all $vtype_subdirs are defined in path_to_volume_id
# or have a test
is_deeply($seen_vtype, $vtype_subdirs, "vtype_subdir check");

#cleanup
# File::Temp unlinks tempdir on exit

done_testing();

1;
