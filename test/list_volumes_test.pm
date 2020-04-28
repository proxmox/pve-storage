package PVE::Storage::TestListVolumes;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use PVE::Cluster;
use PVE::Tools qw(run_command);

use Test::More;
use Test::MockModule;

use Cwd;
use File::Basename;
use File::Path qw(make_path remove_tree);
use File::stat qw();
use File::Temp;
use Storable qw(dclone);

use constant DEFAULT_SIZE => 131072; # 128 kiB
use constant DEFAULT_USED => 262144; # 256 kiB
use constant DEFAULT_CTIME => 1234567890;

# get_vmlist() return values
my $mocked_vmlist = {
    'version' => 1,
    'ids' => {
	'16110' => {
	    'node'    => 'x42',
	    'type'    => 'qemu',
	    'version' => 4,
	},
	'16112' => {
	    'node'    => 'x42',
	    'type'    => 'lxc',
	    'version' => 7,
	},
	'16114' => {
	    'node'    => 'x42',
	    'type'    => 'qemu',
	    'version' => 2,
	},
	'16113' => {
	    'node'    => 'x42',
	    'type'    => 'qemu',
	    'version' => 5,
	},
	'16115' => {
	    'node'    => 'x42',
	    'type'    => 'qemu',
	    'version' => 1,
	},
	'9004' => {
	    'node'    => 'x42',
	    'type'    => 'qemu',
	    'version' => 6,
	}
    }
};

my $storage_dir = File::Temp->newdir();
my $scfg = {
    'type'     => 'dir',
    'maxfiles' => 0,
    'path'     => $storage_dir,
    'shared'   => 0,
    'content'  => {
	'iso'      => 1,
	'rootdir'  => 1,
	'vztmpl'   => 1,
	'images'   => 1,
	'snippets' => 1,
	'backup'   => 1,
    },
};

# The test cases are comprised of an arry of hashes with the following keys:
# description => displayed on error by Test::More
# vmid        => used for image matches by list_volume
# files       => array of files for qemu-img to create
# expected    => returned result hash
#                (content, ctime, format, parent, size, used, vimd, volid)
my @tests = (
    {
	description => 'VMID: 16110, VM, qcow2, backup, snippets',
	vmid => '16110',
	files => [
	    "$storage_dir/images/16110/vm-16110-disk-0.qcow2",
	    "$storage_dir/images/16110/vm-16110-disk-1.raw",
	    "$storage_dir/images/16110/vm-16110-disk-2.vmdk",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_11_40.vma.gz",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_45.vma.lzo",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_13_55.vma",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_13_55.vma.zst",
	    "$storage_dir/snippets/userconfig.yaml",
	    "$storage_dir/snippets/hookscript.pl",
	],
	expected => [
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16110',
		'volid'   => 'local:16110/vm-16110-disk-0.qcow2',
	    },
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'raw',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16110',
		'volid'   => 'local:16110/vm-16110-disk-1.raw',
	    },
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'vmdk',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16110',
		'volid'   => 'local:16110/vm-16110-disk-2.vmdk',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585595500,
		'format'  => 'vma.gz',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16110',
		'volid'   => 'local:backup/vzdump-qemu-16110-2020_03_30-21_11_40.vma.gz',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585595565,
		'format'  => 'vma.lzo',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16110',
		'volid'   => 'local:backup/vzdump-qemu-16110-2020_03_30-21_12_45.vma.lzo',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585595635,
		'format'  => 'vma',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16110',
		'volid'   => 'local:backup/vzdump-qemu-16110-2020_03_30-21_13_55.vma',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585595635,
		'format'  => 'vma.zst',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16110',
		'volid'   => 'local:backup/vzdump-qemu-16110-2020_03_30-21_13_55.vma.zst',
	    },
	    {
		'content' => 'snippets',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'snippet',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:snippets/hookscript.pl',
	    },
	    {
		'content' => 'snippets',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'snippet',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:snippets/userconfig.yaml',
	    },
	],
    },
    {
	description => 'VMID: 16112, lxc, raw, backup',
	vmid => '16112',
	files => [
	    "$storage_dir/images/16112/vm-16112-disk-0.raw",
	    "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_39_30.tar.lzo",
	    "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_49_30.tar.gz",
	    "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_49_30.tar.zst",
	    "$storage_dir/dump/vzdump-lxc-16112-2020_03_30-21_59_30.tgz",
	],
	expected => [
	    {
		'content' => 'rootdir',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'raw',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16112',
		'volid'   => 'local:16112/vm-16112-disk-0.raw',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585597170,
		'format'  => 'tar.lzo',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16112',
		'volid'   => 'local:backup/vzdump-lxc-16112-2020_03_30-21_39_30.tar.lzo',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585597770,
		'format'  => 'tar.gz',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16112',
		'volid'   => 'local:backup/vzdump-lxc-16112-2020_03_30-21_49_30.tar.gz',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585597770,
		'format'  => 'tar.zst',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16112',
		'volid'   => 'local:backup/vzdump-lxc-16112-2020_03_30-21_49_30.tar.zst',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1585598370,
		'format'  => 'tgz',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '16112',
		'volid'   => 'local:backup/vzdump-lxc-16112-2020_03_30-21_59_30.tgz',
	    },
	],
    },
    {
	description => 'VMID: 16114, VM, qcow2, linked clone',
	vmid => '16114',
	files => [
	    "$storage_dir/images/16114/vm-16114-disk-0.qcow2",
	    "$storage_dir/images/16114/vm-16114-disk-1.qcow2",
	],
	parent => [
	    "../9004/base-9004-disk-0.qcow2",
	    "../9004/base-9004-disk-1.qcow2",
	],
	expected => [
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => '../9004/base-9004-disk-0.qcow2',
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16114',
		'volid'   => 'local:9004/base-9004-disk-0.qcow2/16114/vm-16114-disk-0.qcow2',
	    },
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => '../9004/base-9004-disk-1.qcow2',
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '16114',
		'volid'   => 'local:9004/base-9004-disk-1.qcow2/16114/vm-16114-disk-1.qcow2',
	    },
	],
    },
    {
	description => 'VMID: 9004, VM, template, qcow2',
	vmid => '9004',
	files => [
	    "$storage_dir/images/9004/base-9004-disk-0.qcow2",
	    "$storage_dir/images/9004/base-9004-disk-1.qcow2",
	],
	expected => [
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '9004',
		'volid'   => 'local:9004/base-9004-disk-0.qcow2',
	    },
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => undef,
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '9004',
		'volid'   => 'local:9004/base-9004-disk-1.qcow2',
	    },
	],
    },
    {
	description => 'VMID: none, templates, snippets, backup',
	vmid => undef,
	files => [
	    "$storage_dir/dump/vzdump-lxc-19253-2020_02_03-19_57_43.tar.gz",
	    "$storage_dir/dump/vzdump-lxc-19254-2019_01_21-19_29_19.tar",
	    "$storage_dir/template/iso/archlinux-2020.02.01-x86_64.iso",
	    "$storage_dir/template/iso/debian-8.11.1-amd64-DVD-1.iso",
	    "$storage_dir/template/iso/debian-9.12.0-amd64-netinst.iso",
	    "$storage_dir/template/iso/proxmox-ve_6.1-1.iso",
	    "$storage_dir/template/cache/archlinux-base_20190924-1_amd64.tar.gz",
	    "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.tar.gz",
	    "$storage_dir/template/cache/alpine-3.10-default_20190626_amd64.tar.xz",
	    "$storage_dir/snippets/userconfig.yaml",
	    "$storage_dir/snippets/hookscript.pl",
	    "$storage_dir/private/1234/", # fileparse needs / at the end
	    "$storage_dir/private/1234/subvol-1234-disk-0.subvol/", # fileparse needs / at the end
	],
	expected => [
	    {
		'content' => 'vztmpl',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'txz',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:vztmpl/alpine-3.10-default_20190626_amd64.tar.xz',
	    },
	    {
		'content' => 'vztmpl',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'tgz',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:vztmpl/archlinux-base_20190924-1_amd64.tar.gz',
	    },
	    {
		'content' => 'vztmpl',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'tgz',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:vztmpl/debian-10.0-standard_10.0-1_amd64.tar.gz',
	    },
	    {
		'content' => 'iso',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'iso',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:iso/archlinux-2020.02.01-x86_64.iso',
	    },
	    {
		'content' => 'iso',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'iso',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:iso/debian-8.11.1-amd64-DVD-1.iso',
	    },
	    {
		'content' => 'iso',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'iso',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:iso/debian-9.12.0-amd64-netinst.iso',
	    },
	    {
		'content' => 'iso',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'iso',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:iso/proxmox-ve_6.1-1.iso',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1580756263,
		'format'  => 'tar.gz',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '19253',
		'volid'   => 'local:backup/vzdump-lxc-19253-2020_02_03-19_57_43.tar.gz',
	    },
	    {
		'content' => 'backup',
		'ctime'   => 1548095359,
		'format'  => 'tar',
		'size'    => DEFAULT_SIZE,
		'vmid'    => '19254',
		'volid'   => 'local:backup/vzdump-lxc-19254-2019_01_21-19_29_19.tar',
	    },
	    {
		'content' => 'snippets',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'snippet',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:snippets/hookscript.pl',
	    },
	    {
		'content' => 'snippets',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'snippet',
		'size'    => DEFAULT_SIZE,
		'volid'   => 'local:snippets/userconfig.yaml',
	    },
	],
    },
    {
	description => 'VMID: none, parent, non-matching',
	# string instead of vmid in folder
	#"$storage_dir/images/ssss/base-4321-disk-0.qcow2/1234/vm-1234-disk-0.qcow2",
	vmid => undef,
	files => [
	    "$storage_dir/images/1234/vm-1234-disk-0.qcow2",
	],
	parent => [
	    "../ssss/base-4321-disk-0.qcow2",
	],
	expected => [
	    {
		'content' => 'images',
		'ctime'   => DEFAULT_CTIME,
		'format'  => 'qcow2',
		'parent'  => '../ssss/base-4321-disk-0.qcow2',
		'size'    => DEFAULT_SIZE,
		'used'    => DEFAULT_USED,
		'vmid'    => '1234',
		'volid'   => 'local:1234/vm-1234-disk-0.qcow2',
	    }
	],
    },
    {
	description => 'VMID: none, non-matching',
	# failed matches
	vmid => undef,
	files => [
	    "$storage_dir/images/ssss/base-4321-disk-0.raw",
	    "$storage_dir/images/ssss/vm-1234-disk-0.qcow2",
	    "$storage_dir/template/iso/yet-again-a-installation-disk.dvd",
	    "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.zip.gz",
	    "$storage_dir/template/cache/debian-10.0-standard_10.0-1_amd64.tar.bz2",
	    "$storage_dir/private/subvol-19254-disk-0/19254",
	    "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.tar.bz2",
	    "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.zip.gz",
	    "$storage_dir/dump/vzdump-openvz-16112-2020_03_30-21_39_30.tgz.lzo",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_40.vma.xz",
	    "$storage_dir/dump/vzdump-qemu-16110-2020_03_30-21_12_40.vms.gz",
	],
	expected => [], # returns empty list
    },
);


# provide static vmlist for tests
my $mock_cluster = Test::MockModule->new('PVE::Cluster', no_auto => 1);
$mock_cluster->redefine(get_vmlist => sub { return $mocked_vmlist; });

# populate is File::stat's method to fill all information from CORE::stat into
# an blessed array.
my $mock_stat = Test::MockModule->new('File::stat', no_auto => 1);
$mock_stat->redefine(populate => sub {
	my (@st) = @_;
	$st[7] = DEFAULT_SIZE;
	$st[10] = DEFAULT_CTIME;

	my $result = $mock_stat->original('populate')->(@st);

	return $result;
});

# override info provided by qemu-img in file_size_info
my $mock_fsi = Test::MockModule->new('PVE::Storage::Plugin', no_auto => 1);
$mock_fsi->redefine(file_size_info => sub {
	my ($filename, $timeout) = @_;
	my ($size, $format, $used, $parent, $ctime) = $mock_fsi->original('file_size_info')->($filename, $timeout);

	$size = DEFAULT_SIZE;
	$used = DEFAULT_USED;

	return wantarray ? ($size, $format, $used, $parent, $ctime) : $size;
});

my $plan = scalar @tests;
plan tests => $plan + 1;

{
    # don't accidentally modify vmlist, see bug report
    # https://pve.proxmox.com/pipermail/pve-devel/2020-January/041096.html
    my $scfg_with_type = { path => $storage_dir, type => 'dir' };
    my $original_vmlist = { ids => {} };
    my $tested_vmlist = dclone($original_vmlist);

    PVE::Storage::Plugin->list_volumes('sid', $scfg_with_type, undef, ['images']);

    is_deeply ($tested_vmlist, $original_vmlist,
	'PVE::Cluster::vmlist remains unmodified')
    || diag ("Expected vmlist to remain\n", explain($original_vmlist),
	"but it turned to\n", explain($tested_vmlist));
}


{
    my $sid = 'local';
    my $types = [ 'rootdir', 'images', 'vztmpl', 'iso', 'backup', 'snippets' ];
    my @suffixes = ( 'qcow2', 'raw', 'vmdk', 'vhdx' );

    # run through test cases
    foreach my $tt (@tests) {
	my $vmid = $tt->{vmid};
	my $files = $tt->{files};
	my $expected = $tt->{expected};
	my $description = $tt->{description};
	my $parent = $tt->{parent};

	# prepare environment
	my $num = 0; #parent disks
	for my $file (@$files) {
	    my ($name, $dir, $suffix) = fileparse($file, @suffixes);

	    make_path($dir, { verbose => 1, mode => 0755 });

	    if ($name) {
		# using qemu-img to also be able to represent the backing device
		my @cmd = ( '/usr/bin/qemu-img', 'create', "$file", DEFAULT_SIZE );
		push @cmd, ( '-f', $suffix ) if $suffix;
		push @cmd, ( '-u', '-b', @$parent[$num] ) if $parent;
		$num++;

		run_command([@cmd]);
	    }
	}

	my $got;
	eval { $got = PVE::Storage::Plugin->list_volumes($sid, $scfg, $vmid, $types) };
	$got = $@ if $@;

	is_deeply($got, $expected, $description) || diag(explain($got));

	# clean up after each test case, otherwise
	# we get wrong results from leftover files
	remove_tree($storage_dir, { verbose => 1 });
    }
}

done_testing();

1;
