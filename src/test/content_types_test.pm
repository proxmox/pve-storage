package PVE::Storage::TestContentTypes;

use strict;
use warnings;

use lib qw(..);

use File::Path qw(make_path);
use File::Temp;
use Test::MockModule;
use Test::More;

use PVE::API2::Storage::Config;
use PVE::Cluster;
use PVE::Storage;
use PVE::Storage::Common;
use PVE::Storage::Plugin;
use PVE::Tools qw(run_command);

# each test is comprised of the following array keys:
# [0] => content types of a storage
# [1] => content type to check for
# [2] => whether the storage allows it
my $allows_tests = [
    ['images,rootdir', 'images', 1],
    ['images,rootdir', 'vm-vol', 1],
    ['images,rootdir', 'vm-state', 1],
    ['images,rootdir', 'ct-vol', 1],
    ['rootdir', 'images', 0],
    ['rootdir', 'vm-vol', 0],
    ['rootdir', 'ct-vol', 1],
    ['images', 'rootdir', 0],
    ['images', 'ct-vol', 0],
    ['backup,iso', 'images', 0],
    ['backup,iso', 'vm-state', 0],
    ['backup,iso', 'iso', 1],
    ['vm-vol', 'images', 1],
    ['vm-vol', 'vm-vol', 1],
    ['vm-vol', 'vm-state', 0],
    ['vm-vol', 'rootdir', 0],
    ['vm-state,iso', 'images', 1],
    ['vm-state,iso', 'vm-vol', 0],
    ['ct-vol,vm-vol', 'rootdir', 1],
    ['ct-vol,vm-vol', 'vm-state', 0],
];

for my $test ($allows_tests->@*) {
    my ($content, $type, $expected) = $test->@*;

    my $allowed =
        PVE::Storage::Common::content_allows({ map { $_ => 1 } split(',', $content) }, $type);
    is($allowed, $expected, "$content " . ($expected ? 'allows' : 'does not allow') . " $type");
}

# each test is comprised of the following array keys:
# [0] => volume type of a volume
# [1] => volume type that is required
# [2] => whether the volume can be used
my $matches_tests = [
    ['vm-vol', 'vm-vol', 1],
    ['vm-state', 'images', 1],
    ['images', 'vm-state', 1],
    ['rootdir', 'ct-vol', 1],
    ['vm-state', 'vm-vol', 0],
    ['ct-vol', 'images', 0],
    ['images', 'ct-vol', 0],
];

for my $test ($matches_tests->@*) {
    my ($vtype, $required, $expected) = $test->@*;

    my $matches = PVE::Storage::Common::vtype_matches($vtype, $required);
    is($matches, $expected, "$vtype " . ($expected ? 'matches' : 'does not match') . " $required");
}

my $storage_dir = File::Temp->newdir();

my $raw = <<"CFG";
dir: guests
	path $storage_dir
	content images,rootdir

dir: local
	path /var/lib/vz
	content iso,vztmpl,backup

zfspool: cts
	pool tank
	content rootdir

lvm: unset
	vgname unset

iscsi: luns
	portal 192.0.2.1
	target iqn.2026-11.example:target
	content images

lvmthin: disks
	vgname disks
	thinpool data
	content vm-vol

dir: states
	path $storage_dir
	content vm-state,iso

zfspool: mixed
	pool mixed
	content ct-vol,vm-vol

lvm: redundant
	vgname redundant
	content images,vm-state
CFG

my $cfg = PVE::Storage::Plugin->parse_config('storage.cfg', $raw);

# each test is comprised of the following array keys:
# [0] => storage ID
# [1] => the content types of the parsed configuration, which are also those that get written back
my $config_tests = [
    ['guests', 'images,rootdir'],
    ['local', 'backup,iso,vztmpl'],
    ['cts', 'rootdir'],
    ['unset', 'images'],
    ['luns', 'images'],
    ['disks', 'vm-vol'],
    ['states', 'iso,vm-state'],
    ['mixed', 'ct-vol,vm-vol'],
    ['redundant', 'images'],
];

my $written = PVE::Storage::Plugin->write_config('storage.cfg', $cfg);

for my $test ($config_tests->@*) {
    my ($storeid, $content) = $test->@*;

    my $parsed = join(',', sort keys $cfg->{ids}->{$storeid}->{content}->%*);
    is($parsed, $content, "$storeid: parsed content");

    my ($section) = $written =~ m/^\S+: \Q$storeid\E\n((?:\t.*\n)*)/m;
    my ($written_content) = $section =~ m/^\tcontent (.*)$/m;
    is(join(',', sort split(',', $written_content)), $content, "$storeid: written content");
}

# each test is comprised of the following array keys:
# [0] => configured content types
# [1] => whether activating a new storage creates the directory for the volumes of guests
my $activation_tests = [
    ['images', 1], ['vm-vol', 1], ['vm-state,ct-vol', 1], ['iso', 0],
];

for my $test ($activation_tests->@*) {
    my ($content, $expected) = $test->@*;

    my $path = File::Temp->newdir();
    my $scfg = {
        type => 'dir',
        path => "$path",
        content => PVE::Storage::Plugin->decode_value('dir', 'content', $content),
    };
    PVE::Storage::DirPlugin->activate_storage('fresh', $scfg);

    is(-d "$path/images" ? 1 : 0, $expected, "activation with content $content: images directory");
}

# guest 100 is a VM and guest 200 a container with a raw volume, while the container that the
# subvolume of guest 300 belonged to is gone
my $mock_cluster = Test::MockModule->new('PVE::Cluster', no_auto => 1);
$mock_cluster->redefine(
    get_vmlist => sub {
        return { ids => { 100 => { type => 'qemu' }, 200 => { type => 'lxc' } } };
    },
);
my $mock_storage = Test::MockModule->new('PVE::Storage', no_auto => 1);
$mock_storage->redefine(activate_storage => sub { });

for my $file (qw(100/vm-100-disk-0.raw 100/vm-100-state-snap1.raw 200/vm-200-disk-0.raw)) {
    my $path = "$storage_dir/images/$file";
    make_path($path =~ s!/[^/]+$!!r);
    run_command(['/usr/bin/qemu-img', 'create', '-f', 'raw', $path, '1M'], outfunc => sub { });
}
make_path("$storage_dir/images/300/subvol-300-disk-0.subvol");

my $vtypes = { map { $_->{volid} => $_->{vtype} } PVE::Storage::volume_list($cfg, 'guests')->@* };
is_deeply(
    $vtypes,
    {
        'guests:100/vm-100-disk-0.raw' => 'vm-vol',
        'guests:100/vm-100-state-snap1.raw' => 'vm-state',
        'guests:200/vm-200-disk-0.raw' => 'ct-vol',
        'guests:300/subvol-300-disk-0.subvol' => 'ct-vol',
    },
    'guests: volume types of the listed volumes',
);

# each test is comprised of the following array keys:
# [0] => storage ID
# [1] => content type to list the volumes of
# [2] => the listed volumes
my $listing_tests = [
    ['guests', 'vm-vol', ['100/vm-100-disk-0.raw']],
    ['guests', 'vm-state', ['100/vm-100-state-snap1.raw']],
    ['guests', 'images', ['100/vm-100-disk-0.raw', '100/vm-100-state-snap1.raw']],
    ['guests', 'ct-vol', ['200/vm-200-disk-0.raw', '300/subvol-300-disk-0.subvol']],
    ['guests', 'rootdir', ['200/vm-200-disk-0.raw', '300/subvol-300-disk-0.subvol']],
    # plugins list all volumes of VMs under one content type, so the disk is found on the storage
    # for VM states too
    ['states', 'vm-vol', ['100/vm-100-disk-0.raw']],
    ['states', 'vm-state', ['100/vm-100-state-snap1.raw']],
    ['states', 'images', ['100/vm-100-disk-0.raw', '100/vm-100-state-snap1.raw']],
    ['states', 'ct-vol', []],
];

for my $test ($listing_tests->@*) {
    my ($storeid, $type, $expected) = $test->@*;

    my $volids = [map { $_->{volid} } PVE::Storage::volume_list($cfg, $storeid, undef, $type)->@*];
    is_deeply($volids, [map { "$storeid:$_" } $expected->@*], "$storeid: volumes listed for $type");
}

# each test is comprised of the following array keys:
# [0] => storage ID
# [1] => content types to set through the API
# [2] => whether that requires all cluster nodes to know the volume types as content types
my $api_tests = [
    ['local', 'iso,images', 0],
    ['local', 'iso,images,vm-state', 0], # written as 'images'
    ['local', 'iso,vm-vol', 1],
    ['states', 'iso,vm-state', 0], # in use already
    ['states', 'iso,vm-state,vm-vol', 1],
    ['states', 'iso,images', 0],
];

my $cluster_asserted;
$mock_cluster->redefine(assert_min_cluster_version => sub { $cluster_asserted = 1; });
$mock_storage->redefine(
    lock_storage_config => sub { $_[0]->(); },
    config => sub { return PVE::Storage::Plugin->parse_config('storage.cfg', $raw); },
    write_config => sub { },
);

for my $test ($api_tests->@*) {
    my ($storeid, $content, $expected) = $test->@*;

    $cluster_asserted = 0;
    PVE::API2::Storage::Config->update({ storage => $storeid, content => $content });

    is($cluster_asserted, $expected, "$storeid: cluster check for setting content $content");
}

done_testing();

1;
