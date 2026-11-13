package PVE::Storage::TestContentTypes;

use strict;
use warnings;

use lib qw(..);

use File::Path qw(make_path);
use File::Temp;
use Test::MockModule;
use Test::More;

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
CFG

my $cfg = PVE::Storage::Plugin->parse_config('storage.cfg', $raw);

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

done_testing();

1;
