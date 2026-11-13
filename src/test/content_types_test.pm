package PVE::Storage::TestContentTypes;

use strict;
use warnings;

use lib qw(..);

use Test::More;

use PVE::Storage::Common;

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

done_testing();

1;
