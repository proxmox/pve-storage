package PVE::Storage::TestFreeImage;

use strict;
use warnings;

use lib qw(..);

use File::Path qw(make_path);
use File::Temp;
use PVE::Storage;
use PVE::Tools qw(file_set_contents);
use Test::More;

my $storage_dir = File::Temp->newdir();
my $scfg = { type => 'dir', path => "$storage_dir" };

# each test is comprised of the following array keys:
# [0] => volname of the only volume in its directory, freed by the test
# [1] => parent directory of the volume, relative to the storage path
# [2] => whether the now empty parent directory is expected to be kept
my $tests = [
    ['100/vm-100-disk-0.raw', 'images/100', 0],
    ['iso/some.iso', 'template/iso', 1],
    ['vztmpl/some.tar.zst', 'template/cache', 1],
    ['snippets/hook.pl', 'snippets', 1],
    ['import/some.ova', 'import', 1],
];

plan tests => 2 * scalar(@$tests);

for my $tt (@$tests) {
    my ($volname, $subdir, $keep_dir) = @$tt;

    my $path = PVE::Storage::DirPlugin->filesystem_path($scfg, $volname);
    make_path("$storage_dir/$subdir");
    file_set_contents($path, '');

    my $format = (PVE::Storage::DirPlugin->parse_volname($volname))[6];
    PVE::Storage::DirPlugin->free_image('local', $scfg, $volname, 0, $format);

    ok(!-e $path, "$volname - volume removed");
    is(
        !!-d "$storage_dir/$subdir",
        !!$keep_dir,
        "$volname - empty directory '$subdir' " . ($keep_dir ? 'kept' : 'removed'),
    );
}

done_testing();

1;
