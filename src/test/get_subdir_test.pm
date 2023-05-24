package PVE::Storage::TestGetSubdir;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage::Plugin;
use Test::More;

my $scfg_with_path = { path => '/some/path' };
my $vtype_subdirs = PVE::Storage::Plugin::get_vtype_subdirs();

# each test is comprised of the following array keys:
# [0] => storage config; positive with path key
# [1] => storage type;  see $vtype_subdirs
# [2] => expected return from get_subdir
my $tests = [
    # failed matches
    [ $scfg_with_path, 'none', "unknown vtype 'none'\n" ],
    [ {}, 'iso', "storage definition has no path\n" ],
];

# creates additional positive tests
foreach my $type (keys %$vtype_subdirs) {
    my $path = "$scfg_with_path->{path}/$vtype_subdirs->{$type}";
    push @$tests, [ $scfg_with_path, $type, $path ];
}

# creates additional tests for overrides
foreach my $type (keys %$vtype_subdirs) {
    my $override = "${type}_override";
    my $scfg_with_override = { path => '/some/path', 'content-dirs' => { $type => $override } };
    push @$tests, [ $scfg_with_override, $type, "$scfg_with_override->{path}/$scfg_with_override->{'content-dirs'}->{$type}" ];
}

plan tests => scalar @$tests;

foreach my $tt (@$tests) {
    my ($scfg, $type, $expected) = @$tt;

    my $got;
    eval { $got = PVE::Storage::Plugin->get_subdir($scfg, $type) };
    $got = $@ if $@;

    is ($got, $expected, "get_subdir for $type") || diag(explain($got));
}

done_testing();

1;
