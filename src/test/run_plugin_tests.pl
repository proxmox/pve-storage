#!/usr/bin/perl

use strict;
use warnings;

# to ensure consistent ctime values on all systems
$ENV{TZ} = 'UTC';

use TAP::Harness;

my $harness = TAP::Harness->new({ verbosity => -1 });
my $res = $harness->runtests(
    "archive_info_test.pm",
    "parse_volname_test.pm",
    "list_volumes_test.pm",
    "path_to_volume_id_test.pm",
    "get_subdir_test.pm",
    "filesystem_path_test.pm",
    "prune_backups_test.pm",
);

exit -1 if !$res || $res->{failed} || $res->{parse_errors};
