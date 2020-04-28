#!/usr/bin/perl

use strict;
use warnings;

use TAP::Harness;

my $harness = TAP::Harness->new( { verbosity => -1 });
my $res = $harness->runtests(
    "archive_info_test.pm",
    "parse_volname_test.pm",
    "list_volumes_test.pm",
);

exit -1 if !$res || $res->{failed} || $res->{parse_errors};

