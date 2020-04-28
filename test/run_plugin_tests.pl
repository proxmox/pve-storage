#!/usr/bin/perl

use strict;
use warnings;

use TAP::Harness;

my $harness = TAP::Harness->new( { verbosity => -1 });
my $res = $harness->runtests("archive_info_test.pm");

exit -1 if !$res || $res->{failed} || $res->{parse_errors};

