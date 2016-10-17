#!/usr/bin/perl

use strict;
use warnings;

use TAP::Harness;

my $harness = TAP::Harness->new( { verbosity => -2 });
$harness->runtests( "disklist_test.pm" );

