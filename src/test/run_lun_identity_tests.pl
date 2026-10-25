#!/usr/bin/perl

use strict;
use warnings;
use lib qw(..);
use Test::More;
use Test::MockModule;
use PVE::Multipath::Identity;

my $mp = Test::MockModule->new('PVE::Multipath');
my $id = Test::MockModule->new('PVE::Multipath::Identity');
my $topology = Test::MockModule->new('PVE::ISCSI::Topology');
$topology->mock(sessions => sub { {} });
$id->mock(
    dir_glob_foreach => sub { $_[2]->($_) for qw(sda sdb sdc) },
    file_read_firstline => sub { '8:0' },
    file_get_contents => sub { "E:ID_SERIAL=3600udev\n" },
);
my $answer = "sda|3600daemon\nsdb|[undef]\n";
$mp->mock(
    query_path_identities => sub {
        return PVE::Multipath::parse_path_identities($answer) if $answer;
        die "unavailable\n";
    },
);
my $ids = PVE::Multipath::Identity::collect(all => 1);
is(
    PVE::Multipath::Identity::path_wwid($ids, 'sda'),
    '3600daemon',
    'daemon identity is authoritative',
);
is(
    PVE::Multipath::Identity::path_wwid($ids, 'sdb'),
    '3600udev',
    'udev identity for a path whose WWID the daemon does not know',
);
is(
    PVE::Multipath::Identity::path_wwid($ids, 'sdc'),
    '3600udev',
    'udev identity for a path the daemon does not list',
);
$answer = undef;
$ids = PVE::Multipath::Identity::collect(all => 1);
is(
    PVE::Multipath::Identity::path_wwid($ids, 'sda'),
    '3600udev',
    'udev fallback when daemon cannot answer',
);
is_deeply(
    PVE::Multipath::parse_path_identities("sda|3600one\nsdb|[undef]\n"),
    { sda => { wwid => '3600one' } },
    'path-list parser skips unknown identities',
);
is_deeply(PVE::Multipath::parse_path_identities('ok'), {}, 'empty daemon reply');
ok(!defined(PVE::Multipath::parse_path_identities('timeout')), 'invalid daemon reply');

done_testing();
