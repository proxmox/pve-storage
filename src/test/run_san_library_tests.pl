#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Cluster;

my %registered;
my $cluster = Test::MockModule->new('PVE::Cluster');
$cluster->mock(cfs_register_file => sub { $registered{ $_[0] } = [@_[1, 2]] });
require PVE::Multipath::Config;
require PVE::Multipath::Generator;

subtest 'CFS callbacks preserve filenames, data and method receivers' => sub {
    my ($parse, $write) = $registered{'multipath.cfg'}->@*;
    my $cfg = $parse->('multipath.cfg', undef);
    is_deeply($cfg->{ids}, {}, 'an absent configuration parses as empty');
    $cfg = $parse->('multipath.cfg', "wwid: 3600abc\n\talias san-lun\n\tnodes n2,n1\n");
    is_deeply($cfg->{ids}->{'3600abc'}->{nodes}, { n1 => 1, n2 => 1 }, 'nodes are decoded');
    my $again = $parse->('multipath.cfg', $write->('multipath.cfg', $cfg));
    is_deeply($again->{ids}, $cfg->{ids}, 'sections survive the registered writer and parser');
    ($parse, $write) = $registered{'multipath-overrides.conf'}->@*;
    is($parse->('multipath-overrides.conf', undef), '', 'absent overrides parse as empty');
    is($write->('multipath-overrides.conf', undef), '', 'undefined overrides write as empty');
    is($write->('multipath-overrides.conf', "devices { }  \n"), "devices { }\n",
        'writer trims');
};

subtest 'omitted optional arguments behave like undef' => sub {
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    $config->mock(read_config => sub { die "configuration unavailable\n" });
    for my $args ([], [undef, undef, undef]) {
        eval { PVE::Multipath::Generator::regenerate(@$args) };
        is($@, "configuration unavailable\n", 'regeneration reads the default configuration');
    }
    $config->mock(cfs_lock_file => sub { $@ = "lock unavailable\n" });
    for my $lock (
        \&PVE::Multipath::Config::lock_config, \&PVE::Multipath::Config::lock_overrides,
    ) {
        for my $args ([], [undef]) {
            eval {
                $lock->(sub { die "unexpected callback\n" }, @$args);
            };
            is($@, "lock unavailable\n", 'an omitted error prefix leaves the error intact');
        }
    }
};

done_testing();
