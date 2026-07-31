#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use lib qw(. ..);
use PVE::API2::Storage::Scan;

for my $case (['PVE::API2::Storage::Scan', 'sanlunscan']) {
    my ($class, @names) = @$case;
    for my $name (@names) {
        is_deeply(
            $class->map_method_by_name($name)->{permissions}->{check},
            [
                'or',
                ['perm', '/nodes/{node}', ['Sys.Audit']],
                ['perm', '/storage', ['Datastore.Allocate']],
            ],
            "$class $name read permission",
        );
    }
}

done_testing();
