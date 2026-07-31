#!/usr/bin/perl

use strict;
use warnings;
use Test::MockModule;
use Test::More;
use lib qw(. ..);
use PVE::API2::Storage::Scan;
use PVE::CLI::pvesm;

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

my $formatter = Test::MockModule->new('PVE::CLIFormatter');
my $data = [{ wwid => '3600abc', devpath => '/dev/sda', serial => 'S1' }];
my $schema = PVE::API2::Storage::Scan->map_method_by_name('sanlunscan')->{returns};
$formatter->mock(
    print_api_result => sub {
        is($_[0], $data, 'CLI forwards complete rows');
        is($_[1], $schema, 'CLI forwards complete schema');
        ok(grep({ $_ eq 'state' } $_[2]->@*), 'CLI shows final state');
        like(
            join(' ', $_[2]->@*),
            qr/\bpaths-active paths-total paths-expected\b/,
            'CLI shows the path counts',
        );
        is($_[3]->{'output-format'}, 'json', 'CLI preserves output format');
    },
);
$PVE::CLI::pvesm::cmddef->{scan}->{'san-luns'}->[4]
    ->($data, $schema, { 'output-format' => 'json' });

done_testing();
