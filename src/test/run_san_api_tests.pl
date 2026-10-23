#!/usr/bin/perl

use strict;
use warnings;
use Test::MockModule;
use Test::More;
use lib qw(. ..);
use PVE::API2::ISCSI;
use PVE::API2::SAN::Node;
use PVE::API2::Storage::Scan;
use PVE::CLI::pvesm;

my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
my $topology = Test::MockModule->new('PVE::ISCSI::Topology');
my $storage = Test::MockModule->new('PVE::Storage');
$plugin->mock(iscsi_supported => sub { 1 });
$plugin->mock(read_initiator_info => sub { { name => 'iqn.test', alias => 'test' } });
$plugin->mock(iscsi_node_list => sub { [{ target => 'iqn.target', portal => '192.0.2.2:3260' }] });
$storage->mock(config => sub { { ids => { san => { type => 'iscsi', target => 'iqn.target' } } } });
my $state = 'LOGGED_IN';
$topology->mock(
    sessions => sub {
        return {
            7 => {
                target => 'iqn.target',
                portal => '192.0.2.1:3260',
                state => $state,
                devices => { sda => { lun => 1 }, sdb => { lun => 0 } },
            },
        };
    },
);
my $status = PVE::API2::ISCSI->map_method_by_name('status');
is_deeply(
    $status->{code}->({ node => 'test' }),
    { supported => 1, 'initiator-name' => 'iqn.test', 'initiator-alias' => 'test' },
    'initiator identity',
);
my $targets = PVE::API2::ISCSI->map_method_by_name('targets');
for my $case (
    ['LOGGED_IN', 'logged-in'],
    ['FAILED', 'recovering'],
    ['FREE', 'failed'],
    ['', 'unknown'],
) {
    $state = $case->[0];
    my $rows = $targets->{code}->({ node => 'test' });
    is($rows->[0]->{state}, $case->[1], "session state $state");
    is_deeply(
        $rows->[0]->{devices},
        [{ devpath => '/dev/sdb', lun => 0 }, { devpath => '/dev/sda', lun => 1 }],
        'session devices ordered by LUN',
    );
    is($rows->[1]->{state}, 'not-connected', 'node record without a session');
    is($rows->[0]->{'used-by-storage'}, 'san', 'target storage attribution');
    eval { PVE::JSONSchema::validate($rows, $targets->{returns}) };
    is($@, '', 'rows satisfy the schema');
}
for my $case (
    ['PVE::API2::ISCSI', qw(status targets)], ['PVE::API2::SAN::Node', 'luns'],
) {
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
{
    # a user without VM.Audit on the guest still learns that a process uses the LUN
    my $san = Test::MockModule->new('PVE::Storage::SAN');
    my $rpc = Test::MockModule->new('PVE::RPCEnvironment');
    $san->mock(
        scan_luns => sub {
            return [
                { wwid => '3600a', 'used-by-process' => '4242 (kvm)', 'used-by-guest' => 100 },
                { wwid => '3600b', 'used-by-process' => '4243 (kvm)', 'used-by-guest' => 101 },
            ];
        },
    );
    {

        package SANScanTestRPC;
        sub get_user { 'auditor@pve' }
        sub check { $_[2] eq '/vms/100' && $_[3]->[0] eq 'VM.Audit' && $_[4] }
    }
    $rpc->mock(get => sub { bless {}, 'SANScanTestRPC' });
    my $scan = PVE::API2::SAN::Node->map_method_by_name('luns')->{code};
    my %rows = map { $_->{wwid} => $_ } $scan->({ node => 'test' })->@*;
    is($rows{'3600a'}->{'used-by-guest'}, 100, 'a guest that the user may audit is named');
    ok(!exists($rows{'3600b'}->{'used-by-guest'}), 'another one is not');
    is($rows{'3600b'}->{'used-by-process'}, '4243 (kvm)', 'but its process is');
}
is_deeply(
    PVE::API2::ISCSI->map_method_by_name('index')->{code}->({}),
    [{ name => 'status' }, { name => 'target' }],
    'only read-only children',
);
is_deeply(
    [map { $_->{method} } PVE::API2::Storage::Scan->index({ node => 'test' })->@*],
    [qw(cifs iscsi lvm lvmthin nfs nvmeof pbs zfs)],
    'scan index lists the discovery scans',
);
is_deeply(
    PVE::API2::SAN::Node->index({ node => 'test' }),
    [{ name => 'iscsi' }, { name => 'luns' }],
    'node SAN index',
);
for my $case (
    ['/iscsi/status', 'PVE::API2::ISCSI', 'status'],
    ['/luns', 'PVE::API2::SAN::Node', 'luns'],
) {
    my ($path, $class, $name) = @$case;
    my ($handler, $info) = PVE::API2::SAN::Node->find_handler('GET', $path, {});
    is("$handler $info->{name}", "$class $name", "node SAN path $path");
}
{
    my ($class, $name) = $PVE::CLI::pvesm::cmddef->{scan}->{'san-luns'}->@[0, 1];
    is(
        $class->map_method_by_name($name),
        PVE::API2::SAN::Node->map_method_by_name('luns'),
        'CLI calls the LUN inventory',
    );
}

my $formatter = Test::MockModule->new('PVE::CLIFormatter');
my $data = [{ wwid => '3600abc', devpath => '/dev/sda', serial => 'S1' }];
my $schema = PVE::API2::SAN::Node->map_method_by_name('luns')->{returns};
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
