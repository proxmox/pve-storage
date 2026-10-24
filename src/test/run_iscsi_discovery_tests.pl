#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::API2::Storage::Scan;
use PVE::Storage::ISCSIPlugin;

plan skip_all => 'open-iscsi executable required for support check' if !-x '/usr/bin/iscsiadm';

my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
my $portals = ['192.0.2.1', '192.0.2.2'];
my $no_targets = 'iscsiadm: No portals found';
my (%reachable, %errors, %output, @commands, @probes);
$plugin->mock(
    iscsi_test_portal => sub {
        push @probes, $_[1];
        return $reachable{ $_[1] };
    },
    iscsi_portals => sub { $portals },
    iscsi_session => sub { undef },
    run_command => sub {
        my ($cmd, %options) = @_;
        push @commands, $cmd;
        if ($cmd->[2] eq 'discovery') {
            my $portal = $cmd->[-1];
            $options{outfunc}->($_) for ($output{$portal} // [])->@*;
            # fail through the real helper to get the error format of a failing iscsiadm
            if (my $error = $errors{$portal}) {
                PVE::Tools::run_command(
                    ['sh', '-c', 'echo "$1" >&2; exit 1', 'sh', $error],
                    errmsg => $options{errmsg},
                );
            }
        } else {
            is($cmd->[2], 'node', 'only node update and login commands allowed');
        }
    },
);

my sub reset_case {
    %reachable = ();
    %errors = ();
    %output = ();
    @commands = ();
    @probes = ();
}

for my $case (
    ['unreachable', sub { }, "no portal of the target answered: 192.0.2.1\n"],
    [
        'failed discovery',
        sub { $reachable{'192.0.2.1'} = 1; $errors{'192.0.2.1'} = 'iscsiadm: login failed'; },
        "discovery failed on every portal - 192.0.2.1: iscsiadm: login failed\n",
    ],
    [
        'no targets',
        sub { $reachable{'192.0.2.1'} = 1; $errors{'192.0.2.1'} = $no_targets; },
        '',
    ],
) {
    subtest $case->[0] => sub {
        reset_case();
        $case->[1]->();
        my $result = eval {
            PVE::API2::Storage::Scan->iscsiscan({ node => 'test', portal => $portals->[0] });
        };
        is($@, $case->[2], 'scan API distinguishes failed and empty discovery');
        is_deeply($result, [], 'scan of a portal without targets returns an empty list')
            if !$case->[2];
        is(scalar(@commands), 0, 'unreachable portal runs no command') if !%reachable;

        for my $activate (0, 1) {
            @commands = ();
            my $warnings = '';
            local $SIG{__WARN__} = sub { $warnings .= $_[0] };
            eval {
                if ($activate) {
                    PVE::Storage::ISCSIPlugin->activate_storage(
                        'san', { target => 'iqn.test', portal => $portals->[0] }, {},
                    );
                } else {
                    PVE::Storage::ISCSIPlugin::iscsi_login('iqn.test', $portals, {});
                }
            };
            is(
                $@,
                '',
                $activate
                ? 'storage activation tolerates discovery failure'
                : 'login continues',
            );
            is($commands[-1]->[-1], '--login', 'login command still attempted');
            like($warnings, qr/^no portal of the target answered:/, 'unreachable portals warn')
                if !%reachable;
            like($warnings, qr/^discovery failed on every portal -/, 'failed discovery warns')
                if %errors && $case->[2];
            is($warnings, '', 'portal without targets does not warn') if !$case->[2];
        }
    };
}

reset_case();
eval { PVE::Storage::ISCSIPlugin::iscsi_discovery(undef, $portals) };
is($@, "no portal of the target answered: 192.0.2.1, 192.0.2.2\n", 'all portals named');
is_deeply(\@probes, $portals, 'every portal probed');

%reachable = map { $_ => 1 } $portals->@*;
%errors = map { $_ => "failure on $_" } $portals->@*;
$output{ $portals->[0] } = ['192.0.2.1:3260,1 iqn.partial'];
eval { PVE::Storage::ISCSIPlugin::iscsi_discovery(undef, $portals) };
is(
    $@,
    "discovery failed on every portal - 192.0.2.2: failure on 192.0.2.2\n",
    'last failure reported',
);

for my $answering ($portals->@*) {
    %errors = map { $_ => "failure on $_" } grep { $_ ne $answering } $portals->@*;
    $errors{$answering} = $no_targets;
    %output = ();
    my $result = eval { PVE::Storage::ISCSIPlugin::iscsi_discovery(undef, $portals) };
    is($@, '', "no targets on $answering tolerates another failed portal");
    is_deeply($result, {}, 'no targets returned');
}

%errors = ($portals->[0] => $no_targets);
%output = ($portals->[1] => ['192.0.2.2:3260,1 iqn.test']);
is_deeply(
    PVE::Storage::ISCSIPlugin::iscsi_discovery(undef, $portals),
    { 'iqn.test' => ['192.0.2.2:3260'] },
    'portal without targets falls through to the next portal',
);

%errors = ($portals->[0] => 'failure');
%output = (
    $portals->[0] => ['192.0.2.1:3260,1 iqn.partial'],
    $portals->[1] => ['192.0.2.2:3260,1 iqn.test'],
);
is_deeply(
    PVE::Storage::ISCSIPlugin::iscsi_discovery(undef, $portals),
    { 'iqn.test' => ['192.0.2.2:3260'] },
    'successful fallback excludes partial output from failed discovery',
);

done_testing();
