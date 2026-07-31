#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

plan skip_all => 'open-iscsi executable required for support check' if !-x '/usr/bin/iscsiadm';

my (@output, $error);
my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
$plugin->mock(
    run_command => sub {
        my ($cmd, %options) = @_;
        $options{outfunc}->($_) for @output;
        # fail through the real helper to get the error format of a failing iscsiadm
        if ($error) {
            PVE::Tools::run_command(
                ['sh', '-c', 'echo "$1" >&2; exit 1', 'sh', $error],
                errmsg => $options{errmsg},
            );
        }
    },
);

my sub sessions {
    @output = @_;
    return PVE::Storage::ISCSIPlugin::iscsi_session_list();
}

# iscsiadm prints the name the kernel's iSCSI transport registered, and brackets addresses without
# a dot.
for my $transport (qw(tcp iser bnx2i be2iscsi cxgb4i qedi qla4xxx)) {
    is_deeply(
        sessions("$transport: [3] 192.0.2.1:3260,1 iqn.test (non-flash)"),
        { 'iqn.test' => [{ session_id => 3, portal => '192.0.2.1:3260' }] },
        "$transport session listed",
    );
}

is_deeply(
    sessions('qla4xxx: [4] 192.0.2.1:3260,1 iqn.test (flash)'),
    { 'iqn.test' => [{ session_id => 4, portal => '192.0.2.1:3260' }] },
    'flash node session listed',
);

is_deeply(
    sessions('iser: [2] [2001:db8::1]:3260,1 iqn.test (non-flash)'),
    { 'iqn.test' => [{ session_id => 2, portal => '[2001:db8::1]:3260' }] },
    'IPv6 portal keeps its brackets',
);

is_deeply(
    sessions(
        'tcp: [1] 192.0.2.1:3260,1 iqn.a (non-flash)',
        'iser: [2] 192.0.2.2:3260,2 iqn.a (non-flash)',
        'bnx2i: [3] 192.0.2.3:3260,1 iqn.b (non-flash)',
    ),
    {
        'iqn.a' => [
            { session_id => 1, portal => '192.0.2.1:3260' },
            { session_id => 2, portal => '192.0.2.2:3260' },
        ],
        'iqn.b' => [{ session_id => 3, portal => '192.0.2.3:3260' }],
    },
    'sessions of one target are grouped across transports',
);

is_deeply(sessions('unrelated output'), {}, 'other output is not a session');

# iscsiadm reports an empty session list as a failure with this message on stderr
$error = 'iscsiadm: No active sessions.';
is_deeply(sessions(), {}, 'no active sessions results in an empty list');

$error = 'iscsiadm: some failure';
eval { sessions() };
is($@, "iscsi session scan failed: iscsiadm: some failure\n", 'other failures are reported');

done_testing();
