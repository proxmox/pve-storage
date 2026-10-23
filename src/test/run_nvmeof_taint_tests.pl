#!/usr/bin/perl -T

use v5.36;
use lib qw(..);
use File::Temp qw(tempdir);
use Scalar::Util qw(tainted);
use Test::MockModule;
use Test::More;

use PVE::Tools qw(file_get_contents file_set_contents);

use PVE::NVMeOF;

$ENV{PATH} = '/usr/sbin:/usr/bin:/sbin:/bin';
delete @ENV{qw(IFS CDPATH ENV BASH_ENV)};
my $dir = tempdir('nvmeof-taint-XXXX', DIR => '.', CLEANUP => 1);
local $PVE::NVMeOF::HOSTNQN_FILE = "$dir/hostnqn";
local $PVE::NVMeOF::HOSTID_FILE = "$dir/hostid";
local $PVE::NVMeOF::RUN_DIR = "$dir/run";
file_set_contents($PVE::NVMeOF::HOSTNQN_FILE, "nqn.host\n");
file_set_contents($PVE::NVMeOF::HOSTID_FILE, "12345678-1234-1234-1234-123456789abc\n");
file_set_contents("$dir/config", "nqn.test\n192.0.2.1\n120\n");
my ($nqn, $portal, $fail) = split(/\n/, file_get_contents("$dir/config"));
ok(tainted($nqn), 'configuration input is tainted');
my $nvme = Test::MockModule->new('PVE::NVMeOF');
$nvme->mock(with_lock => sub { $_[0]->() });
my $commands = 0;
$nvme->mock(
    subsystem_controllers => sub {
        return $commands ? { nvme0 => { traddr => '192.0.2.1', trsvcid => 4420 } } : {};
    },
);
$nvme->mock(assert_support => sub { });
$nvme->mock(
    run_command => sub($cmd, %opts) {
        $commands++;
        is(
            scalar(grep { tainted($_) } @$cmd),
            0,
            'all connect operands are validated and untainted',
        );
        PVE::Tools::run_command(['/usr/bin/true', @$cmd], timeout => 5);
        $opts{outfunc}->('{"records":[]}') if $opts{outfunc};
    },
);
my $path = PVE::NVMeOF::parse_portal($portal);
eval { PVE::NVMeOF::connect_path({ subsysnqn => $nqn, 'fast-io-fail-tmo' => $fail }, $path) };
is($@, '', 'connect command can execute in taint mode');
is($commands, 1, 'one connect');
PVE::NVMeOF::note_connect_failure({ subsysnqn => $nqn }, $path);
ok(
    PVE::NVMeOF::in_cooldown({ subsysnqn => $nqn }, $path),
    'cooldown accepts validated file-backed configuration',
);
for my $bad ('nqn.bad,nqn=other', "nqn.test\n", 'nqn.2014-08.org.nvmexpress.discovery') {
    eval { PVE::NVMeOF::connect_path({ subsysnqn => $bad }, $path) };
    like($@, qr/invalid subsystem/, 'invalid connect subsystem refused');
}
my $net = Test::MockModule->new('PVE::Network');
$net->mock(tcp_ping => sub { 1 });
eval { PVE::NVMeOF::discover($portal) };
is($@, '', 'discovery can execute with file-backed host identity in taint mode');
file_set_contents($PVE::NVMeOF::HOSTID_FILE, "00000000-0000-0000-0000-000000000000\n");
eval { PVE::NVMeOF::host_identity() };
like($@, qr/nonzero UUID/, 'zero host ID refused');

done_testing();
