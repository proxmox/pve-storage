#!/usr/bin/perl

use v5.36;
use lib qw(..);
use File::Path qw(make_path remove_tree);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use PVE::API2::NVMeOF;
use PVE::Storage::SAN;

my $sys = tempdir('nvmeof-api-test-XXXX', DIR => '.', CLEANUP => 1);
local $PVE::NVMeOF::SYSFS = $sys;
local $PVE::NVMeOF::SWAPS = "$sys/swaps";
PVE::Tools::file_set_contents($PVE::NVMeOF::SWAPS, "Filename Type Size Used Priority\n");

subtest 'last-path disconnect is checked inside serialization' => sub {
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $disk = Test::MockModule->new('PVE::Diskmanage');
    my $storage = Test::MockModule->new('PVE::Storage');
    my $san = Test::MockModule->new('PVE::Storage::SAN');
    my $cluster = Test::MockModule->new('PVE::Cluster');
    our $locked;
    my ($commands, $checks, $mounted, $busy, $headless) = (0, 0, 0, 0, 0);
    my $own = { device => '/dev/nvme1', state => 'live' };
    my $other = { device => '/dev/nvme2', state => 'live', 'ana-state' => 'optimized' };
    $nvme->mock(with_lock =>
        sub { die "NVMe over Fabrics is busy\n" if $busy; local $locked = 1; $_[0]->() });
    $nvme->mock(
        controllers => sub {
            ok($locked, 'controller snapshot is locked');
            { nvme1 => { subsystem => 'nqn.test' } };
        },
    );
    $nvme->mock(namespaces => sub { $headless ? {} : { nvme0n1 => { wwid => 'eui.abc' } } });
    $nvme->mock(is_supported => sub { 1 });
    my @statuses;
    $disk->mock(
        get_nvme_path_status =>
            sub { shift(@statuses) // { nvme0n1 => { paths => [$own, $other] } } },
    );
    $disk->mock(is_mounted => sub { $mounted });
    $disk->mock(has_holder => sub { 0 });
    my $claimed = 0;
    $nvme->mock(is_claimed => sub { $claimed });
    my ($opener, $opens);
    $disk->mock(device_opener => sub { $opens++; ref($opener) ? shift(@$opener) : $opener });
    my $hires = Test::MockModule->new('Time::HiRes');
    $hires->redefine(usleep => sub { });
    $cluster->mock(cfs_update => sub { ok($locked, 'configuration refreshed under lock') });
    $storage->mock(config => sub { { ids => {} } });
    $san->mock(
        assert_lun_unreferenced => sub { $checks++; die "storage uses this namespace\n" });
    $nvme->mock(
        run_command => sub($cmd, %opts) {
            ok($locked, 'deletion keeps the lock');
            is($opts{timeout}, 30, 'deletion is bounded');
            is_deeply(
                $cmd,
                ['/usr/sbin/nvme', 'disconnect', '--device', '/dev/nvme1'],
                'only the selected controller is removed',
            );
            $commands++;
        },
    );
    for my $ana ('optimized', 'non-optimized') {
        $other->{'ana-state'} = $ana;
        PVE::NVMeOF::disconnect_guarded('nvme1');
    }
    is($commands, 2, 'another usable path permits removal');
    is($checks, 0, 'no usage probe when another path is usable');
    @statuses = (
        { nvme0n1 => { paths => [$own, $other] } },
        { nvme0n1 => { paths => [$own, { %$other, state => 'connecting' }] } },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/paths of the subsystem changed/, 'a path that fails meanwhile refuses');
    @statuses = (
        { nvme0n1 => { paths => [$own, $other] } },
        { nvme0n1 => { paths => [$own, $other] }, nvme0n2 => { paths => [$own] } },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/paths of the subsystem changed/, 'so does a path of another namespace');
    # the controller reconnects while the checks skip the storage references of its namespace
    @statuses = (
        { nvme0n1 => { paths => [{ device => '/dev/nvme1', state => 'connecting' }] } },
        { nvme0n1 => { paths => [{ device => '/dev/nvme1', state => 'live' }] } },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/paths of the subsystem changed/, 'so does the last path that recovers');
    my $calls = 0;
    $nvme->mock(
        namespaces => sub {
            my $res = { nvme0n1 => { wwid => 'eui.abc' } };
            $res->{nvme0n2} = { wwid => 'eui.new' } if $calls++;
            return $res;
        },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/paths of the subsystem changed/, 'and a namespace that appears');
    $calls = 0;
    $nvme->mock(
        namespaces => sub {
            make_path("$sys/class/nvme-fabrics/ctl/nvme1/nvme1n1") if $calls++;
            return { nvme0n1 => { wwid => 'eui.abc' } };
        },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/paths of the subsystem changed/, 'or a device of the controller alone');
    remove_tree("$sys/class/nvme-fabrics/ctl/nvme1/nvme1n1");
    $nvme->mock(namespaces => sub { $headless ? {} : { nvme0n1 => { wwid => 'eui.abc' } } });
    my $replaced = 0;
    $nvme->mock(
        controllers => sub {
            { nvme1 => { subsystem => $replaced++ ? 'nqn.other' : 'nqn.test' } };
        },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/controller changed meanwhile/, 'and a controller replaced meanwhile');
    $replaced = 0;
    $nvme->mock(
        controllers => sub {
            { nvme1 => { subsystem => 'nqn.test', transport => $replaced++ ? 'rdma' : 'tcp' } };
        },
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/controller changed meanwhile/, 'also over another transport');
    $nvme->mock(
        controllers => sub {
            ok($locked, 'controller snapshot is locked');
            { nvme1 => { subsystem => 'nqn.test' } };
        },
    );
    is($commands, 2, 'none of these disconnects');
    for my $ana ('inaccessible', 'persistent-loss', 'change') {
        $other->{'ana-state'} = $ana;
        eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
        like($@, qr/storage uses this namespace/, "$ana does not protect an in-use namespace");
    }
    $other = { device => '/dev/nvme2', state => 'connecting', 'ana-state' => 'optimized' };
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/storage uses this namespace/, 'reconnecting is not a usable path');
    is($commands, 2, 'refusals never delete');
    $own->{state} = 'connecting';
    PVE::NVMeOF::disconnect_guarded('nvme1');
    is($commands, 3, 'a controller that is no usable path either needs no storage check');
    $opener = '4711 (kvm)';
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like(
        $@,
        qr/process 4711 \(kvm\) has namespace/,
        'but a process that has the namespace open refuses',
    );
    $opener = ['4711 (lvs)'];
    PVE::NVMeOF::disconnect_guarded('nvme1');
    is($commands, 4, 'an opener that is gone again shortly does not refuse');
    $opener = undef;
    $own->{state} = 'live';
    $san->mock(assert_lun_unreferenced => sub { $checks++ });
    $mounted = 1;
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/is in use/, 'local mount refuses');
    $mounted = 0;
    PVE::Tools::file_set_contents(
        $PVE::NVMeOF::SWAPS,
        "Filename Type Size Used Priority\n/dev/nvme0n1p2 partition 1024 0 -2\n",
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/is in use/, 'a swap area on the namespace refuses');
    PVE::Tools::file_set_contents($PVE::NVMeOF::SWAPS, "Filename Type Size Used Priority\n");
    $claimed = 1;
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/is in use/, 'a claim, like a mount of a container, refuses');
    $claimed = 0;
    eval { PVE::NVMeOF::disconnect_guarded('nvme1', { subsysnqn => 'nqn.other' }) };
    like($@, qr/no longer connected to the listed subsystem/, 'a replaced controller refuses');
    $opener = '4711 (kvm)';
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/process 4711 \(kvm\) has namespace 'nvme0n1' open/,
        'an open namespace refuses');
    $opener = undef;
    PVE::NVMeOF::disconnect_guarded('nvme1');
    is($commands, 5, 'unused namespace can lose its final path');
    make_path(
        "$sys/class/nvme-fabrics/ctl/nvme1/nvme1n1",
        "$sys/class/nvme-fabrics/ctl/nvme1/nvme0c1n1",
    );
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/do not use native NVMe multipath/, 'a namespace without head device refuses');
    is($commands, 5, 'and nothing is disconnected');
    remove_tree("$sys/class/nvme-fabrics/ctl/nvme1/nvme1n1");
    make_path("$sys/class/nvme-fabrics/ctl/nvme1/nvme3n2");
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/do not use native NVMe multipath/, 'also when named after the subsystem');
    remove_tree("$sys/class/nvme-fabrics/ctl/nvme1/nvme3n2");
    PVE::NVMeOF::disconnect_guarded('nvme1');
    is($commands, 6, 'the path device of native multipath does not');
    $headless = 1;
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/has no device/, 'uncheckable namespace refuses');
    $busy = 1;
    $nvme->mock(controllers => sub { die "must not count paths\n" });
    eval { PVE::NVMeOF::disconnect_guarded('nvme1') };
    like($@, qr/is busy/, 'ongoing deletion blocks another snapshot');
    eval { PVE::NVMeOF::disconnect_guarded('../nvme1') };
    like($@, qr/invalid controller/, 'invalid controller refused before locking');
};

subtest 'node API contract' => sub {
    my $api = 'PVE::API2::NVMeOF';
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $storage = Test::MockModule->new('PVE::Storage');
    $storage->mock(
        config =>
            sub { { ids => { fabric => { type => 'nvmeof', subsysnqn => 'nqn.test' } } } },
    );
    $nvme->mock(is_supported => sub { 0 });
    $nvme->mock(host_identity => sub { { nqn => 'nqn.host', id => 'uuid' } });
    my $status = $api->status({ node => 'n1' });
    is($status->{supported}, 0, 'availability means nvme-cli, not identity or version');
    is($status->{'host-nqn'}, 'nqn.host', 'host identity shown independently');
    $nvme->mock(is_supported => sub { 1 });
    $nvme->mock(host_identity => sub { die "missing host identity\n" });
    $status = $api->status({ node => 'n1' });
    is_deeply(
        $status,
        { supported => 1, error => 'missing host identity' },
        'identity error does not hide installed tools',
    );
    $nvme->mock(
        controllers => sub { {
            nvme1 => {
                subsystem => 'nqn.test',
                transport => 'tcp',
                traddr => '192.0.2.1',
                trsvcid => 4420,
                state => 'connecting',
            },
        } },
    );
    $nvme->mock(
        namespaces => sub { die "must not walk the subsystems for each controller\n" },
        namespace_counts => sub { { 'nqn.test' => 2, 'nqn.other' => 5 } },
    );
    my $rows = $api->controllers({ node => 'n1' });
    is($rows->[0]->{namespaces}, 2, 'namespaces counted for the subsystem of the controller');
    is($rows->[0]->{'used-by-storage'}, 'fabric', 'controller attributed');
    is($rows->[0]->{state}, 'connecting', 'kernel state remains visible');

    for my $pair (['status', $status], ['controllers', $rows]) {
        my $method = $api->map_method_by_name($pair->[0]);
        eval { PVE::JSONSchema::validate($pair->[1], $method->{returns}) };
        is($@, '', "$pair->[0] schema");
        ok($method->{protected} && $method->{proxyto} eq 'node', 'protected and node-proxied');
    }
    is(
        $api->map_method_by_name('disconnect')->{path},
        'controller/{controller}',
        'REST-shaped deletion',
    );
    eval {
        PVE::JSONSchema::validate(
            { node => 'n1', controller => 'nvme1', subsysnqn => 'testsubsys' },
            $api->map_method_by_name('disconnect')->{parameters},
        );
    };
    is($@, '', 'the listed subsystem name is taken as is, also when it is no NQN');
    my $rpc = Test::MockModule->new('PVE::RPCEnvironment');
    {

        package NVMeTestRPC;
        sub get_user { 'root@pam' }
        sub fork_worker { $_[4]->(); return 'UPID:test' }
    }
    $rpc->mock(get => sub { bless {}, 'NVMeTestRPC' });
    my $removed;
    $nvme->mock(disconnect_guarded => sub { $removed = $_[0] });
    is(
        $api->disconnect({ node => 'n1', controller => 'nvme1' }),
        'UPID:test',
        'API returns task identifier',
    );
    is($removed, 'nvme1', 'task calls guarded disconnect');
};

done_testing();
