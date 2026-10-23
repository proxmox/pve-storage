#!/usr/bin/perl

use v5.36;
use lib qw(..);
use Fcntl qw(:flock);
use File::Path qw(make_path remove_tree);
use File::Temp qw(tempdir);
use JSON qw(encode_json);
use POSIX ();
use Test::MockModule;
use Test::More;
use Time::HiRes ();

use PVE::Tools qw(file_set_contents);

use PVE::API2::Storage::Config;
use PVE::API2::Storage::Scan;
use PVE::CLI::pvesm;
use PVE::NVMeOF;
use PVE::Storage;

my $dir = tempdir('nvmeof-test-XXXX', DIR => '.', CLEANUP => 1);
local $PVE::NVMeOF::RUN_DIR = "$dir/run";
local $PVE::NVMeOF::SYSFS = "$dir/sys";
my $class = 'PVE::Storage::NVMeOFPlugin';
my $cfg = {
    type => 'nvmeof',
    subsysnqn => 'nqn.test',
    portals => '192.0.2.1,[2001:db8::2]:4421',
    content => { none => 1 },
};

subtest 'configuration, portals and volumes' => sub {
    is(PVE::Storage::Plugin->lookup('nvmeof'), $class, 'plugin registered');
    is_deeply(
        [sort keys $class->properties()->%*],
        [qw(fast-io-fail-tmo portals subsysnqn)],
        'only three plugin-specific properties',
    );
    my sub nvmeof_schema($schema) {
        my @types = map { ($_->{oneOf} // [])->@* } ($schema->{allOf} // [])->@*;
        my ($nvmeof) = grep { $_->{'instance-type'} eq 'nvmeof' } @types;
        return $nvmeof->{properties} // {};
    }
    my $create = nvmeof_schema(PVE::Storage::Plugin->createSchema());
    is_deeply(
        [sort grep { !$create->{$_}->{optional} } keys $create->%*],
        [qw(portals subsysnqn)],
        'adding a storage requires the subsystem and the portals',
    );
    my $update = nvmeof_schema(PVE::Storage::Plugin->updateSchema());
    ok($update->{portals} && !$update->{subsysnqn}, 'the subsystem cannot be changed');
    my $parsed = PVE::Storage::Plugin->parse_config(
        'storage.cfg',
        "nvmeof: fabric\n\tsubsysnqn nqn.test\n\tportals 192.0.2.1\n\tcontent none\n",
    );
    is($parsed->{ids}->{fabric}->{subsysnqn}, 'nqn.test', 'storage.cfg accepts the plugin');
    is_deeply(
        $parsed->{ids}->{fabric}->{content},
        { none => 1 },
        'connection-only content parses',
    );
    is_deeply(
        PVE::NVMeOF::parse_portal('192.0.2.1'),
        { traddr => '192.0.2.1', trsvcid => 4420 },
        'default I/O port',
    );
    is_deeply(
        PVE::NVMeOF::parse_portal('[2001:0db8::1]:4421'),
        { traddr => '2001:db8::1', trsvcid => 4421 },
        'canonical IPv6',
    );
    is(
        PVE::NVMeOF::format_path({ traddr => '2001:db8::1', trsvcid => 4421 }),
        '[2001:db8::1]:4421',
        'an IPv6 portal in brackets',
    );
    is(
        PVE::NVMeOF::format_path({ traddr => 'nn-0x2000:pn-0x2001' }),
        'nn-0x2000:pn-0x2001',
        'the address of a Fibre Channel port as it is',
    );

    for my $bad (
        'host.example', '1.2.3.4:0', '1.2.3.4:65536', '[fe80::1]', '[::]', '224.0.0.1',
    ) {
        ok(!PVE::NVMeOF::parse_portal($bad), "invalid portal $bad");
    }
    eval { $class->on_add_hook('fabric', { %$cfg, portals => '192.0.2.1,192.0.2.1:4420' }); };
    like($@, qr/listed more than once/, 'duplicate portals refused');
    eval { $class->on_add_hook('fabric', { %$cfg, portals => ',,' }); };
    like($@, qr/no portal given/, 'a list without a portal refused');
    eval {
        $class->on_add_hook(
            'fabric',
            { %$cfg, subsysnqn => 'nqn.2014-08.org.nvmexpress.discovery' },
        );
    };
    like($@, qr/discovery subsystem/, 'discovery subsystem refused');
    ok(!PVE::NVMeOF::subsystem_nqn("nqn.test\e[31m"),
        'an NQN with a control character refused');
    ok(!PVE::NVMeOF::subsystem_nqn("nqn.test\x{2028}x"), 'so is one with a Unicode separator');
    my $storage = Test::MockModule->new('PVE::Storage');
    my $ids = { fabric => $cfg, local => { type => 'dir' } };
    $storage->mock(config => sub { { ids => $ids } });
    my $first = PVE::NVMeOF::storage_paths($cfg)->[0];
    PVE::NVMeOF::note_connect_failure($cfg, $first);
    is(eval { $class->on_add_hook('fabric', $cfg); 1 }, 1, 'a storage is not in its own way');
    ok(!PVE::NVMeOF::in_cooldown($cfg, $first), 'adding it tries all portals again');
    eval { $class->on_add_hook('second', {%$cfg}) };
    like($@, qr/storage 'fabric' already uses this subsystem/, 'one storage per subsystem');
    $ids->{vg} = { type => 'lvm', base => 'fabric:nvme-eui.0123456789abcdef', vgname => 'vg' };
    my $images = { content => { images => 1 } };
    eval { $class->on_update_hook_full('fabric', $cfg, $images, [], {}) };
    like(
        $@,
        qr/storage 'vg' uses a namespace/,
        'no direct use of a namespace with a volume group',
    );
    my $none = { %$cfg, content => { none => 1 } };
    eval { $class->on_update_hook_full('fabric', $none, {}, ['content'], {}) };
    like($@, qr/storage 'vg' uses a namespace/, 'also not through the default content');
    is(
        eval { $class->on_update_hook_full('other', $cfg, $images, [], {}); 1 },
        1,
        'unless unused',
    );
    delete $ids->{vg};
    PVE::NVMeOF::note_connect_failure($cfg, $first);
    $class->on_update_hook_full('fabric', $cfg, { portals => $cfg->{portals} }, [], {});
    ok(!PVE::NVMeOF::in_cooldown($cfg, $first), 'a new portal list tries all portals again');
    is(
        scalar($class->filesystem_path($cfg, 'nvme-eui.0123456789abcdef')),
        '/dev/disk/by-id/nvme-eui.0123456789abcdef',
        'stable volume path',
    );
    {
        my $disk = Test::MockModule->new('PVE::Diskmanage');
        my $nqn = 'nqn.other';
        $disk->redefine(read_subsysnqn => sub { $nqn });
        my $sysfs = Test::MockModule->new('PVE::NVMeOF');
        my $head = 1;
        $sysfs->redefine(namespace_wwid => sub { 'x' }, is_namespace_head => sub { $head });
        my $check = sub {
            eval { PVE::Storage::NVMeOFPlugin::assert_namespace($cfg, @_) };
            $@;
        };
        like($check->('nvme-x', '/dev/nvme0n1'), qr/not part of subsystem/, 'other subsystem');
        like($check->('nvme-x', '/dev/sda'), qr/not an NVMe namespace/, 'other device type');
        like($check->('nvme-x', '/dev/nvme0n1p1'), qr/not an NVMe namespace/, 'partition');
        $nqn = 'nqn.test';
        is($check->('nvme-x', '/dev/nvme0n1'), '', 'namespace of the subsystem');
        $head = 0;
        like(
            $check->('nvme-x', '/dev/nvme0n1'),
            qr/device of a single controller/,
            'a namespace without native multipath',
        );
        $head = 1;
        like(
            $check->('nvme-Vendor_Model_123_1', '/dev/nvme0n1'),
            qr/not named after the WWID/,
            'another by-id name of a namespace of the subsystem',
        );
        my $cwd = Test::MockModule->new('Cwd');
        $cwd->redefine(realpath => sub { $dir });
        eval { $class->filesystem_path($cfg, 'nvme-eui.0123456789abcdef') };
        like($@, qr/not an NVMe namespace/, 'the path of an existing device is checked');
    }

    for my $bad ('nvme-../../sda', 'nvme-eui.abc-part1', 'sda') {
        eval { $class->parse_volname($bad) };
        like($@, qr/unable to parse/, "invalid volume $bad");
    }
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    $nvme->mock(namespaces => sub { { nvme0n1 => { wwid => 'eui.abc', size => 4096 } } });
    is(
        $class->list_volumes('fabric', $cfg)->[0]->{volid},
        'fabric:nvme-eui.abc',
        'content none still offers bases',
    );
    is_deeply($class->list_images('fabric', $cfg, 100), [], 'namespaces have no VM owner');
    $nvme->mock(subsystem_controllers => sub { {} });
    eval { $class->activate_volume('fabric', $cfg, 'nvme-nonexistent-test') };
    like($@, qr/not available/,
        'missing namespace fails without waiting on absent controllers');
    my ($now, $queries) = (0, 0);
    my $clock = Test::MockModule->new('Time::HiRes');
    $clock->redefine(time => sub { $now += 5 }, usleep => sub { });
    $nvme->mock(subsystem_controllers => sub { $queries++; { nvme1 => {} } });
    eval { $class->activate_volume('fabric', $cfg, 'nvme-nonexistent-test', undef, {}) };
    is($queries, 1, 'a missing namespace of a connected subsystem is waited for');
    eval {
        $class->activate_volume(
            'fabric',
            $cfg,
            'nvme-nonexistent-test',
            undef,
            { status_only => 1 },
        );
    };
    like($@, qr/not available/, 'in a status pass it fails as well');
    is($queries, 1, 'but without a wait');
    is(
        ($class->parse_volname('nvme-eui.0123456789abcdef'))[0],
        'vm-vol',
        'namespaces are VM disks',
    );
};

subtest 'the first storage of the type needs an upgraded cluster' => sub {
    my $raw = "dir: local\n\tpath /var/lib/vz\n\tcontent iso\n";
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $asserted;
    $cluster->mock(assert_min_cluster_version => sub { $asserted = join('.', @_) });
    my $storage = Test::MockModule->new('PVE::Storage');
    $storage->mock(
        lock_storage_config => sub { $_[0]->() },
        config => sub { PVE::Storage::Plugin->parse_config('storage.cfg', $raw) },
        write_config => sub { $raw = PVE::Storage::Plugin->write_config('storage.cfg', $_[0]) },
        activate_storage => sub { },
    );
    my $add = sub($storeid, $nqn) {
        $asserted = undef;
        PVE::API2::Storage::Config->create({
            storage => $storeid,
            type => 'nvmeof',
            subsysnqn => $nqn,
            portals => '192.0.2.1',
        });
        return $asserted;
    };
    is($add->('fabric1', 'nqn.first'), '9.2.38', 'the first one checks the cluster');
    is($add->('fabric2', 'nqn.second'), undef, 'a further one does not');
};

subtest 'sysfs topology' => sub {
    my $sys = $PVE::NVMeOF::SYSFS;
    make_path(
        "$sys/class/nvme-fabrics/ctl/nvme1",
        "$sys/class/nvme-subsystem/nvme-subsys0/nvme0n1",
        "$sys/block/nvme0n1",
    );
    my %attrs = (
        address => 'traddr=192.0.2.1,trsvcid=4420',
        transport => 'tcp',
        subsysnqn => 'nqn.test',
        state => 'connecting',
    );
    file_set_contents("$sys/class/nvme-fabrics/ctl/nvme1/$_", "$attrs{$_}\n") for keys %attrs;
    file_set_contents("$sys/class/nvme-subsystem/nvme-subsys0/subsysnqn", "nqn.test\n");
    file_set_contents("$sys/block/nvme0n1/wwid", "eui.abc\n");
    file_set_contents("$sys/block/nvme0n1/size", "8\n");
    make_path(
        "$sys/devices/virtual/nvme-subsystem/nvme-subsys0",
        "$sys/devices/virtual/nvme-fabrics/ctl/nvme1",
        "$sys/block/nvme1n1",
    );
    symlink('../../devices/virtual/nvme-subsystem/nvme-subsys0', "$sys/block/nvme0n1/device")
        or die "symlink failed - $!\n";
    symlink('../../devices/virtual/nvme-fabrics/ctl/nvme1', "$sys/block/nvme1n1/device")
        or die "symlink failed - $!\n";
    ok(
        PVE::NVMeOF::is_namespace_head('nvme0n1'),
        'a device of the subsystem is a namespace head',
    );
    ok(!PVE::NVMeOF::is_namespace_head('nvme1n1'), 'the device of a single controller is not');
    ok(!PVE::NVMeOF::is_namespace_head('nvme9n1'), 'nor one that is gone');
    is(
        PVE::NVMeOF::controllers()->{nvme1}->{state},
        'connecting',
        'reconnecting controller is still present',
    );
    is(
        PVE::NVMeOF::path_controller(
            PVE::NVMeOF::subsystem_controllers('nqn.test'),
            PVE::NVMeOF::parse_portal('192.0.2.1'),
        ),
        'nvme1',
        'portal controller found',
    );
    is_deeply(
        PVE::NVMeOF::namespaces('nqn.test'),
        { nvme0n1 => { wwid => 'eui.abc', size => 4096 } },
        'namespace facts',
    );
    is_deeply(PVE::NVMeOF::namespaces('nqn.other'), {}, 'other subsystem excluded');
    unlink("$sys/block/nvme0n1/wwid");
    eval { PVE::NVMeOF::namespaces('nqn.test') };
    like($@, qr/cannot read the WWID/, 'an unreadable namespace is an error, not a gone one');
    file_set_contents("$sys/block/nvme0n1/wwid", "eui.abc\n");
    make_path("$sys/class/nvme-subsystem/nvme-subsys9");
    eval { PVE::NVMeOF::namespaces('nqn.test') };
    like($@, qr/cannot read the NQN/, 'so is a subsystem that could be the one asked for');
    rmdir("$sys/class/nvme-subsystem/nvme-subsys9");
};

subtest 'serialized commands inherit the lock' => sub {
    local $PVE::NVMeOF::LOCK_WAIT = 0;
    my $script = join(
        ' ',
        'my @held = grep { (readlink($_) // "") =~ /fabrics.lock$/ } glob("/proc/self/fd/*");',
        'die "not inherited" if !@held;',
        'open(my $fh, ">>", $ARGV[0]) or die $!;',
        'die "lock missing" if flock($fh, LOCK_EX|LOCK_NB);',
        'print "locked\\n";',
    );
    my $value = PVE::NVMeOF::with_lock(sub {
        my $out = '';
        PVE::Tools::run_command(
            [
                '/usr/bin/perl',
                '-MFcntl=:flock',
                '-e',
                $script,
                "$PVE::NVMeOF::RUN_DIR/fabrics.lock",
            ],
            timeout => 5,
            outfunc => sub { $out .= $_[0] },
        );
        return $out;
    });
    is($value, 'locked', 'exec cannot acquire the lock held by its parent');
    open(my $lock, '>>', "$PVE::NVMeOF::RUN_DIR/fabrics.lock") or die $!;
    flock($lock, LOCK_EX) or die $!;
    eval {
        PVE::NVMeOF::with_lock(sub { die "must not count paths\n" });
    };
    like($@, qr/is busy/, 'busy operation never enters callback');
    close($lock);

    my $tools = Test::MockModule->new('PVE::Tools');
    my $timed_out = 1;
    $tools->mock(run_fork_with_timeout => sub { return ($_[1]->(), $timed_out) });
    my $retained;
    eval {
        PVE::NVMeOF::with_lock(sub {
            my ($path) =
                grep { (readlink($_) // '') =~ /fabrics.lock$/ } glob('/proc/self/fd/*');
            my ($fd) = $path =~ m{/(\d+)$};
            open($retained, '>&', $fd) or die $!;
            return 1;
        });
    };
    like($@, qr/timed out.*may still be running/, 'timeout does not claim deletion completed');
    $timed_out = 0;
    eval {
        PVE::NVMeOF::with_lock(sub { die "must not count paths\n" });
    };
    like($@, qr/is busy/, 'retained descriptor keeps serialization after the caller returns');
    close($retained);
    is(PVE::NVMeOF::with_lock(sub { 1 }), 1, 'last descriptor close releases serialization');

    my @timeouts;
    $tools->mock(run_fork_with_timeout => sub { push @timeouts, $_[0]; return ($_[1]->(), 0) });
    PVE::NVMeOF::with_lock(sub { 1 }, $_) for undef, 100, 4.2, 0.3;
    my $hung = PVE::NVMeOF::storage_paths($cfg)->[0];
    $tools->mock(run_fork_with_timeout => sub { return (undef, 1) });
    eval { PVE::NVMeOF::connect_path($cfg, $hung) };
    is($@, $PVE::NVMeOF::TIMED_OUT, 'a hanging connect times out');
    ok(PVE::NVMeOF::in_cooldown($cfg, $hung), 'and arms the cooldown of its portal');
    unlink(glob("$PVE::NVMeOF::RUN_DIR/retry/*"));
    my $full = $PVE::NVMeOF::LOCK_WAIT + 35;
    is_deeply(
        \@timeouts, [$full, 100, 5, 1], 'a limit sets the time in whole seconds',
    );
};

subtest 'a short hold of the lock does not fail a connect' => sub {
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $net = Test::MockModule->new('PVE::Network');
    my $scfg = { %$cfg, subsysnqn => 'nqn.wait', portals => '192.0.2.3' };
    my $ctl = "$PVE::NVMeOF::SYSFS/class/nvme-fabrics/ctl/nvme9";
    $net->mock(tcp_ping => sub { 1 });
    $nvme->mock(assert_support => sub { });
    $nvme->mock(host_identity =>
        sub { { nqn => 'nqn.host', id => '12345678-1234-1234-1234-123456789abc' } });
    $nvme->mock(
        run_command => sub(@) {
            my %attrs = (
                address => 'traddr=192.0.2.3,trsvcid=4420',
                transport => 'tcp',
                subsysnqn => 'nqn.wait',
                state => 'live',
            );
            make_path($ctl);
            file_set_contents("$ctl/$_", "$attrs{$_}\n") for keys %attrs;
        },
    );
    pipe(my $held, my $signal) or die "pipe failed - $!\n";
    my $holder = fork() // die "fork failed - $!\n";
    if (!$holder) {
        open(my $lock, '>>', "$PVE::NVMeOF::RUN_DIR/fabrics.lock") or POSIX::_exit(1);
        flock($lock, LOCK_EX) or POSIX::_exit(1);
        syswrite($signal, '1');
        Time::HiRes::sleep(0.5);
        POSIX::_exit(0);
    }
    close($signal);
    sysread($held, my $state, 1);
    is($state, '1', 'another process holds the lock');
    my $warning = '';
    {
        local $SIG{__WARN__} = sub { $warning .= $_[0] };
        $class->activate_storage('wait', $scfg);
    }
    waitpid($holder, 0);
    is($warning, '', 'the connect waits for the lock');
    my $path = PVE::NVMeOF::storage_paths($scfg)->[0];
    ok(!PVE::NVMeOF::in_cooldown($scfg, $path), 'and arms no cooldown');
    is(
        PVE::NVMeOF::path_controller(PVE::NVMeOF::subsystem_controllers('nqn.wait'), $path),
        'nvme9',
        'the portal is connected',
    );
    remove_tree($ctl);

    local $PVE::NVMeOF::LOCK_WAIT = 0.2;
    pipe($held, $signal) or die "pipe failed - $!\n";
    $holder = fork() // die "fork failed - $!\n";
    if (!$holder) {
        open(my $lock, '>>', "$PVE::NVMeOF::RUN_DIR/fabrics.lock") or POSIX::_exit(1);
        flock($lock, LOCK_EX) or POSIX::_exit(1);
        syswrite($signal, '1');
        Time::HiRes::sleep(1);
        POSIX::_exit(0);
    }
    close($signal);
    sysread($held, $state, 1);
    {
        local $SIG{__WARN__} = sub { };
        eval { $class->activate_storage('wait', $scfg) };
    }
    waitpid($holder, 0);
    like($@, qr/NVMe over Fabrics is busy/, 'a longer hold fails the connect');
    ok(!PVE::NVMeOF::in_cooldown($scfg, $path), 'but arms no cooldown for the portal');
};

subtest 'connect options, kernel reconnect and cooldown' => sub {
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $net = Test::MockModule->new('PVE::Network');
    my @commands;
    my $controllers = {};
    my @limits;
    $nvme->mock(with_lock => sub { push @limits, $_[1]; $_[0]->() });
    $nvme->mock(assert_support => sub { });
    $nvme->mock(host_identity =>
        sub { { nqn => 'nqn.host', id => '12345678-1234-1234-1234-123456789abc' } });
    $nvme->mock(subsystem_controllers => sub { $controllers });
    my $appear = 1;
    our $state = 'live';
    my ($now, $connect_secs) = (0, 0);
    $nvme->mock(
        run_command => sub($cmd, %opts) {
            is($opts{timeout}, 30, 'bounded connect');
            push @commands, $cmd;
            $now += $connect_secs;
            my %arg =
                map { $cmd->[$_] => $cmd->[$_ + 1] } grep { $cmd->[$_] =~ /^--tr/ } 0 .. $#$cmd;
            my $controller =
                { traddr => $arg{'--traddr'}, trsvcid => $arg{'--trsvcid'}, state => $state };
            $controllers->{ 'nvme' . scalar(@commands) } = $controller if $appear;
        },
    );
    $net->mock(tcp_ping => sub { 1 });
    $class->activate_storage('fabric', $cfg);
    is(scalar(@commands), 2, 'one connect per missing portal');
    ok(grep({ $_ eq '--ctrl-loss-tmo=-1' } $commands[0]->@*), 'kernel reconnects indefinitely');
    is_deeply([@{ $commands[0] }[-2, -1]], ['--fast_io_fail_tmo', 120], 'fast failure default');
    {
        my $clock = Test::MockModule->new('Time::HiRes');
        $clock->redefine(time => sub { $now });
        my $activate = sub($secs) {
            ($controllers, $connect_secs, @commands) = ({}, $secs);
            local $SIG{__WARN__} = sub { };
            eval { $class->activate_storage('fabric', $cfg) };
            unlink(glob("$PVE::NVMeOF::RUN_DIR/retry/*"));
            return scalar(@commands);
        };
        is($activate->(20), 1, 'with a live controller, a spent time budget leaves the rest');
        $appear = 0;
        is($activate->(20), 2, 'without one, a slow portal does not keep the next one out');
        is($activate->(50), 1, 'but a very slow one does');
        @limits = ();
        is($activate->(40), 2, 'one that starts late is still tried');
        is_deeply(\@limits, [45, 5], 'but only for the rest of the time limit');
        local $state = 'connecting';
        $appear = 1;
        is($activate->(20), 2, 'a new controller that is still connecting is not live');
        $appear = 0;
        ($appear, $connect_secs) = (1, 0);
    }
    @commands = ();
    $controllers = {
        nvme1 => { traddr => '192.0.2.1', trsvcid => 4420, state => 'connecting' },
        nvme2 => { traddr => '2001:db8::2', trsvcid => 4421, state => 'live' },
    };
    $class->activate_storage('fabric', $cfg);
    is_deeply(\@commands, [],
        'existing controllers are never reconnected, regardless of state');
    $controllers->{nvme2}->{state} = 'connecting';
    eval { $class->activate_storage('fabric', $cfg) };
    like($@, qr/no controller of the subsystem is live/, 'without a live one it fails');
    is_deeply(\@commands, [], 'but leaves the reconnect to the kernel');
    $controllers->{nvme2}->{state} = 'live';
    $class->deactivate_storage('fabric', $cfg);
    is_deeply(\@commands, [], 'deactivation does not remove controllers');
    $controllers = {};
    my $path = PVE::NVMeOF::storage_paths($cfg)->[0];
    PVE::NVMeOF::note_connect_failure($cfg, $path);
    ok(PVE::NVMeOF::in_cooldown($cfg, $path), 'failure starts cooldown');
    $class->activate_storage('fabric', $cfg);
    is(scalar(@commands), 1, 'only the other portal connects');
    ($controllers, $appear) = ({}, 0);
    my $other = PVE::NVMeOF::storage_paths($cfg)->[1];
    my $warning = '';
    {
        local $SIG{__WARN__} = sub { $warning .= $_[0] };
        eval { $class->activate_storage('fabric', $cfg) };
    }
    like($warning, qr/no controller appeared/, 'a connect without a controller is a failure');
    ok(PVE::NVMeOF::in_cooldown($cfg, $other), 'which starts the cooldown too');
    like(
        $@,
        qr/^cannot connect to any portal - .*failed less than a minute ago; .*no controller appeared/,
        'without any controller the activation fails with the reason of each portal',
    );
    unlink(glob("$PVE::NVMeOF::RUN_DIR/retry/*"));
    $nvme->mock(assert_support => sub { die "nvme-cli is not installed\n" });
    @commands = ();
    eval { $class->activate_storage('fabric', $cfg) };
    is($@, "nvme-cli is not installed\n", 'a missing prerequisite fails the activation');
    is_deeply(\@commands, [], 'before any connect');
    ok(!PVE::NVMeOF::in_cooldown($cfg, $path), 'and without a cooldown');
    $controllers = { nvme1 => { traddr => '192.0.2.1', trsvcid => 4420, state => 'live' } };
    $warning = '';
    {
        local $SIG{__WARN__} = sub { $warning .= $_[0] };
        is(
            eval { $class->activate_storage('fabric', $cfg); 1 },
            1,
            'but not with a usable path',
        );
    }
    like($warning, qr/cannot connect to the other portals - nvme-cli/, 'which it warns about');
    $nvme->mock(assert_support => sub { });
    $controllers =
        { nvme1 => { traddr => '192.0.2.1', trsvcid => 4420, state => 'connecting' } };
    {
        local $SIG{__WARN__} = sub { };
        eval { $class->activate_storage('fabric', $cfg) };
    }
    like(
        $@,
        qr/^cannot connect to any portal/,
        'a reconnecting path is not usable when the missing one fails',
    );
    # a connect that worked, but whose controller is not live yet
    ($controllers, $now, $connect_secs) = ({}, 0, 0);
    $net->mock(tcp_ping => sub { 1 });
    $nvme->mock(
        run_command => sub($cmd, %opts) {
            my %arg =
                map { $cmd->[$_] => $cmd->[$_ + 1] } grep { $cmd->[$_] =~ /^--tr/ } 0 .. $#$cmd;
            $controllers->{nvme7} =
                { traddr => $arg{'--traddr'}, trsvcid => $arg{'--trsvcid'}, state => 'connecting' };
        },
    );
    eval { $class->activate_storage('fabric', { %$cfg, portals => '192.0.2.7' }) };
    is($@, "no controller of the subsystem is live yet\n", 'says so without an empty reason');
};

subtest 'the connection check has a time limit too' => sub {
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $net = Test::MockModule->new('PVE::Network');
    my $clock = Test::MockModule->new('Time::HiRes');
    my ($now, $pings) = (0, 0);
    $clock->redefine(time => sub { $now });
    $nvme->mock(subsystem_controllers => sub { {} });
    $net->mock(tcp_ping => sub { $now += 2; $pings++; 0 });
    my $scfg = { %$cfg, portals => join(',', map { "192.0.2.$_" } 1 .. 20) };
    is($class->check_connection('many', $scfg), 1, 'untried portals go to the activation');
    is($pings, 8, 'which the check leaves after its time limit');
    ok(
        PVE::NVMeOF::in_cooldown($scfg, PVE::NVMeOF::parse_portal('192.0.2.8')),
        'the failed ones cool down',
    );
    ($now, $pings) = (0, 0);
    is($class->check_connection('many', $scfg), 1, 'a later pass skips the failed portals');
    ok(
        PVE::NVMeOF::in_cooldown($scfg, PVE::NVMeOF::parse_portal('192.0.2.16')),
        'and gets further',
    );
    unlink(glob("$PVE::NVMeOF::RUN_DIR/retry/*"));
    $scfg->{portals} = '192.0.2.1,192.0.2.2';
    ($now, $pings) = (0, 0);
    is($class->check_connection('few', $scfg), 0, 'all tried and none answers');
    unlink(glob("$PVE::NVMeOF::RUN_DIR/retry/*"));
    my $reconnecting = { traddr => '192.0.2.1', trsvcid => 4420, state => 'connecting' };
    $nvme->mock(subsystem_controllers => sub { { nvme1 => $reconnecting } });
    ($scfg->{portals}, $pings) = ('192.0.2.1', 0);
    eval { $class->check_connection('few', $scfg) };
    is($@, "no controller of the subsystem is live\n", 'with a controller for each portal');
    is($pings, 0, 'which the check does not ping');
};

subtest 'TCP discovery, fallback and API result' => sub {
    my $record = {
        subtype => 'nvme subsystem',
        trtype => 'tcp',
        subnqn => 'nqn.test',
        traddr => '192.0.2.1',
        trsvcid => '4420',
    };
    my $raw = encode_json({
        records =>
            [$record, $record, { %$record, trtype => 'rdma' }, { %$record, trsvcid => 70000 }],
    });
    is_deeply(
        PVE::NVMeOF::parse_discovery_log($raw),
        { 'nqn.test' => [{ traddr => '192.0.2.1', trsvcid => 4420 }] },
        'TCP only, unique valid portals',
    );
    my $padded =
        { %$record, traddr => '192.0.2.1' . (' ' x 10), trsvcid => '4420' . (' ' x 28) };
    is_deeply(
        PVE::NVMeOF::parse_discovery_log(encode_json({ records => [$padded] })),
        { 'nqn.test' => [{ traddr => '192.0.2.1', trsvcid => 4420 }] },
        'padded address fields',
    );
    for my $bad ('garbage', '{}', '[]') {
        eval { PVE::NVMeOF::parse_discovery_log($bad) };
        ok($@, 'malformed discovery is not an empty success');
    }
    my $nvme = Test::MockModule->new('PVE::NVMeOF');
    my $net = Test::MockModule->new('PVE::Network');
    my (@ports, @commands);
    our $locked;
    $nvme->mock(with_lock => sub { local $locked = 1; $_[0]->() });
    $nvme->mock(assert_support => sub { });
    $nvme->mock(host_identity => sub { { nqn => 'nqn.host', id => 'id' } });
    $net->mock(tcp_ping => sub { push @ports, $_[1]; $_[1] != 8009 });
    $nvme->mock(
        run_command => sub($cmd, %opts) {
            ok($locked, 'discovery holds the NVMe lock');
            is($opts{timeout}, 30, 'discovery command timeout');
            push @commands, $cmd;
            $opts{outfunc}->($raw);
        },
    );
    my $rows = PVE::API2::Storage::Scan->nvmeofscan({ node => 'n1', portal => '192.0.2.1' });
    is_deeply(\@ports, [8009, 4420], 'unspecified port tries discovery then I/O port');
    is_deeply($rows, [{ subsysnqn => 'nqn.test', portals => '192.0.2.1:4420' }], 'API shape');
    eval {
        PVE::JSONSchema::validate(
            $rows,
            PVE::API2::Storage::Scan->map_method_by_name('nvmeofscan')->{returns},
        );
    };
    is($@, '', 'API schema accepts result');
    @ports = ();
    PVE::NVMeOF::discover('[2001:db8::1]:4421');
    is_deeply(\@ports, [4421], 'explicit port does not fall back');
    # the discovery port answers but refuses, the I/O port does not answer
    $net->mock(tcp_ping => sub { $_[1] == 8009 });
    $nvme->mock(run_command => sub { die "host not allowed\n" });
    eval { PVE::NVMeOF::discover('192.0.2.1') };
    like($@, qr/failed - host not allowed$/, 'the error of the answering port is kept');
    $net->mock(tcp_ping => sub { $_[1] == 4420 });
    eval { PVE::NVMeOF::discover('192.0.2.1') };
    like($@, qr/failed - host not allowed$/, 'also if the other port answers');
    $net->mock(tcp_ping => sub { 0 });
    eval { PVE::NVMeOF::discover('192.0.2.1') };
    like($@, qr/failed - no answer from portal$/, 'without any answer');
    $nvme->mock(assert_support => sub { die "nvme-cli is not installed\n" });
    eval { PVE::NVMeOF::discover('192.0.2.1') };
    is($@, "nvme-cli is not installed\n", 'a missing prerequisite is reported as it is');
    # nvme-cli itself writes its diagnostic to stderr and exits with an error
    $nvme->unmock('run_command');
    $nvme->mock(assert_support => sub { });
    my $cli = "$dir/nvme-refusing";
    file_set_contents($cli, "#!/bin/sh\necho 'host not allowed' >&2\nexit 1\n", 0755);
    {
        local $PVE::NVMeOF::NVME = $cli;
        $net->mock(tcp_ping => sub { $_[1] == 8009 });
        eval { PVE::NVMeOF::discover('192.0.2.1') };
        like(
            $@,
            qr/failed - nvme discover: host not allowed$/,
            'with the diagnostic of nvme-cli',
        );
    }
    my $formatter = Test::MockModule->new('PVE::CLIFormatter');
    my $schema = PVE::API2::Storage::Scan->map_method_by_name('nvmeofscan')->{returns};
    $formatter->mock(
        print_api_result => sub {
            is($_[0], $rows, 'CLI forwards complete discovery rows');
            is($_[1], $schema, 'CLI forwards discovery schema');
            is($_[3]->{'output-format'}, 'json', 'CLI supports structured output');
        },
    );
    $PVE::CLI::pvesm::cmddef->{scan}->{nvmeof}->[4]
        ->($rows, $schema, { 'output-format' => 'json' });
};

done_testing();
