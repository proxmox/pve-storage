package PVE::API2::Disks::Test;

use v5.36;

use lib qw(..);

use PVE::API2::Disks;
use PVE::API2::Disks::LVM;
use PVE::Diskmanage;
use PVE::Storage;
use PVE::Storage::LVMPlugin;

use Test::MockModule;
use Test::More;

my @transport_cases = (
    ['none', undef],
    ['unknown', 'unknown'],
    ['ata', 'ata'],
    ['sata', 'sata'],
    ['sas', 'sas'],
    ['spi', 'spi'],
    ['sbp', 'sbp'],
    ['usb', 'usb'],
    ['fc', 'fc'],
    ['fcoe', 'fcoe'],
    ['iscsi', 'iscsi'],
    ['nvme', 'nvme'],
    ['virtio', 'virtio'],
    ['nvme-fc', 'nvme-fc'],
    ['nvme-tcp', 'nvme-tcp'],
    ['nvme-rdma', 'nvme-rdma'],
    ['nvme-loop', 'nvme-loop'],
);

my $disks = {};
my @all;
for my $case (@transport_cases) {
    my ($name, $transport) = $case->@*;
    my $devpath = "/dev/test-$name";
    my $entry = {
        devpath => $devpath,
        gpt => 0,
        osdid => -1,
    };
    $entry->{transport} = $transport if defined($transport);
    $disks->{$name} = $entry;
    push @all, $devpath;
}

my $diskmanage_module = Test::MockModule->new('PVE::Diskmanage', no_auto => 1);
$diskmanage_module->mock('get_disks' => sub { return $disks });

my $list_handler = PVE::API2::Disks->map_method_by_name('list')->{code};
my $list_devpaths = sub($param) {
    my $result = $list_handler->({ node => 'test-node', skipsmart => 1, $param->%* });
    return [sort map { $_->{devpath} } $result->@*];
};

is_deeply(
    $list_devpaths->({}), [sort @all], 'the disk list uses the default disk inventory',
);

is_deeply(
    $list_devpaths->({ type => 'unused' }),
    [sort @all],
    'unused disk selection preserves transport visibility',
);

$diskmanage_module->mock(
    'get_disks' => sub {
        return {
            'fc-disk' => {
                devpath => '/dev/fc-disk',
                gpt => 0,
                osdid => -1,
                transport => 'fc',
            },
            'fc-partition' => {
                devpath => '/dev/fc-disk1',
                gpt => 0,
                osdid => -1,
                parent => '/dev/fc-disk',
            },
            'loop-disk' => {
                devpath => '/dev/loop-disk',
                gpt => 0,
                osdid => -1,
                transport => 'nvme-loop',
            },
            'loop-partition' => {
                devpath => '/dev/loop-disk1',
                gpt => 0,
                osdid => -1,
                parent => '/dev/loop-disk',
            },
        };
    },
);

is_deeply(
    $list_devpaths->({ 'include-partitions' => 1 }),
    ['/dev/fc-disk', '/dev/fc-disk1', '/dev/loop-disk', '/dev/loop-disk1'],
    'the plain list keeps FC disks and their partitions',
);

is_deeply(
    $list_devpaths->({ 'include-partitions' => 1, type => 'unused' }),
    ['/dev/fc-disk', '/dev/fc-disk1', '/dev/loop-disk', '/dev/loop-disk1'],
    'unused partitions preserve transport visibility',
);

subtest 'volume group creation leaves SAN locking to the LVM helper' => sub {
    my $storage = Test::MockModule->new('PVE::Storage');
    my $disk = $diskmanage_module;
    my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
    my $rpc = Test::MockModule->new('PVE::RPCEnvironment');
    my (@stack, @created);
    my $lock = sub($name, $code) {
        push @stack, $name;
        my $res = eval { $code->() };
        my $err = $@;
        pop @stack;
        die $err if $err;
        return $res;
    };
    $storage->mock(lock_storage_config => sub { $lock->('storage.cfg', $_[0]) });
    $disk->mock(locked_disk_action => sub { $lock->('disk', $_[0]) });
    $disk->mock(verify_blockdev_path => sub { $_[0] });
    $disk->mock(assert_disk_unused => sub { });
    $disk->mock(is_partition => sub { 0 });
    $disk->mock(udevadm_trigger => sub { });
    $lvm->mock(lvm_vgs => sub { {} });
    $lvm->mock(
        lvm_create_volume_group => sub {
            is(scalar(@_), 3, 'disk API does not exempt a storage');
            push @created, [$_[0], join('>', @stack)];
        },
    );
    {

        package MultipathTestRPC;
        sub get_user { 'root@pam' }
        sub check { 1 }
        sub fork_worker { $_[4]->(); return 'test-upid' }
    }
    $rpc->mock(get => sub { bless {}, 'MultipathTestRPC' });

    my ($is_map, $is_san) = (1, 0);
    my $san = Test::MockModule->new('PVE::Storage::SAN');
    $san->mock(is_san_device => sub { $is_map || $is_san });
    PVE::API2::Disks::LVM->create({ node => 'n1', name => 'vg1', device => '/dev/dm-3' });
    ($is_map, $is_san) = (0, 1);
    PVE::API2::Disks::LVM->create({ node => 'n1', name => 'vg2', device => '/dev/sdc' });
    ($is_map, $is_san) = (0, 0);
    PVE::API2::Disks::LVM->create({ node => 'n1', name => 'vg3', device => '/dev/sdb' });
    is_deeply(
        \@created,
        [
            ['/dev/dm-3', ''], ['/dev/sdc', ''], ['/dev/sdb', 'disk'],
        ],
        'SAN writes use no local disk or storage configuration lock',
    );
    my $config_api = Test::MockModule->new('PVE::API2::Storage::Config');
    my (@params, @registered);
    $config_api->mock(
        create_or_update => sub {
            push @registered, $_[5] ? 'check' : 'create or update';
            push @params, $_[3];
        },
        create => sub {
            push @registered, 'create';
            push @params, $_[1];
        },
    );
    my $cfg =
        { ids => { other => { type => 'lvm', vgname => 'other', nodes => { n2 => 1 } } } };
    $storage->mock(config => sub { $cfg });
    $is_san = 1;
    PVE::API2::Disks::LVM->create({
        node => 'n1',
        name => 'vg4',
        device => '/dev/sdc',
        add_storage => 1,
    });
    is($params[-1]->{shared}, 1, 'SAN storage is shared');
    ok(!exists($params[-1]->{nodes}), 'SAN storage covers all nodes');
    is_deeply(
        \@registered,
        ['check', 'create'],
        'and only gets created: it never joins a storage added in the meantime',
    );
    @registered = ();
    @created = ();
    eval {
        PVE::API2::Disks::LVM->create({
            node => 'n1',
            name => 'other',
            device => '/dev/sdc',
            add_storage => 1,
        });
    };
    like($@, qr/already defined/, 'SAN volume group does not join the storage of another node');
    is_deeply(\@created, [], 'existing storage ID prevents initialization');
    $is_san = 0;
    PVE::API2::Disks::LVM->create({
        node => 'n1',
        name => 'vg5',
        device => '/dev/sdb',
        add_storage => 1,
    });
    is($params[-1]->{shared}, 0, 'local storage is not shared');
    is($params[-1]->{nodes}, 'n1', 'local storage remains node-restricted');
    is_deeply(\@registered, ['check', 'create or update'], 'and may join one of its name');
    @created = ();
    $disk->mock(assert_disk_unused => sub { die "device is already in use\n" });
    eval {
        PVE::API2::Disks::LVM->create({
            node => 'n1',
            name => 'vg',
            device => '/dev/sdc',
        });
    };
    like($@, qr/already in use/, 'disk API rejects a used inventory entry');
    is_deeply(\@created, [], 'used inventory prevents initialization');
    $disk->mock(assert_disk_unused => sub { });
    $lvm->mock(lvm_vgs => sub { { vg => {} } });
    eval {
        PVE::API2::Disks::LVM->create({
            node => 'n1',
            name => 'vg',
            device => '/dev/sdc',
        });
    };
    like(
        $@,
        qr/volume group with name 'vg' already exists/,
        'stale inventory does not permit an existing VG',
    );
    is_deeply(\@created, [], 'existing VG prevents initialization');
    my @mocked = qw(
        locked_disk_action verify_blockdev_path assert_disk_unused is_partition udevadm_trigger
    );
    $disk->unmock($_) for @mocked;
};

done_testing();
