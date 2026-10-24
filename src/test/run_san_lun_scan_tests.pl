#!/usr/bin/perl

use strict;
use warnings;
use lib qw(..);
use Test::More;
use Test::MockModule;

use PVE::Storage::SAN;
use PVE::API2::SAN::Node;

my $disk = Test::MockModule->new('PVE::Diskmanage');
my $mp = Test::MockModule->new('PVE::Multipath');
my $id = Test::MockModule->new('PVE::Multipath::Identity');
my $config = Test::MockModule->new('PVE::Multipath::Config');
my $storage = Test::MockModule->new('PVE::Storage');
my $iscsi = Test::MockModule->new('PVE::ISCSI::Topology');
my $scan = Test::MockModule->new('PVE::Storage::SAN');
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $hints;
$lvm->mock(file_get_contents => sub { $hints // die "No such file\n" });
$lvm->mock(abs_path => sub { $_[0] });
$lvm->mock(run_command => sub { die "unexpected LVM device probe\n" });

my $disks = {
    sda => { devpath => '/dev/sda', size => 100, transport => 'sata' },
    sdb => { devpath => '/dev/sdb', size => 200, transport => 'iscsi' },
    sdc => { devpath => '/dev/sdc', size => 200, transport => 'iscsi', used => 'LVM2_member' },
    sdd =>
        { devpath => '/dev/sdd', size => 300, transport => 'fc', multipath => '/dev/mapper/lun' },
    sde => { devpath => '/dev/sde', size => 400, transport => 'fc' },
};
my $maps = {
    'dm-0' => {
        devpath => '/dev/mapper/lun',
        size => 300,
        wwn => '3600map',
        paths => 2,
        slaves => ['/dev/sdd', '/dev/sdf'],
        transport => 'fc',
    },
};
$id->mock(collect => sub { {} });
$id->mock(path_wwid => sub { $_[1] =~ /^(sdb|sdc)$/ ? '3600plain' : undef });
$config->mock(read_config => sub { { ids => { '3600map' => { type => 'wwid' } } } });
my $generator = Test::MockModule->new('PVE::Multipath::Generator');
$generator->mock(derived_wwids => sub { {} });
$iscsi->mock(
    device_map => sub {
        return { map { $_ => { target => 'iqn.test', lun => 1, portal => '192.0.2.10:3260' } }
            qw(sdb sdc) };
    },
);
my @listed;
$scan->redefine(
    dir_glob_foreach => sub {
        my ($dir, $regex, $code) = @_;
        return PVE::Tools::dir_glob_foreach($dir, $regex, $code) if $dir ne '/sys/block';
        $code->($_) for grep { /^$regex$/ } qw(sda sdb sdc sdd sde sdf dm-0 loop0 nvme0n1);
    },
);
my ($openers, @opener_query) = ({});
$disk->mock(
    device_openers => sub { @opener_query = @_; return $openers },
    get_disks => sub {
        ok($_[3], 'SAN inventory opts into iSCSI');
        @listed = sort $_[0]->@*;
        return $disks;
    },
    get_multipath_disks => sub { $maps },
    get_nvme_path_status => sub { {} },
    run_command => sub { die 'unexpected device probe' },
);
$mp->mock(list_etc_multipath_wwids => sub { [] });
$mp->mock(
    map_info => sub {
        return {
            map {
                $_ => {
                    wwid => $maps->{$_}->{wwn},
                    slaves => [map { s{^/dev/}{}r } $maps->{$_}->{slaves}->@*],
                }
            } keys %$maps
        };
    },
);
$mp->mock(
    path_status_by_wwid => sub {
        return {
            '3600map' => {
                active => 1,
                paths => [
                    { device => '/dev/sdd', state => 'active', 'checker-state' => 'ghost' },
                    { device => '/dev/sdf', state => 'failed', 'checker-state' => 'faulty' },
                ],
            },
        };
    },
);
$scan->mock(abs_path => sub { $_[0] });
$scan->mock(stable_path => sub { '/dev/disk/by-id/scsi-' . $_[1] });
my %partitions;
$scan->mock(lun_partitions => sub { [($partitions{ $_[0]->{devpath} } // [])->@*] });
my %device_state;
$scan->mock(
    device_state => sub { exists($device_state{ $_[0] }) ? $device_state{ $_[0] } : 'running' },
);
my $cfg =
    { ids => { direct => { type => 'iscsi', target => 'iqn.test', content => { images => 1 } } } };
my $rows = PVE::Storage::SAN::scan_luns($cfg);
is(scalar(@$rows), 2, 'only identified SAN LUNs, one row per identity');
is_deeply(
    \@listed,
    [qw(nvme0n1 sda sdb sdc sde)],
    'the paths of a map are left out of the disk list',
);
my %row = map { $_->{wwid} => $_ } @$rows;
is($row{'3600plain'}->{usage}, 'lvm', 'positive usage from any path survives folding');
is($row{'3600plain'}->{'used-by-storage'}, 'direct', 'a storage serving the LUNs uses them');
{
    local $cfg->{ids}->{direct}->{content} = { none => 1 };
    my %plain = map { $_->{wwid} => $_ } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($plain{'3600plain'}->{'used-by-storage'}, undef, 'a connection storage does not');
}
{
    # a process has a map open by the device it resolves to
    $scan->mock(abs_path => sub { $_[0] eq '/dev/mapper/lun' ? '/dev/dm-0' : $_[0] });
    my $cgroup = '0::/qemu.slice/100.scope';
    $scan->mock(
        file_read_firstline => sub {
            return $_[0] eq '/proc/4242/cgroup' ? $cgroup : PVE::Tools::file_read_firstline(@_);
        },
    );
    local $openers->{'/dev/dm-0'} = { pid => 4242, command => 'kvm' };
    my %open = map { $_->{wwid} => $_ } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($open{'3600map'}->{'used-by-process'}, '4242 (kvm)', 'a process with the map open');
    is($open{'3600map'}->{'used-by-guest'}, 100, 'that belongs to a virtual machine');
    ok(!exists($open{'3600plain'}->{'used-by-process'}), 'no process for the other LUN');
    my ($devices, %param) = @opener_query;
    is_deeply(
        [sort @$devices],
        [qw(/dev/mapper/lun /dev/sdb /dev/sdc /dev/sdd /dev/sdf)],
        'asked once for all LUNs and their paths',
    );
    ok($param{writable}, 'a process that only reads a LUN does not use it');
    {
        local $openers->{'/dev/sdc'} = { pid => 4243, command => 'kvm' };
        local $openers->{'/dev/sdf'} = { pid => 4244, command => 'kvm' };
        my %path = map { $_->{wwid} => $_ } PVE::Storage::SAN::scan_luns($cfg)->@*;
        is($path{'3600plain'}->{'used-by-process'}, '4243 (kvm)', 'a process with another path');
        is($path{'3600map'}->{'used-by-process'}, '4242 (kvm)', 'the map before its paths');
    }
    {
        # a kernel partition of a path, one that kpartx maps on the map, and one of a member
        local $partitions{'/dev/sdc'} = ['/dev/sdc1'];
        local $partitions{'/dev/mapper/lun'} = ['/dev/dm-5'];
        local $partitions{'/dev/sdf'} = ['/dev/sdf1'];
        delete local $openers->{'/dev/dm-0'};
        local $openers->{'/dev/sdc1'} = { pid => 4242, command => 'kvm' };
        local $openers->{'/dev/dm-5'} = { pid => 4242, command => 'kvm' };
        my %part = map { $_->{wwid} => $_ } PVE::Storage::SAN::scan_luns($cfg)->@*;
        is($part{'3600plain'}->{'used-by-guest'}, 100, 'a guest that writes to a partition');
        is($part{'3600map'}->{'used-by-guest'}, 100, 'also to one that kpartx maps');
        ($devices) = @opener_query;
        is_deeply(
            [sort @$devices],
            [qw(/dev/dm-5 /dev/mapper/lun /dev/sdb /dev/sdc /dev/sdc1 /dev/sdd /dev/sdf /dev/sdf1)],
            'the partitions in the same query',
        );
    }
    my $schema = PVE::API2::SAN::Node->map_method_by_name('luns')->{returns};
    eval { PVE::JSONSchema::validate([values %open], $schema) };
    is($@, '', 'rows with a process match the public schema');

    $cgroup = '0::/system.slice/100.scope';
    ($open{'3600map'}) = grep { $_->{wwid} eq '3600map' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($open{'3600map'}->{'used-by-process'}, '4242 (kvm)', 'a process of no guest');
    ok(!exists($open{'3600map'}->{'used-by-guest'}), 'has no guest ID');
    $scan->unmock('file_read_firstline');
    $scan->mock(abs_path => sub { $_[0] });
}
is_deeply(
    $row{'3600plain'}->{'path-list'},
    [
        map { { device => $_, state => 'running', address => '192.0.2.10:3260' } }
            qw(/dev/sdb /dev/sdc)
    ],
    'without a map, the devices of a LUN are its paths, with the portal of their session',
);
is($row{'3600plain'}->{state}, 'optimal', 'their kernel state gives the health of the LUN');
ok(!exists($row{'3600map'}->{'path-list'}->[0]->{address}), 'no portal without a session');
{
    local $device_state{sdc} = 'transport-offline';
    my ($down) = grep { $_->{wwid} eq '3600plain' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($down->{'paths-active'}, 1, 'only a running device takes I/O');
    is($down->{state}, 'degraded', 'a LUN with a device down is degraded');
    $device_state{sdc} = undef;
    ($down) = grep { $_->{wwid} eq '3600plain' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($down->{'path-list'}->[1]->{state}, 'unknown', 'a device without a state');
    is($down->{'paths-active'}, 1, 'does not count as running');
}
{
    # a LUN whose requested map is missing
    $mp->mock(list_etc_multipath_wwids => sub { ['3600plain'] });
    my ($pending) = grep { $_->{wwid} eq '3600plain' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($pending->{multipath}, 'pending', 'waits for its map');
    is(scalar($pending->{'path-list'}->@*), 2, 'lists its devices');
    ok(!exists($pending->{state}), 'but has no state from them');
    $mp->mock(list_etc_multipath_wwids => sub { [] });
}
{
    # an iSCSI path of a map
    my $sessions = PVE::ISCSI::Topology::device_map();
    $iscsi->mock(device_map => sub { +{ %$sessions, sdd => { %{ $sessions->{sdb} } } } });
    my ($map) = grep { $_->{wwid} eq '3600map' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($map->{'path-list'}->[0]->{address}, '192.0.2.10:3260', 'names the portal of its session');
    $iscsi->mock(device_map => sub { $sessions });
}
{
    # a LUN with a single path and no map
    $id->mock(path_wwid => sub { $_[1] eq 'sdb' ? '3600plain' : undef });
    my ($single) = grep { $_->{wwid} eq '3600plain' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($single->{'paths-total'}, 1, 'one device, one path');
    is($single->{state}, 'optimal', 'which the web UI shows as OK with one path');
    local $device_state{sdb} = 'blocked';
    ($single) = grep { $_->{wwid} eq '3600plain' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($single->{state}, 'failed', 'a blocked device takes no I/O');
    $id->mock(path_wwid => sub { $_[1] =~ /^(sdb|sdc)$/ ? '3600plain' : undef });
}
is($row{'3600map'}->{state}, 'degraded', 'final state counts usable paths');
is($row{'3600map'}->{multipath}, 'manual', 'explicit map');
is($row{'3600map'}->{devpath}, '/dev/mapper/lun', 'map is the selected device');
my $schema = PVE::API2::SAN::Node->map_method_by_name('luns')->{returns};
eval { PVE::JSONSchema::validate($rows, $schema) };
is($@, '', 'rows match the public schema');
is($row{'3600map'}->{name}, 'lun', 'map alias is exposed');
$maps->{'dm-0'}->{devpath} = '/dev/mapper/3600map';
$rows = PVE::Storage::SAN::scan_luns($cfg);
my ($unnamed) = grep { $_->{wwid} eq '3600map' } @$rows;
ok(!exists($unnamed->{name}), 'WWID is not repeated as an alias');
$maps->{'dm-0'}->{devpath} = '/dev/mapper/lun';

{
    my $healthy = {
        '3600map' => {
            active => 2,
            paths => [map { { device => $_, state => 'active' } } qw(/dev/sdd /dev/sdf)],
        },
    };
    $mp->mock(path_status_by_wwid => sub { $healthy });
    my ($map) = grep { $_->{wwid} eq '3600map' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($map->{state}, 'optimal', 'all paths usable without an expected count');
    $config->mock(
        read_config =>
            sub { { ids => { '3600map' => { type => 'wwid', 'paths-expected' => 3 } } } },
    );
    ($map) = grep { $_->{wwid} eq '3600map' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is($map->{'paths-expected'}, 3, 'expected count of the entry');
    is($map->{state}, 'degraded', 'fewer paths than the entry expects');
    $config->mock(read_config => sub { { ids => { '3600map' => { type => 'wwid' } } } });
}

$mp->mock(path_status_by_wwid => sub { {} });
$rows = PVE::Storage::SAN::scan_luns($cfg);
my ($unobserved) = grep { $_->{wwid} eq '3600map' } @$rows;
is($unobserved->{'paths-total'}, 2, 'map membership supplies total without daemon status');
ok(!exists($unobserved->{'paths-active'}), 'no invented active count');
ok(!exists($unobserved->{state}), 'no false failed state');

$cfg->{ids}->{data} = { type => 'lvm', vgname => 'data' };
$hints = "hints_version: 2.1\nscan:/dev/mapper/lun pvid:abc devn:253:0 vg:data\n";
$rows = PVE::Storage::SAN::scan_luns($cfg);
my ($cached) = grep { $_->{wwid} eq '3600map' } @$rows;
is($cached->{vgname}, 'data', 'hints supply the display VG');
is($cached->{usage}, 'lvm', 'hints also supply cached usage evidence');
is($cached->{'used-by-storage'}, 'data', 'VG allows storage attribution');
for my $contents (undef, '', "scan=/dev/mapper/lun vg=data\n") {
    $hints = $contents;
    $rows = PVE::Storage::SAN::scan_luns($cfg);
    is(scalar(@$rows), 2, 'missing or unrecognized hints do not lose LUNs');
    my ($uncached) = grep { $_->{wwid} eq '3600map' } @$rows;
    ok(!exists($uncached->{vgname}), 'VG name is absent without a hint');
    ok(!exists($uncached->{'used-by-storage'}), 'VG-derived attribution is also absent');
    is($uncached->{usage}, 'unused', 'usage falls back to other cached evidence');
}
for my $base (qw(direct scsi-direct)) {
    # a base volume by the name that an iSCSI storage gives it without multipath
    local $cfg->{ids}->{legacy} =
        { type => 'lvm', vgname => 'legacy', base => "$base:0.0.1.scsi-3600map" };
    my ($based) = grep { $_->{wwid} eq '3600map' } PVE::Storage::SAN::scan_luns($cfg)->@*;
    is(
        $based->{'used-by-storage'},
        'legacy',
        "the storage of a SCSI base volume on storage '$base', without hints",
    );
}

$disks->{sdc}->{used} = 'ext4';
$maps = {};
$rows = PVE::Storage::SAN::scan_luns($cfg);
is($rows->[0]->{usage}, 'other', 'foreign data from a sibling path is retained');
is($rows->[0]->{detail}, 'ext4', 'signature detail');
for my $order ([qw(sdb sdc)], [qw(sdc sdb)]) {
    my ($member, $data) = @$order;
    local $disks->{$member}->{used} = 'mpath_member';
    local $disks->{$data}->{used} = 'ext4';
    $rows = PVE::Storage::SAN::scan_luns($cfg);
    is($rows->[0]->{detail}, 'ext4', "a path claimed by multipath ($member) does not hide data");
}
{
    local $disks->{sdb}->{used} = 'mpath_member';
    local $disks->{sdc}->{used} = 'mpath_member';
    $rows = PVE::Storage::SAN::scan_luns($cfg);
    is($rows->[0]->{usage}, 'unused', 'paths claimed by multipath are no use of the LUN');
    ok(!exists($rows->[0]->{detail}), 'and give no signature detail');
    local $partitions{'/dev/sdc'} = ['/dev/sdc1'];
    $rows = PVE::Storage::SAN::scan_luns($cfg);
    is_deeply(
        [$rows->[0]->@{qw(usage detail)}],
        ['other', 'partitions'],
        'but the partitions that one of them keeps are',
    );
}
$scan->mock(stable_path => sub { $_[0] eq '/dev/sdc' ? '/dev/disk/by-id/scsi-3600plain' : undef });
$rows = PVE::Storage::SAN::scan_luns($cfg);
is($rows->[0]->{'by-id'}, '/dev/disk/by-id/scsi-3600plain', 'by-id link of the second path');
is($rows->[0]->{devpath}, '/dev/sdc', 'the row names the path that its by-id link resolves to');

$disks = { nvme0n1 => { devpath => '/dev/nvme0n1', size => 100, transport => 'nvme-fc' } };
$scan->mock(file_read_firstline => sub { $_[0] =~ m{/nsid$} ? 1 : 'eui.0123456789abcdef' });
$disk->mock(read_subsysnqn => sub { 'nqn.test' });
$cfg->{ids}->{fabric} = {
    type => 'nvmeof',
    subsysnqn => 'nqn.test',
    portals => '192.0.2.1,192.0.2.2',
    content => { images => 1 },
};
$scan->unmock('stable_path');
$scan->mock(abs_path => sub { '/dev/nvme0n1' });
$rows = PVE::Storage::SAN::scan_luns($cfg);
is($rows->[0]->{multipath}, 'none', 'NVMe without native multipath paths');
$disk->mock(
    get_nvme_path_status => sub {
        { nvme0n1 =>
            { active => 1, paths => [{ device => '/dev/nvme0c0n1', state => 'live' }] } };
    },
);
$rows = PVE::Storage::SAN::scan_luns($cfg);
is($rows->[0]->{multipath}, 'native', 'NVMe uses kernel-native multipath');
is($rows->[0]->{'by-id'}, '/dev/disk/by-id/nvme-eui.0123456789abcdef', 'NVMe stable name');
eval { PVE::JSONSchema::validate($rows, $schema) };
is($@, '', 'native row matches the API schema');
is($rows->[0]->{'nvme-subsystem'}, 'nqn.test', 'subsystem NQN exposed');
is($rows->[0]->{'nvme-nsid'}, 1, 'namespace ID exposed');
is($rows->[0]->{'used-by-storage'}, 'fabric', 'a storage serving the namespaces uses them');
is($rows->[0]->{'paths-expected'}, 2, 'one expected path per configured portal');

for my $count (1, 3) {
    $config->mock(
        read_config => sub { {
            ids => { 'eui.0123456789abcdef' => { type => 'wwid', 'paths-expected' => $count } },
        } },
    );
    my $entry = PVE::Storage::SAN::scan_luns($cfg);
    is($entry->[0]->{'paths-expected'}, 2, "a multipath entry does not change it ($count)");
}
$config->mock(read_config => sub { { ids => { '3600map' => { type => 'wwid' } } } });
{
    local $cfg->{ids}->{fabric}->{content} = { none => 1 };
    my $plain = PVE::Storage::SAN::scan_luns($cfg);
    is($plain->[0]->{'used-by-storage'}, undef, 'a connection storage does not');
    is($plain->[0]->{'paths-expected'}, 2, 'but still tells the expected paths');
}
{
    local $cfg->{ids}->{fabric}->{disable} = 1;
    my $disabled = PVE::Storage::SAN::scan_luns($cfg);
    is($disabled->[0]->{'paths-expected'}, undef, 'a disabled storage expects no paths');
    is($disabled->[0]->{'used-by-storage'}, 'fabric', 'but still uses the namespaces');
}
{
    local $cfg->{ids}->{fabric}->{nodes} = { other => 1 };
    my $elsewhere = PVE::Storage::SAN::scan_luns($cfg);
    is($elsewhere->[0]->{'paths-expected'}, undef, 'nor a storage of other nodes');
}
for my $base (qw(fabric nvme-fabric)) {
    $cfg->{ids}->{data}->{base} = "$base:nvme-eui.0123456789abcdef";
    $rows = PVE::Storage::SAN::scan_luns($cfg);
    is($rows->[0]->{'used-by-storage'}, 'data', "LVM base consumer takes precedence ($base)");
}

$disk->mock(is_partition => sub { 1 });
ok(!PVE::Storage::SAN::is_san_device('/dev/sdb1'), 'a partition is never a LUN');
$disk->mock(is_partition => sub { 0 });
my $mp_module = Test::MockModule->new('PVE::Multipath');
$mp_module->redefine(device_map => sub { { wwid => '3600a', partition => 1 } });
ok(!PVE::Storage::SAN::is_san_device('/dev/dm-5'), 'neither is one that kpartx maps');
$mp_module->redefine(device_map => sub { { wwid => '3600a' } });
ok(PVE::Storage::SAN::is_san_device('/dev/dm-4'), 'while the map itself is');

done_testing();
