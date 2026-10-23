#!/usr/bin/perl

use strict;
use warnings;

our ($backing, @opens);

BEGIN {
    *CORE::GLOBAL::sysopen = sub (*$$;$) {
        push @opens, $_[1];
        my $path = $_[1] =~ m{^/dev/disk/by-id/nvme-} ? $backing : $_[1];
        return CORE::sysopen($_[0], $path, $_[2]);
    };
}
use lib qw(..);
use File::Temp qw(tempfile);
use Test::More;
use Test::MockModule;
use PVE::Storage::SAN;

my ($fh, $file) = tempfile('san-nvme-XXXX', DIR => '.', UNLINK => 1);
close($fh);
$backing = $file;
my $device = '/dev/nvme0n1';
my $link = '/dev/disk/by-id/nvme-eui.0123456789abcdef';
my $lun =
    { devpath => $device, 'by-id' => $link, wwid => 'eui.0123456789abcdef', multipath => 'native' };
my $san = Test::MockModule->new('PVE::Storage::SAN');
my $disk = Test::MockModule->new('PVE::Diskmanage');
my $mp = Test::MockModule->new('PVE::Multipath');
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $storage = Test::MockModule->new('PVE::Storage');
my $cluster = Test::MockModule->new('PVE::Cluster');
my $tools = Test::MockModule->new('PVE::Tools');
my $cfg = { ids => {} };
my @order;
$storage->mock(config => sub { $cfg });
$disk->mock(verify_blockdev_path => sub { $device });
$disk->mock(is_partition => sub { 0 });
$disk->mock(get_nvme_transport => sub { 'fc' });
$disk->mock(read_subsysnqn => sub { 'nqn.test' });
$disk->mock(is_iscsi_device => sub { 0 });
$disk->mock(is_mounted => sub { 0 });
$disk->mock(has_holder => sub { 0 });
$disk->mock(device_opener => sub { undef });
$mp->mock(device_map => sub { undef });
$san->mock(abs_path => sub { $device });
$san->mock(scan_luns => sub { [$lun] });
$san->mock(lun_partitions => sub { [] });
$san->mock(dir_glob_foreach => sub { });
$san->mock(assert_same_device => sub { ok(defined(fileno($_[1])), 'held descriptor checked') });
$lvm->mock(lun_volume_groups => sub { push @order, 'check'; [] });
$cluster->mock(
    cfs_lock_domain => sub {
        is($_[0], 'san-eui.0123456789abcdef', 'native namespace uses the LUN lock');
        push @order, 'lock';
        $_[2]->();
        push @order, 'unlock';
        $@ = '';
    },
);
is(PVE::Storage::SAN::stable_path($device, $lun->{wwid}), $link, 'native stable path');
is_deeply(PVE::Storage::SAN::lun_for_device($link), $lun, 'native row selected for writes');
my $model_link = '/dev/disk/by-id/nvme-NetApp_ONTAP_Controller_81Mkq_1';
is_deeply(PVE::Storage::SAN::lun_for_device($model_link), $lun, 'any name of the namespace');
{
    # the name passes to another device while the LUNs are scanned
    my $scanned = 0;
    $san->mock(scan_luns => sub { $scanned = 1; [$lun] });
    $san->mock(abs_path => sub { $_[0] eq $model_link && $scanned ? '/dev/nvme9n1' : $device });
    eval { PVE::Storage::SAN::lun_for_device($model_link) };
    like($@, qr/the device behind '\Q$model_link\E' changed/, 'a name that changed refuses');
    $san->mock(scan_luns => sub { [$lun] });
    $san->mock(abs_path => sub { $device });
}
{
    $san->mock(scan_luns => sub { [] });
    eval { PVE::Storage::SAN::lun_for_device($link) };
    like($@, qr/use the device that the SAN LUN list shows/, 'a SAN device without a row refuses');
    $san->mock(scan_luns => sub { [$lun] });
}
$disk->mock(is_partition => sub { 1 });
is(PVE::Storage::SAN::lun_for_device($link), undef, 'a partition is handled as a local device');
$disk->mock(is_partition => sub { 0 });
for my $base (qw(fabric nvme-fabric)) {
    $cfg->{ids}->{data} = { type => 'lvm', base => "$base:nvme-eui.0123456789abcdef" };
    eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun) };
    like($@, qr/base volume/, "native base reference on storage '$base' refuses");
}
$cfg->{ids} =
    { fabric =>
        { type => 'nvmeof', subsysnqn => 'nqn.test', disable => 1, content => { images => 1 } } };
eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun) };
like(
    $@,
    qr/serves images from this subsystem/,
    'direct namespace image use refuses even when disabled',
);
$cfg->{ids}->{fabric}->{content} = { 'vm-vol' => 1 };
eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun) };
like($@, qr/serves images from this subsystem/, 'also when restricted to VM disks');
$cfg->{ids}->{fabric}->{content} = { none => 1 };
eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun) };
is($@, '', 'connection-only storage permits namespace initialization');

for my $nqn (undef, '') {
    $disk->mock(read_subsysnqn => sub { $nqn });
    eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun) };
    like($@, qr/cannot read its NVMe subsystem/, 'an unreadable subsystem refuses');
}
$disk->mock(read_subsysnqn => sub { 'nqn.test' });
$cfg->{ids} = {};
@order = @opens = ();
$tools->mock(file_get_contents => sub { 1024 });
$disk->mock(
    run_command => sub {
        my ($cmd, %opts) = @_;
        push @order, $cmd->[0];
        my ($path) = map { /^of=(.*)$/ ? $1 : m{^(/proc/\d+/fd/\d+)$} ? $1 : () } @$cmd;
        ok(defined($path) && -e $path, 'native wipe writes through the held descriptor');
    },
);
PVE::Storage::SAN::with_lun_lock($lun, sub { PVE::Storage::SAN::wipe_lun($lun) });
is_deeply(\@order, [qw(lock check wipefs dd unlock)], 'native wipe sequencing');
is_deeply(\@opens, [$link, $link], 'native wipe opens its by-id name');

@order = @opens = ();
$lvm->mock(file_read_firstline => sub { $_[0] =~ m{/size$} ? 1024 : undef });
$lvm->mock(lvm_pv_info => sub { undef });
$lvm->mock(lvm_vgs => sub { {} });
$lvm->mock(
    clear_first_sector => sub { push @order, 'clear'; ok(-e $_[0], 'VG clear uses held fd') });
$lvm->mock(
    run_command => sub {
        my ($cmd, %opts) = @_;
        if ($cmd->[0] eq 'wipefs') {
            $opts{outfunc}->('{"signatures":[]}');
        } else {
            is($cmd->[-1], $device, 'VG command uses the checked device');
            push @order, $cmd->[0];
        }
    },
);
PVE::Storage::LVMPlugin::lvm_create_volume_group($link, 'data', 1);
is_deeply(
    \@order,
    [qw(lock check clear /sbin/pvcreate /sbin/vgcreate unlock)],
    'native VG sequencing',
);
is_deeply(\@opens, [$link], 'VG creation holds the native by-id device');

done_testing();
