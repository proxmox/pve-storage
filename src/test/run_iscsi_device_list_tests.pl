#!/usr/bin/perl

use strict;
use warnings;

use File::Path qw(make_path);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

my $root = tempdir(CLEANUP => 1);
local $PVE::Storage::ISCSIPlugin::SYSFS = "$root/sys";

my sub put {
    my ($path, $value) = @_;
    my ($dir) = "$root/sys/$path" =~ m{^(.*)/[^/]+$};
    make_path($dir);
    open(my $fh, '>', "$root/sys/$path") or die $!;
    print $fh "$value\n";
    close($fh) or die $!;
}

# The sysfs layout of the kernel's iSCSI transport class: the SCSI target and its LUNs are children
# of the session device, and the SCSI bus lists the LUNs by H:C:T:L.
my sub session {
    my ($sid, $host, $target, @luns) = @_;
    my $sessiondir = "devices/platform/host$host/session$sid";
    make_path("$root/sys/$sessiondir");
    put("class/iscsi_session/session$sid/targetname", $target);
    symlink("$root/sys/$sessiondir", "$root/sys/class/iscsi_session/session$sid/device")
        or die $!;
    for my $lun (@luns) {
        my ($hctl, $dev) = @$lun;
        my ($tdir) = $hctl =~ m/^(\d+:\d+:\d+):\d+$/;
        my $devdir = "$sessiondir/target$tdir/$hctl";
        put("$devdir/type", '0');
        make_path("$root/sys/$devdir/block/$dev");
        make_path("$root/sys/bus/scsi/devices");
        symlink("$root/sys/$devdir", "$root/sys/bus/scsi/devices/$hctl") or die $!;
        put("block/$dev/size", '2048');
        make_path("$root/sys/block/$dev/holders");
    }
}

# An offload initiator with two sessions on one SCSI host, a software initiator session on a host
# of its own, and a session without LUNs.
session(1, 7, 'iqn.target-a', ['7:0:0:1', 'sda']);
session(2, 7, 'iqn.target-b', ['7:0:1:1', 'sdb'], ['7:0:1:2', 'sdc']);
session(3, 8, 'iqn.target-c', ['8:0:0:0', 'sdd']);
session(4, 9, 'iqn.target-d');

my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
$plugin->mock(
    load_stable_scsi_paths => sub {
        return { map { $_ => "scsi-3$_" } qw(sda sdb sdc sdd) };
    },
);

my $list = PVE::Storage::ISCSIPlugin::iscsi_device_list();
is_deeply(
    { map { $_ => [sort keys $list->{$_}->%*] } keys %$list },
    {
        'iqn.target-a' => ['0.0.1.scsi-3sda'],
        'iqn.target-b' => ['0.1.1.scsi-3sdb', '0.1.2.scsi-3sdc'],
        'iqn.target-c' => ['0.0.0.scsi-3sdd'],
    },
    'each target lists the LUNs of its own session, also on a shared SCSI host',
);
is_deeply(
    $list->{'iqn.target-b'}->{'0.1.2.scsi-3sdc'},
    { format => 'raw', size => 2048 * 512, vmid => 0, channel => 0, id => 1, lun => 2 },
    'LUN address and size',
);

done_testing();
