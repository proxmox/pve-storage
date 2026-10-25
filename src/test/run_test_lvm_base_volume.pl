#!/usr/bin/perl

# Adding an LVM storage on top of an iSCSI base volume initializes that volume as a physical volume,
# so the target membership check done by activating the volume has to happen first.

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage;
use PVE::Storage::LVMPlugin;

my (@calls, $volume_error);

my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(
    config => sub { {} },
    storage_config => sub { { type => 'iscsi' } },
    path => sub { '/dev/disk/by-id/scsi-test' },
    activate_storage => sub { push @calls, 'activate_storage' },
    activate_volumes => sub {
        my ($cfg, $vollist) = @_;
        push @calls, "activate_volumes @$vollist";
        die $volume_error if $volume_error;
    },
);

my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm->mock(lvm_create_volume_group => sub { push @calls, 'lvm_create_volume_group' });

my $scfg = { base => 'san:0.0.0.scsi-test', vgname => 'vg', shared => 1 };

PVE::Storage::LVMPlugin->on_add_hook('lvm', $scfg);
is_deeply(
    \@calls,
    ['activate_storage', 'activate_volumes san:0.0.0.scsi-test', 'lvm_create_volume_group'],
    'base volume is activated before creating the volume group',
);

@calls = ();
$volume_error = "volume '0.0.0.scsi-test' not part of target 'iqn.test'\n";
eval { PVE::Storage::LVMPlugin->on_add_hook('lvm', $scfg) };
is($@, $volume_error, 'failing base volume activation aborts adding the storage');
ok(!grep({ $_ eq 'lvm_create_volume_group' } @calls), 'no volume group created on that device');

done_testing();
