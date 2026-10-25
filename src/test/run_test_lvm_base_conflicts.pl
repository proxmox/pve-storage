#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage;
use PVE::Storage::LVMPlugin;

my $storage = Test::MockModule->new('PVE::Storage');
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my ($cfg, $created, $existing);
$storage->mock(
    config => sub { $cfg },
    path => sub { '/dev/sda' },
    activate_storage => sub { },
    activate_volumes => sub { },
);
$lvm->mock(
    lvm_create_volume_group => sub {
        my ($device, $vgname, $shared, $guard) = @_;
        return if $existing;
        $guard->{check_storage}->($device);
        $created++;
    },
    assert_discard_supported => sub { },
);

for my $case (
    ['iscsi', {}, 0],
    ['iscsi', { images => 1 }, 1],
    ['iscsidirect', {}, 1],
    ['zfs', {}, 1],
    ['vendor', { images => 1 }, 1],
) {
    my ($type, $content, $conflict) = @$case;
    $created = 0;
    $cfg = {
        ids => {
            base => { type => 'iscsi', target => 'iqn.test', content => { none => 1 } },
            other => {
                type => $type,
                target => 'iqn.test',
                content => $content,
                disable => 1,
                nodes => { elsewhere => 1 },
            },
        },
    };
    eval {
        PVE::Storage::LVMPlugin->on_add_hook(
            'test',
            { base => 'base:0.0.1.scsi-test', vgname => 'test' },
        );
    };
    if ($conflict) {
        like($@, qr/storage 'other' serves disk images from this target/, "$type conflict refused");
        like($@, qr/set its content to 'none'/, "$type: remedy named for iSCSI only")
            if $type eq 'iscsi';
    } else {
        is($@, '', 'connection-only storage permits initialization');
    }
    is($created, $conflict ? 0 : 1, 'no writes on conflicting target');

    $existing = 1;
    eval {
        PVE::Storage::LVMPlugin->on_add_hook(
            'test',
            { base => 'base:0.0.1.scsi-test', vgname => 'test' },
        );
    };
    is($@, '', "$type: an existing volume group on the base volume can be added");
    $existing = 0;
}

done_testing();
