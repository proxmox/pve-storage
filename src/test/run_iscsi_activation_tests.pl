#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
my $identity = Test::MockModule->new('PVE::Multipath::Identity');
my $mp = Test::MockModule->new('PVE::Multipath');
my $cwd = Test::MockModule->new('Cwd');
my ($resolved, $waits, $ready, $visible) = (undef, 0, 0, undef);
$identity->mock(
    collect => sub {
        die "unexpected identity query\n" if !defined($visible);
        return { paths => { map { $_ => { wwid => '3600a' } } @$visible } };
    },
);
$mp->mock(
    wait_for_map => sub {
        $waits++;
        is($_[0], '3600a', 'map name supplies the WWID');
        return $ready;
    },
);
$cwd->mock(realpath => sub { $resolved });
$plugin->mock(
    filesystem_path => sub { '/dev/disk/by-id/test' },
    file_read_firstline => sub { 'iqn.test' },
    run_command => sub {
        my ($cmd, %opts) = @_;
        is($cmd->[0], 'udevadm', 'normal activation validates target membership');
        $opts{outfunc}->('/devices/platform/host1/session1/target1:0:0/1:0:0:1/block/sda');
    },
);

for my $multipath (0, 1) {
    for my $case ([undef, qr/failed to get realpath/], ['/dev/null', qr/not a block device/]) {
        $resolved = $case->[0];
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        eval {
            PVE::Storage::ISCSIPlugin->activate_volume(
                'san',
                { target => 'iqn.test', 'managed-multipath' => $multipath },
                '0.0.1.scsi-legacy',
            );
        };
        like($@, $case->[1], "plain name uses normal validation with multipath=$multipath");
        is($warnings, '', 'no compatibility warning');
        is($waits, 0, 'no wait for a plain name');
    }
}

my $scfg = { target => 'iqn.test', 'managed-multipath' => 1 };
$visible = [];
eval { PVE::Storage::ISCSIPlugin->activate_volume('san', $scfg, '0.0.1.dm-uuid-mpath-3600a') };
like($@, qr/is not visible on this node/, 'no wait for a LUN without a path on the node');
is($waits, 0, 'absent LUN fails at once');

$visible = ['sda', 'sdb'];
eval { PVE::Storage::ISCSIPlugin->activate_volume('san', $scfg, '0.0.1.dm-uuid-mpath-3600a') };
like($@, qr/did not assemble in time/, 'map-named timeout is fatal');
is($waits, 1, 'map-named volume waits once');

eval {
    PVE::Storage::ISCSIPlugin->activate_volume(
        'san', $scfg, '0.0.1.dm-uuid-mpath-3600a', undef, {},
    );
};
like($@, qr/did not assemble in time/, 'an activation with a cache waits as well');
$waits = 1;

# the mocked identity query dies without a list of visible paths
$visible = undef;
eval {
    PVE::Storage::ISCSIPlugin->activate_volume(
        'san',
        $scfg,
        '0.0.1.dm-uuid-mpath-3600a',
        undef,
        { status_only => 1 },
    );
};
like($@, qr/is not assembled/, 'a status pass fails for a missing map');
is($waits, 1, 'without a wait and without asking for the paths of the LUN');
$visible = ['sda', 'sdb'];

$ready = 1;
$resolved = undef;
eval { PVE::Storage::ISCSIPlugin->activate_volume('san', $scfg, '0.0.1.dm-uuid-mpath-3600a') };
like($@, qr/failed to get realpath/, 'map link is resolved after waiting');
is($waits, 2, 'one additional wait');

SKIP: {
    my ($blockdev) = grep { -b $_ } map { s!^/sys/block/!/dev/!r } glob('/sys/block/*');
    skip 'block device metadata required for successful activation', 3 if !$blockdev;
    $resolved = $blockdev;
    my $before = $waits;
    eval { PVE::Storage::ISCSIPlugin->activate_volume('san', $scfg, '0.0.1.scsi-legacy') };
    is($@, '', 'plain name activates through the normal target check');
    is($waits, $before, 'successful plain activation also has no map wait');
}

done_testing();
