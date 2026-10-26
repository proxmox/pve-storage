#!/usr/bin/perl

use strict;
use warnings;

use Digest::SHA qw(sha256_hex);
use Errno qw(EACCES ENOSPC);
use Fcntl qw(LOCK_EX LOCK_NB);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

my $dir = tempdir(CLEANUP => 1);
local $PVE::Storage::ISCSIPlugin::ISCSI_RESCAN_DIR = $dir;
my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
my @rescans;
my $target;
$plugin->mock(
    iscsi_session =>
        sub { [{ session_id => 1, portal => 'portal' }, { session_id => 2, portal => 'portal' }] },
    iscsi_portals => sub { ['portal'] },
    run_command => sub {
        my ($cmd) = @_;
        push @rescans, $cmd->[-2];
        is($cmd->[-1], '--rescan', 'only session rescans run');
        my $filename = "$dir/pve-iscsi-rescan-" . sha256_hex($target);
        open(my $fh, '>>', $filename) or die $!;
        ok(flock($fh, LOCK_EX | LOCK_NB), 'timestamp lock is released before rescan');
    },
);

my sub activate {
    my ($storeid, $name) = @_;
    $target = $name;
    PVE::Storage::ISCSIPlugin->activate_storage($storeid, { target => $target }, {});
}

activate('first', 'iqn.first');
is_deeply(\@rescans, [1, 2], 'all sessions of first target rescanned');
activate('second', 'iqn.second/../unsafe');
is_deeply(\@rescans, [1, 2, 1, 2], 'second target has its own window and a safe filename');
activate('duplicate', 'iqn.first');
is(scalar(@rescans), 4, 'duplicate storage shares the target allowance');
activate('second', 'iqn.second/../unsafe');
is(scalar(@rescans), 4, 'independent activation cache does not bypass persisted claim');
my $filename = "$dir/pve-iscsi-rescan-" . sha256_hex('iqn.first');
my $inode = (stat($filename))[1];
open(my $stamp, '>', $filename) or die $!;
print $stamp (time() - 11) . "\n";
close($stamp) or die $!;
activate('duplicate', 'iqn.first');
is_deeply(\@rescans, [1, 2, 1, 2, 1, 2], 'next window rescans every session again');
activate('second', 'iqn.second/../unsafe');
is(scalar(@rescans), 6, 'other target remains rate limited');

is((stat($filename))[1], $inode, 'timestamp updates do not replace the locked inode');
my @files = glob("$dir/*");
is(scalar(@files), 2, 'exactly one timestamp and lock file per target');

my $io = Test::MockModule->new('IO::File');
my $new = IO::File->can('new');
my $print = IO::File->can('print');
my $close = IO::File->can('close');
my $failure = '';
$io->mock(
    new => sub {
        if ($failure eq 'open') { $! = EACCES; return undef; }
        $new->(@_);
    },
    print => sub {
        if ($failure eq 'update') { $! = ENOSPC; return 0; }
        $print->(@_);
    },
    close => sub {
        my $res = $close->(@_);
        if ($failure eq 'close') { $! = ENOSPC; return 0; }
        return $res;
    },
);
for my $case (qw(open update close lock)) {
    my $name = "iqn.failure-$case";
    my $file = "$dir/pve-iscsi-rescan-" . sha256_hex($name);
    my $lock;
    if ($case eq 'lock') {
        open($lock, '>>', $file) or die $!;
        flock($lock, LOCK_EX | LOCK_NB) or die $!;
    }
    $failure = $case;
    my $warnings = '';
    local $SIG{__WARN__} = sub { $warnings .= $_[0] };
    my $before = scalar(@rescans);
    eval { activate('failure', $name) };
    is($@, '', "$case failure does not fail activation");
    if ($case eq 'lock') {
        is($warnings, '', 'a concurrent claim skips the rescan silently');
    } else {
        like(
            $warnings,
            qr/^skipping iSCSI rescan of target '\Q$name\E' - /,
            "$case failure identifies best-effort bookkeeping",
        );
    }
    is(scalar(@rescans), $before, "$case failure skips the rescan");
    close($lock) if $lock;
}
$failure = '';
activate('recovered', 'iqn.failure-update');
is(scalar(@rescans), 8, 'empty timestamp after failed update permits a subsequent first claim');

my $partial = "$dir/pve-iscsi-rescan-" . sha256_hex('iqn.partial');
open(my $partial_fh, '>', $partial) or die $!;
print $partial_fh '17';
close($partial_fh) or die $!;
activate('partial', 'iqn.partial');
is(scalar(@rescans), 10, 'an incomplete timestamp counts as none');

done_testing();
