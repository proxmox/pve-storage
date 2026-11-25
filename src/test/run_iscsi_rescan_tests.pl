#!/usr/bin/perl

use strict;
use warnings;

use Digest::SHA qw(sha256_hex);
use Errno qw(EACCES EINVAL EIO ENOSPC);
use Fcntl qw(LOCK_EX LOCK_NB);
use File::Path qw(make_path);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Storage::ISCSIPlugin;

my $dir = tempdir(CLEANUP => 1);
local $PVE::Storage::ISCSIPlugin::ISCSI_RESCAN_DIR = $dir;
my $plugin = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
my @scans;
my $target;
my $sessions = [
    { session_id => 1, portal => 'portal' }, { session_id => 2, portal => 'portal' },
];
$plugin->mock(
    iscsi_session => sub { $sessions },
    iscsi_portals => sub { ['portal'] },
    run_command => sub {
        my ($cmd) = @_;
        die "unexpected command invocation: @$cmd\n";
    },
    iscsi_session_scan_new_luns => sub {
        my ($session_list) = @_;
        my $filename = "$dir/pve-iscsi-rescan-" . sha256_hex($target);
        open(my $fh, '>>', $filename) or die $!;
        ok(flock($fh, LOCK_EX | LOCK_NB), 'timestamp lock is released before scanning');
        push @scans, map { $_->{session_id} } @$session_list;
    },
);

my sub activate {
    my ($storeid, $name, $cache) = @_;
    $target = $name;
    PVE::Storage::ISCSIPlugin->activate_storage($storeid, { target => $target }, $cache // {});
}

my sub set_stamp {
    my ($name, $age) = @_;
    my $filename = "$dir/pve-iscsi-rescan-" . sha256_hex($name);
    open(my $stamp, '>', $filename) or die $!;
    print $stamp (time() - $age) . "\n";
    close($stamp) or die $!;
    return $filename;
}

activate('first', 'iqn.first');
is_deeply(\@scans, [1, 2], 'all sessions of first target scanned');
activate('second', 'iqn.second/../unsafe');
is_deeply(\@scans, [1, 2, 1, 2], 'second target has its own window and a safe filename');
activate('duplicate', 'iqn.first');
is(scalar(@scans), 4, 'duplicate storage shares the target allowance');
activate('second', 'iqn.second/../unsafe');
is(scalar(@scans), 4, 'independent activation cache does not bypass persisted claim');

my $filename = "$dir/pve-iscsi-rescan-" . sha256_hex('iqn.first');
my $inode = (stat($filename))[1];
set_stamp('iqn.first', 11);
activate('duplicate', 'iqn.first');
is_deeply(\@scans, [1, 2, 1, 2, 1, 2], 'next window scans every session again');
activate('second', 'iqn.second/../unsafe');
is(scalar(@scans), 6, 'other target remains rate limited');

is((stat($filename))[1], $inode, 'timestamp updates do not replace the locked inode');
my @files = glob("$dir/*");
is(scalar(@files), 2, 'exactly one timestamp and lock file per target');

# status_only and normal activations share the same target scan window and never run iscsiadm
activate('status', 'iqn.status', { status_only => 1 });
is_deeply(\@scans, [1, 2, 1, 2, 1, 2, 1, 2], 'status pass scans every session for new LUNs');
activate('status', 'iqn.status', { status_only => 1 });
is(scalar(@scans), 8, 'status pass is rate limited');
activate('normal', 'iqn.status');
is(scalar(@scans), 8, 'normal activation shares window with status pass');
set_stamp('iqn.status', 11);
activate('normal', 'iqn.status');
is(scalar(@scans), 10, 'normal activation scans in next window without running iscsiadm');
activate('status', 'iqn.status', { status_only => 1 });
is(scalar(@scans), 10, 'status pass shares window with normal activation');

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
    my $before = scalar(@scans);
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
    is(scalar(@scans), $before, "$case failure skips the rescan");
    close($lock) if $lock;
}
$failure = '';
activate('recovered', 'iqn.failure-update');
is(scalar(@scans), 12, 'empty timestamp after failed update permits a subsequent first claim');

my $partial = "$dir/pve-iscsi-rescan-" . sha256_hex('iqn.partial');
open(my $partial_fh, '>', $partial) or die $!;
print $partial_fh '17';
close($partial_fh) or die $!;
activate('partial', 'iqn.partial');
is(scalar(@scans), 14, 'an incomplete timestamp counts as none');

$io->unmock_all();
$plugin->unmock('iscsi_session_scan_new_luns');

# End-to-end sysfs scanning tests (run_command remains mocked to die if ever invoked)
{
    my $sysfs = tempdir(CLEANUP => 1);
    local $PVE::Storage::ISCSIPlugin::SYSFS = $sysfs;

    my sub make_host {
        my ($host, $with_scan) = @_;
        $with_scan //= 1;
        make_path("$sysfs/class/scsi_host/host$host");
        if ($with_scan) {
            my $scan = "$sysfs/class/scsi_host/host$host/scan";
            open(my $fh, '>', $scan) or die $!;
            close($fh) or die $!;
        }
    }

    my sub make_session {
        my ($bus, $host, $sid, $tid) = @_;
        my $sdir = "$sysfs/devices/$bus/host$host/session$sid/iscsi_session/session$sid";
        make_path($sdir, "$sysfs/class/iscsi_session");
        symlink($sdir, "$sysfs/class/iscsi_session/session$sid") or die $!;
        if (defined($tid)) {
            open(my $fh, '>', "$sdir/target_id") or die $!;
            print $fh "$tid\n";
            close($fh) or die $!;
        }
    }

    my sub read_file {
        my ($path) = @_;
        open(my $fh, '<', $path) or die $!;
        my $content = do { local $/; <$fh> };
        close($fh);
        return $content;
    }

    make_host(7);
    make_host(8);
    make_host(10, 0); # host exists without scan attribute

    make_session('platform', 7, 3, 0);
    make_session('platform', 7, 4, 2);
    make_session('platform', 7, 5, -1); # unbinding / ISCSI_MAX_TARGET
    make_session('platform', 7, 6, undef); # target_id vanished
    make_session('pci0000:00/0000:00:03.0', 8, 9, 12);
    make_session('platform', 10, 10, 1);

    my (@writes, %fh_path);
    my $fail_print_host;
    my $fail_close_host;
    $io->mock(
        new => sub {
            my $fh = $new->(@_);
            $fh_path{$fh} = $_[1] if defined($fh) && defined($_[1]);
            return $fh;
        },
        print => sub {
            my ($fh, @args) = @_;
            my $path = $fh_path{$fh};
            if (defined($path) && $path =~ m!/class/scsi_host/host(\d+)/scan$!) {
                if (defined($fail_print_host) && $1 == $fail_print_host) {
                    $! = EIO;
                    return 0;
                }
                push @writes, ["host$1", join('', @args)];
            }
            return $print->(@_);
        },
        close => sub {
            my ($fh) = @_;
            my $path = delete $fh_path{$fh};
            my $res = $close->(@_);
            if (
                defined($path)
                && defined($fail_close_host)
                && $path =~ m!/class/scsi_host/host\Q$fail_close_host\E/scan$!
            ) {
                $! = EINVAL;
                return 0;
            }
            return $res;
        },
    );

    # End-to-end activation (both normal and status_only) scans multiple sessions/hosts via sysfs
    $sessions = [
        { session_id => 3, portal => 'portal' },
        { session_id => 4, portal => 'portal' },
        { session_id => 5, portal => 'portal' },
        { session_id => 6, portal => 'portal' },
        { session_id => 9, portal => 'portal' },
        { session_id => 99, portal => 'portal' },
        { session_id => '../3', portal => 'portal' },
        { session_id => 'abc', portal => 'portal' },
        { session_id => undef, portal => 'portal' },
    ];

    for my $mode (['normal', {}], ['status_only', { status_only => 1 }]) {
        my ($label, $cache) = @$mode;
        @writes = ();
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        eval { activate("sysfs-$label", "iqn.sysfs-$label", $cache) };
        is($@, '', "$label activation succeeds via sysfs scan");
        is_deeply(
            \@writes,
            [['host7', '0 0 -'], ['host7', '0 2 -'], ['host8', '0 12 -']],
            "$label activation writes target-scoped scans for all valid sessions across hosts",
        );
        is(
            read_file("$sysfs/class/scsi_host/host7/scan"),
            '0 2 -',
            "$label: host7 scan file has expected format",
        );
        is(
            read_file("$sysfs/class/scsi_host/host8/scan"),
            '0 12 -',
            "$label: host8 scan file has expected format",
        );
        is(
            $warnings,
            '',
            "$label: vanished, unbinding (target_id=-1), and invalid session IDs skipped silently",
        );
    }

    # Read error on one session warns without dying and continues to remaining sessions
    {
        @writes = ();
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        my $orig_read = PVE::Storage::ISCSIPlugin->can('file_read_firstline');
        $plugin->mock(
            file_read_firstline => sub {
                my ($path) = @_;
                die "open for reading failed - No such device\n"
                    if $path =~ m!/session3/target_id$!;
                return $orig_read->($path);
            },
        );
        eval {
            PVE::Storage::ISCSIPlugin::iscsi_session_scan_new_luns(
                [{ session_id => 3 }, { session_id => 9 }],
            );
        };
        is($@, '', 'read error on a session does not abort scanning');
        like(
            $warnings,
            qr/^scanning iSCSI session 3 for new LUNs failed - /,
            'read error warns for the failing session',
        );
        is_deeply(\@writes, [['host8', '0 12 -']], 'remaining sessions are still scanned');
        $plugin->unmock('file_read_firstline');
    }

    # Missing host/scan file fails open without creating it and continues to remaining sessions
    {
        @writes = ();
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        eval {
            PVE::Storage::ISCSIPlugin::iscsi_session_scan_new_luns(
                [{ session_id => 10 }, { session_id => 9 }],
            );
        };
        is($@, '', 'missing scan attribute does not abort scanning');
        like(
            $warnings,
            qr/^scanning iSCSI session 10 for new LUNs failed - cannot open /,
            'missing scan attribute warns',
        );
        ok(!-e "$sysfs/class/scsi_host/host10/scan", 'opening scan attribute does not create it');
        is_deeply(\@writes, [['host8', '0 12 -']], 'remaining sessions scanned after open error');
    }

    # Write error on print warns without dying and continues to remaining sessions
    {
        @writes = ();
        $fail_print_host = 7;
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        eval {
            PVE::Storage::ISCSIPlugin::iscsi_session_scan_new_luns(
                [{ session_id => 3 }, { session_id => 9 }],
            );
        };
        $fail_print_host = undef;
        is($@, '', 'print write error does not abort scanning');
        like(
            $warnings,
            qr/^scanning iSCSI session 3 for new LUNs failed - cannot write /,
            'print write error warns',
        );
        is_deeply(\@writes, [['host8', '0 12 -']], 'remaining sessions scanned after print error');
    }

    # Flush/close error warns without dying and continues to remaining sessions
    {
        @writes = ();
        $fail_close_host = 7;
        my $warnings = '';
        local $SIG{__WARN__} = sub { $warnings .= $_[0] };
        eval {
            PVE::Storage::ISCSIPlugin::iscsi_session_scan_new_luns(
                [{ session_id => 3 }, { session_id => 9 }],
            );
        };
        $fail_close_host = undef;
        is($@, '', 'close flush error does not abort scanning');
        like(
            $warnings,
            qr/^scanning iSCSI session 3 for new LUNs failed - cannot write /,
            'close flush error warns',
        );
        is_deeply(
            \@writes,
            [['host7', '0 0 -'], ['host8', '0 12 -']],
            'remaining sessions scanned after close error',
        );
    }

    $io->unmock_all();
}

done_testing();
