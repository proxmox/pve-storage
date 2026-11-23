#!/usr/bin/perl

use v5.36;
use lib qw(..);
use Test::MockModule;
use Test::More;

use PVE::API2::Storage::Config;
use PVE::Storage;
use PVE::Storage::LVMPlugin;

# Adding a storage must not wait for its connection while the configuration is locked.

my $raw = "iscsi: san\n\tportal 192.0.2.1\n\ttarget iqn.2026-01.test:a\n\tcontent none\n\n"
    . "dir: local\n\tpath /var/lib/vz\n\tcontent iso\n";
our $locked;
my (@events, @failures, $during, $lock_fails);
my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(
    lock_storage_config => sub($code, $errmsg) {
        push @events, 'lock';
        die "$errmsg: got lock request timeout\n" if $lock_fails;
        local $locked = 1;
        $code->();
        push @events, 'unlock';
    },
    config => sub { PVE::Storage::Plugin->parse_config('storage.cfg', $raw) },
    write_config => sub { $raw = PVE::Storage::Plugin->write_config('storage.cfg', $_[0]) },
    activate_storage => sub($cfg, $storeid, @) {
        push @events, "activate $storeid" . ($locked ? ' locked' : '');
        $during->() if $during;
        die shift(@failures) if @failures;
    },
    activate_volumes => sub($cfg, $vols, @) {
        push @events, "volumes @$vols" . ($locked ? ' locked' : '');
    },
);
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
$lvm->mock(on_add_hook => sub { push @events, 'hook' . ($locked ? ' locked' : ''); return });

my $add = sub(%param) {
    @events = ();
    return eval { PVE::API2::Storage::Config->create({%param}); 1 } ? '' : $@;
};
my $dir = sub($storeid) { (type => 'dir', storage => $storeid, path => "/srv/$storeid") };

is($add->($dir->('plain')), '', 'added');
is_deeply(\@events, [qw(lock unlock), 'activate plain'], 'activated after the lock');

@failures = ("mount busy\n");
is($add->($dir->('raced')), '', 'a failure that another activation caused');
is_deeply(\@events, [qw(lock unlock), ('activate raced') x 2], 'is tried once more');

@failures = ("no answer\n", "still no answer\n");
like($add->($dir->('broken')), qr/^create storage failed: no answer$/, 'a failure fails');
is_deeply(
    \@events,
    [qw(lock unlock), ('activate broken') x 2, qw(lock unlock)],
    'and takes the lock again',
);
unlike($raw, qr/broken/, 'to remove the storage');
like($raw, qr/plain/, 'but not another one');

@failures = ("no answer\n", "still no answer\n");
$during = sub { $raw =~ s{(dir: changed\n)}{$1\tdisable 1\n} if $raw !~ /disable 1/ };
like($add->($dir->('changed')), qr/was added, but its activation failed/, 'a changed storage');
like($raw, qr/changed/, 'is kept');

@failures = ("no answer\n", "still no answer\n");
$during =
    sub { $raw .= "\nlvm: onit\n\tvgname vg\n\tbase used:0.0.1.scsi-3600\n" if $raw !~ /onit/ };
like($add->($dir->('used')), qr/was added, but its activation failed/, 'so is one in use');
like($raw, qr/dir: used/, 'as base of another one');
$during = undef;

@failures = ("no answer\n", "still no answer\n");
my $calls = 0;
$storage->mock(
    lock_storage_config => sub($code, $errmsg) {
        push @events, 'lock';
        die "$errmsg: got lock request timeout\n" if $calls++;
        local $locked = 1;
        $code->();
        push @events, 'unlock';
    },
);
like(
    $add->($dir->('stuck')),
    qr/activation failed - no answer \(removing it failed: got lock request timeout\)/,
    'a removal without the lock reports both errors',
);
like($raw, qr/dir: stuck/, 'and keeps the storage');
$storage->mock(
    lock_storage_config => sub($code, $errmsg) {
        push @events, 'lock';
        local $locked = 1;
        $code->();
        push @events, 'unlock';
    },
);

my $base = 'san:0.0.1.scsi-36001405aabbccddeeff00112233445566';
is(
    $add->(type => 'lvm', storage => 'shared', vgname => 'vg', base => $base, shared => 1),
    '',
    'an LVM storage on a base volume is added',
);
is_deeply(
    \@events,
    ['activate san', "volumes $base", 'lock', 'hook locked', 'unlock', 'activate shared'],
    'its base is activated before the lock',
);
$add->(type => 'lvm', storage => 'other', vgname => 'vg2', base => 'local:iso/x.iso');
is_deeply(
    \@events,
    ['lock', 'hook locked', 'unlock', 'activate other'],
    'but not a base that is no SAN storage',
);

done_testing();
