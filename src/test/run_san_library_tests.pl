#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::Cluster;

my %registered;
my $cluster = Test::MockModule->new('PVE::Cluster');
$cluster->mock(cfs_register_file => sub { $registered{ $_[0] } = [@_[1, 2]] });
require PVE::Multipath::Config;
require PVE::Multipath::Generator;

subtest 'CFS callbacks preserve filenames, data and method receivers' => sub {
    my ($parse, $write) = $registered{'multipath.cfg'}->@*;
    my $cfg = $parse->('multipath.cfg', undef);
    is_deeply($cfg->{ids}, {}, 'an absent configuration parses as empty');
    $cfg = $parse->('multipath.cfg', "wwid: 3600abc\n\talias san-lun\n\tnodes n2,n1\n");
    is_deeply($cfg->{ids}->{'3600abc'}->{nodes}, { n1 => 1, n2 => 1 }, 'nodes are decoded');
    my $again = $parse->('multipath.cfg', $write->('multipath.cfg', $cfg));
    is_deeply($again->{ids}, $cfg->{ids}, 'sections survive the registered writer and parser');
    ($parse, $write) = $registered{'multipath-overrides.conf'}->@*;
    is($parse->('multipath-overrides.conf', undef), '', 'absent overrides parse as empty');
    is($write->('multipath-overrides.conf', undef), '', 'undefined overrides write as empty');
    is($write->('multipath-overrides.conf', "devices { }  \n"), "devices { }\n",
        'writer trims');
};

subtest 'omitted optional arguments behave like undef' => sub {
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    $config->mock(read_config => sub { die "configuration unavailable\n" });
    for my $args ([], [undef, undef, undef]) {
        eval { PVE::Multipath::Generator::regenerate(@$args) };
        is($@, "configuration unavailable\n", 'regeneration reads the default configuration');
    }
    $config->mock(cfs_lock_file => sub { $@ = "lock unavailable\n" });
    for my $lock (
        \&PVE::Multipath::Config::lock_config, \&PVE::Multipath::Config::lock_overrides,
    ) {
        for my $args ([], [undef]) {
            eval {
                $lock->(sub { die "unexpected callback\n" }, @$args);
            };
            is($@, "lock unavailable\n", 'an omitted error prefix leaves the error intact');
        }
    }
};

subtest 'volume groups are read from the LVM hint file, if it is usable' => sub {
    require PVE::Storage::LVMPlugin;
    my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
    my $contents;
    $lvm->mock(
        file_get_contents => sub {
            is($_[0], '/run/lvm/hints', 'only the existing LVM cache is read');
            die "No such file\n" if !defined($contents);
            return $contents;
        },
    );
    $lvm->mock(abs_path => sub { $_[0] eq '/dev/disk/by-id/test' ? '/dev/dm-0' : $_[0] });
    $lvm->mock(run_command => sub { die "unexpected device probe\n" });
    $lvm->redefine(
        file_read_firstline => sub { $_[0] =~ m{/dm-3/dm/uuid$} ? 'part1-mpath-36001' : undef },
    );
    my $tools = Test::MockModule->new('PVE::Tools');
    $tools->redefine(
        dir_glob_foreach => sub { $_[2]->('dm-2') if $_[0] eq '/sys/class/block/dm-3/slaves' });
    my $head = "# Created by vgs pid 1\nhints_version: 2.1\n";
    for my $case (
        [undef, undef, 'missing file'],
        ['', undef, 'empty file'],
        ["# Created empty by vgs pid 1\n", undef, 'file that LVM left without content'],
        ["scan:/dev/sda pvid:abc devn:8:0 vg:data\n", undef, 'no version line'],
        [
            "hints_version: 3.0\nscan:/dev/sda pvid:abc devn:8:0 vg:data\n",
            undef,
            'other format version',
        ],
        [$head, {}, 'no PVs'],
        ["${head}scan=/dev/sda pvid=abc devn=8:0 vg=data\n", {}, 'different format'],
        ["${head}scan:/dev/sda pvid:abc devn:8:0 vg:-\n", {}, 'orphan PV'],
        ["${head}scan:/dev/sda pvid:abc devn:8:0 vg:[unknown]\n", {}, 'unknown VG'],
        [
            "${head}scan:/dev/sda pvid:abc devn:8:0 vg:data\n",
            { '/dev/sda' => 'data' },
            'cached VG',
        ],
        [
            "${head}scan:/dev/disk/by-id/test pvid:abc devn:253:0 vg:data\n",
            { '/dev/dm-0' => 'data' },
            'resolved device name',
        ],
        [
            "${head}scan:/dev/dm-3 pvid:abc devn:253:3 vg:data\n",
            { '/dev/dm-3' => 'data', '/dev/dm-2' => 'data' },
            'partition of a multipath map',
        ],
    ) {
        $contents = $case->[0];
        is_deeply(PVE::Storage::LVMPlugin::cached_pv_volume_groups(), $case->[1], $case->[2]);
    }
};

subtest 'the names of a held LUN have to mean the held device' => sub {
    require PVE::Storage::SAN;
    open(my $fh, '<', '/dev/null') or die "cannot open /dev/null - $!\n";
    my $lun = { 'by-id' => '/dev/null', devpath => '/dev/null' };
    my $check = sub {
        eval { PVE::Storage::SAN::assert_same_device(@_) };
        return $@;
    };
    is($check->($lun, $fh, '/dev/null'), '', 'same device behind all names');
    like(
        $check->({ %$lun, 'by-id' => '/dev/zero' }, $fh),
        qr/'\/dev\/zero' changed/,
        'by-id name',
    );
    like(
        $check->({ %$lun, devpath => '/dev/zero' }, $fh),
        qr/'\/dev\/zero' changed/,
        'device path',
    );
    like(
        $check->($lun, $fh, '/dev/zero'),
        qr/'\/dev\/zero' changed/,
        'name used for the commands',
    );
    like($check->($lun, $fh, '/dev/gone'), qr/'\/dev\/gone' changed/, 'name without a device');
};

done_testing();
