#!/usr/bin/perl

use strict;
use warnings;
use lib qw(..);
use Test::More;
use Test::MockModule;
use PVE::Storage::SAN;

my $scan = Test::MockModule->new('PVE::Storage::SAN');
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $storage = Test::MockModule->new('PVE::Storage');
my $cluster = Test::MockModule->new('PVE::Cluster');
my $lun = { wwid => '3600+abc', devpath => '/dev/dm-0', 'iscsi-target' => 'iqn.test' };
# the LUN is exported through two targets, its row names only the first
my $identity = Test::MockModule->new('PVE::Multipath::Identity');
my $paths = sub {
    {
        paths => {
            sdb => { wwid => '3600+abc', target => 'iqn.test' },
            sdc => { wwid => '3600+abc', target => 'iqn.second' },
            sdd => { wwid => '3600+other', target => 'iqn.other' },
        },
    };
};
$identity->mock(collect => $paths);
my %sessions = (sdb => 'iqn.test', sdc => 'iqn.second', sdd => 'iqn.other');
my $topology = Test::MockModule->new('PVE::ISCSI::Topology');
$topology->mock(
    device_map => sub {
        return { map { $_ => { target => $sessions{$_} } } keys %sessions };
    },
);
my @slaves;
$scan->mock(dir_glob_foreach => sub { $_[2]->($_) for $_[0] =~ m{/dm-0/slaves$} ? @slaves : () });
{
    my (@cmd, @reads);
    my $blank = "\0" x 4096;
    my %data = ('/dev/null' => $blank, '/dev/zero' => $blank);
    my $rows = '';
    my (%sectors, %block) = (null => 8, zero => 8);
    $lvm->mock(
        file_read_firstline => sub {
            return $sectors{$1} if $_[0] =~ m{^/sys/class/block/(\w+)/size$};
            return $block{$1} if $_[0] =~ m{^/sys/class/block/(\w+)/queue/logical_block_size$};
            return undef;
        },
    );
    $scan->mock(lun_partitions => sub { ['/dev/zero'] });
    my $check = sub {
        my $cfg = { ids => { other => { type => 'lvm', vgname => 'other' } } };
        eval {
            PVE::Storage::SAN::assert_lun_unreferenced($cfg, { %$lun, devpath => '/dev/null' });
        };
        return $@;
    };
    $lvm->mock(
        run_command => sub {
            my ($cmd, %param) = @_;
            if ($cmd->[0] eq '/sbin/pvs') {
                @cmd = @$cmd;
                return $param{outfunc}->(qq({"report":[{"pv":[$rows]}]}));
            }
            my ($device) = $cmd->[1] =~ /^if=(.+)$/;
            push @reads, [@$cmd, $param{timeout}];
            if ($device eq '/dev/null' && ($data{$device} // '') eq 'EIO') {
                $param{errfunc}->("dd: error reading '/dev/null': Input/output error");
                die "command 'dd' failed: exit code 1\n";
            }
            die "got timeout\n" if !defined($data{$device});
            open(my $out, $param{output}) or die $!;
            print {$out} $data{$device};
            close($out) or die $!;
        },
    );
    my @expected = map { ['dd', "if=$_", 'bs=4096', 'count=1', 'iflag=direct', 'status=none', 10] }
        qw(/dev/null /dev/zero);
    is_deeply(
        [$check->(), \@reads],
        ['', \@expected],
        'a blank LUN passes after bounded direct reads of both the LUN and its partition',
    );
    like(
        join(' ', @cmd),
        qr{ --config devices/multipath_component_detection=0 },
        'pvs reports a PV on a path that LVM takes for a member of a missing map',
    );
    ok(grep({ $_ eq '--foreign' } @cmd), 'pvs reports the volume groups of other hosts');
    ok(grep({ $_ eq '--shared' } @cmd), 'pvs reports the volume groups of lvmlockd');
    $data{'/dev/null'} = undef;
    like($check->(), qr{cannot read '/dev/null' to check it for LVM}, 'a read timeout refuses');
    $data{'/dev/null'} = 'EIO';
    like(
        $check->(),
        qr{cannot read '/dev/null' to check it for LVM - dd: error reading '/dev/null': Input/output error},
        'a failed read refuses with the reason of dd',
    );
    $data{'/dev/null'} = $blank;
    $sectors{zero} = 2;
    $data{'/dev/zero'} = "\0" x 1024;
    is(
        $check->(),
        '',
        'a device smaller than 4 KiB, like an extended partition, is read as a whole',
    );
    $sectors{zero} = 8;
    $data{'/dev/zero'} = $blank;
    ($block{null}, $sectors{null}, $data{'/dev/null'}) = (16384, 64, "\0" x 16384);
    @reads = ();
    is($check->(), '', 'a device with larger logical blocks passes');
    is($reads[0]->[2], 'bs=16384', 'after a direct read of one of its blocks');
    ($block{null}, $sectors{null}, $data{'/dev/null'}) = (undef, 8, $blank);
    $data{'/dev/zero'} = 'short';
    like($check->(), qr{cannot read '/dev/zero' to check it for LVM}, 'a short read refuses');
    $data{'/dev/zero'} = "\r\n" x 2048;
    substr($data{'/dev/zero'}, 3 * 512, 8) = 'LABELONE';
    $rows = '{"pv_name":"/dev/null","vg_name":""}';
    like(
        $check->(),
        qr{cannot use '/dev/zero' - it has an LVM label that LVM does not report},
        'the label on a partition needs its own PV row',
    );
    $rows .= ',{"pv_name":"/dev/./zero","vg_name":"data"}';
    @reads = ();
    is_deeply(
        [$check->(), \@reads],
        ['', \@expected],
        'reported devices are read and a PV that LVM names differently accounts for its label',
    );
    is_deeply(
        PVE::Storage::LVMPlugin::lun_volume_groups(['/dev/null', '/dev/zero']),
        ['data'],
        'the volume groups of the PVs on the LUN',
    );
    $lvm->unmock('run_command');
}
my $vgs = [];
$scan->mock(lun_partitions => sub { [] });
$lvm->mock(lun_volume_groups => sub { $vgs });
$lvm->mock(cached_pv_volume_groups => sub { die "write check must not use cached PV metadata\n" });
my $cfg = { ids => {} };
$storage->mock(config => sub { $cfg });
my $check = sub {
    eval { PVE::Storage::SAN::assert_lun_unreferenced($cfg, $lun, @_) };
    return $@;
};
is($check->(), '', 'unreferenced LUN accepted');
for my $type (qw(lvm lvmthin custom)) {
    $vgs = ['data'];
    $cfg->{ids} = {
        data => {
            type => $type,
            vgname => 'data',
            shared => 1,
            disable => 1,
            nodes => { other => 1 },
        },
    };
    for my $cached_vg (undef, 'unrelated') {
        $lun->{vgname} = $cached_vg;
        $lun->{usage} = 'unused';
        like($check->(), qr/used by storage 'data'/, "$type refusal ignores display VG and usage");
    }
    is($check->('data'), '', 'the storage being added can be exempted');
}
delete $lun->{vgname};
delete $lun->{usage};
$vgs = [];
$cfg->{ids} = { direct => { type => 'iscsi', target => 'iqn.test', content => { images => 1 } } };
like($check->(), qr/storage 'direct' serves images from this target$/, 'direct image use refuses');
$cfg->{ids}->{direct}->{target} = 'iqn.second';
like($check->(), qr/storage 'direct' serves images/, 'also through another target of the LUN');
$cfg->{ids}->{direct}->{target} = 'iqn.other';
is($check->(), '', 'a target of another LUN is no reference');
{
    # multipathd does not answer and udev knows the path by another identity
    local $lun->{'iscsi-target'} = 'iqn.other';
    $identity->mock(collect => sub { { paths => {} } });
    like($check->(), qr/storage 'direct' serves images/, 'the target of the LUN itself counts');
    $cfg->{ids}->{direct}->{target} = 'iqn.second';
    is($check->(), '', 'a target that no known path of the LUN uses does not count');
    @slaves = qw(sdb sdc);
    like(
        $check->(),
        qr/storage 'direct' serves images/,
        'the targets of the map members count without their identities',
    );
    @slaves = ();
    $identity->mock(collect => $paths);
}
$cfg->{ids}->{direct}->{target} = 'iqn.test';
$cfg->{ids}->{direct}->{content} = { none => 1 };
is($check->(), '', 'connection-only storage permits use');

for my $type (qw(iscsidirect zfs)) {
    local $cfg->{ids}->{other} = { type => $type, target => 'iqn.test', content => {} };
    like(
        $check->(),
        qr/storage 'other' serves images from this target$/,
        "$type storage of the target refuses",
    );
}
$cfg->{ids}->{data} = { type => 'lvm', base => 'direct:0.0.1.dm-uuid-mpath-3600+abc' };
like($check->(), qr/base volume/, 'base reference refuses');
$cfg->{ids}->{data}->{base} = 'direct:0.0.1.scsi-3600+abc';
like($check->(), qr/base volume/, 'scsi WWID in base name refuses');
$cfg->{ids}->{data}->{base} = 'direct:0.0.1.dm-uuid-part1-mpath-3600+abc';
like($check->(), qr/base volume/, 'a partition of the map as base refuses');
$cfg->{ids}->{data}->{base} = 'direct:0.0.1.scsi-3600+abc-part1';
like($check->(), qr/base volume/, 'a partition of the LUN by its SCSI name as base refuses');
$cfg->{ids}->{data}->{base} = 'direct:0.0.1.scsi-legacy';
$storage->mock(path => sub { '/dev/disk/by-id/legacy' });
$scan->mock(abs_path => sub { '/dev/dm-0' });
like($check->(), qr/base volume/, 'resolved alias to the LUN refuses');
$scan->mock(abs_path => sub { $_[0] eq $lun->{devpath} ? '/dev/dm-0' : '/dev/dm-1' });
is($check->(), '', 'base on another device is no match');
$scan->mock(abs_path => sub { undef });
is($check->(), '', 'unresolvable base is no match');
$storage->mock(path => sub { die "base unavailable\n" });
is($check->(), '', 'unavailable base is no match');
{
    local $lun->{wwid} = '1VENDOR_vol-part1';
    for my $prefix (qw(dm-uuid-mpath dm-uuid-part2-mpath scsi)) {
        local $cfg->{ids}->{data}->{base} = "direct:0.0.1.$prefix-$lun->{wwid}";
        like(
            $check->(),
            qr/base volume/,
            "a '$prefix' base with the full WWID refuses without a local link",
        );
    }
}
ok(!PVE::Storage::LVMPlugin->can('create_volume_group'), 'creation implementation is lexical');
$cfg->{ids} = {};
my @order;
$cluster->mock(
    cfs_lock_domain => sub {
        is($_[0], 'san-3600=2babc', 'lock name escapes unsafe characters without collisions');
        push @order, 'lock';
        $_[2]->();
        push @order, 'unlock';
        $@ = '';
    },
);
PVE::Storage::SAN::with_lun_lock($lun, sub { push @order, 'write' });
is_deeply(\@order, [qw(lock write unlock)], 'the code runs under the lock of the LUN');

# volume group creation checks the references only when it has to initialize the LUN
my $disks = Test::MockModule->new('PVE::Diskmanage');
$disks->mock(
    verify_blockdev_path => sub { $_[0] },
    is_mounted => sub { undef },
    has_holder => sub { undef },
    device_opener => sub { undef },
);
my ($signatures, $pvinfo) = ('', undef);
$lun->{'by-id'} = '/dev/null';
$scan->mock(
    lun_for_device => sub { $lun },
    assert_same_device => sub { },
    assert_lun_unreferenced => sub { push @order, 'check'; die "referenced\n" },
);
$lvm->mock(
    file_read_firstline => sub { $_[0] =~ m{/size$} ? '2048' : undef },
    lvm_pv_info => sub { $pvinfo },
    clear_first_sector => sub { push @order, 'clear' },
    run_command => sub {
        my ($cmd, %param) = @_;
        return $param{outfunc}->(qq({"signatures":[$signatures]})) if $cmd->[0] eq 'wipefs';
        push @order, $cmd->[0];
    },
);
@order = ();
$signatures = '{"type":"LVM2_member","offset":"0x218"}';
$pvinfo = { vgname => 'data', in_use => '1' };
eval {
    PVE::Storage::LVMPlugin::lvm_create_volume_group(
        '/dev/null',
        'data',
        1,
        { exempt => 'new' },
    );
};
is($@, '', 'an existing volume group can be added whatever references its LUN');
is_deeply(\@order, [qw(lock unlock)], 'without reference check or write');
eval { PVE::Storage::LVMPlugin::lvm_create_volume_group('/dev/null', 'data', 1) };
like(
    $@,
    qr/already used by volume group 'data'/,
    'creating a volume group refuses one of the same name that is already on the LUN',
);
eval {
    PVE::Storage::LVMPlugin::lvm_create_volume_group(
        '/dev/null',
        'other',
        1,
        { exempt => 'new' },
    );
};
like($@, qr/already used by volume group 'data'/, 'another volume group on the LUN refuses');
@order = ();
($signatures, $pvinfo) = ('', undef);
eval {
    PVE::Storage::LVMPlugin::lvm_create_volume_group(
        '/dev/null',
        'data',
        1,
        { exempt => 'new' },
    );
};
like($@, qr/referenced/, 'initializing a referenced LUN refuses');
is_deeply(\@order, [qw(lock check)], 'the check precedes the first write');
$lun->{devpath} = '/dev/mapper/alias';
$scan->mock(
    assert_lun_unreferenced => sub {
        is($_[2], 'new', 'new storage exempt');
        is($_[1]->{devpath}, '/dev/null', 'the reference check probes the held device');
        push @order, 'check';
    },
);
$scan->mock(
    assert_lun_paths_unused => sub {
        is($_[0]->{devpath}, '/dev/null', 'and so does the check of the other paths');
        push @order, 'paths';
        die "path in use\n";
    },
);
@order = ();
eval {
    PVE::Storage::LVMPlugin::lvm_create_volume_group(
        '/dev/null',
        'data',
        1,
        { exempt => 'new' },
    );
};
like($@, qr/path in use/, 'a LUN that is in use through another path refuses');
is_deeply(\@order, [qw(lock check paths)], 'before the first write');
$scan->mock(assert_lun_paths_unused => sub { push @order, 'paths' });
@order = ();
eval {
    PVE::Storage::LVMPlugin::lvm_create_volume_group(
        '/dev/null',
        'data',
        1,
        { exempt => 'new' },
    );
};
is($@, '', 'an unreferenced empty LUN is initialized');
is_deeply(
    \@order,
    [qw(lock check paths clear /sbin/pvcreate /sbin/vgcreate unlock)],
    'checked once, then written under the lock',
);

done_testing();
