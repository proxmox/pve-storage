package PVE::Storage::TestPruneBackups;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use Test::More;
use Test::MockModule;

my $storeid = 'BackTest123';
my @vmids = (1234, 9001);

# only includes the information needed for prune_backups
my $mocked_backups_lists = {};

my $basetime = 1577881101; # 2020_01_01-12_18_21 UTC

foreach my $vmid (@vmids) {
    push @{$mocked_backups_lists->{default}}, (
	{
	    'volid' => "$storeid:backup/vzdump-qemu-$vmid-2018_05_26-11_18_21.tar.zst",
	    'ctime' => $basetime - 585*24*60*60 - 60*60,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-qemu-$vmid-2019_12_31-11_18_21.tar.zst",
	    'ctime' => $basetime - 24*60*60 - 60*60,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-qemu-$vmid-2019_12_31-11_19_21.tar.zst",
	    'ctime' => $basetime - 24*60*60 - 60*60 + 60,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-qemu-$vmid-2020_01_01-11_18_21.tar.zst",
	    'ctime' => $basetime - 60*60,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-qemu-$vmid-2020_01_01-12_18_21.tar.zst",
	    'ctime' => $basetime,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-lxc-$vmid-2020_01_01-12_18_21.tar.zst",
	    'ctime' => $basetime,
	    'vmid'  => $vmid,
	},
	{
	    'volid' => "$storeid:backup/vzdump-$vmid-renamed.tar.zst",
	    'ctime' => 1234,
	    'vmid'  => $vmid,
	},
    );
}
push @{$mocked_backups_lists->{year1970}}, (
    {
	'volid' => "$storeid:backup/vzdump-lxc-321-1970_01_01-00_01_23.tar.zst",
	'ctime' => 83,
	'vmid'  => 321,
    },
    {
	'volid' => "$storeid:backup/vzdump-lxc-321-2070_01_01-00_01_00.tar.zst",
	'ctime' => 60*60*24 * (365*100 + 25) + 60,
	'vmid'  => 321,
    },
);
push @{$mocked_backups_lists->{novmid}}, (
    {
	'volid' => "$storeid:backup/vzdump-lxc-novmid.tar.gz",
	'ctime' => 1234,
    },
);
push @{$mocked_backups_lists->{threeway}}, (
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2019_12_25-12_18_21.tar.zst",
	'ctime' => $basetime - 7*24*60*60,
	'vmid'  => 7654,
    },
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2019_12_31-12_18_21.tar.zst",
	'ctime' => $basetime - 24*60*60,
	'vmid'  => 7654,
    },
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2020_01_01-12_18_21.tar.zst",
	'ctime' => $basetime,
	'vmid'  => 7654,
    },
);
push @{$mocked_backups_lists->{weekboundary}}, (
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_03-12_18_21.tar.zst",
	'ctime' => $basetime + (366-31+2)*24*60*60,
	'vmid'  => 7654,
    },
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_04-12_18_21.tar.zst",
	'ctime' => $basetime + (366-31+3)*24*60*60,
	'vmid'  => 7654,
    },
    {
	'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_07-12_18_21.tar.zst",
	'ctime' => $basetime + (366-31+6)*24*60*60,
	'vmid'  => 7654,
    },
);
my $current_list;
my $mock_plugin = Test::MockModule->new('PVE::Storage::Plugin');
$mock_plugin->redefine(list_volumes => sub {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    my $list = $mocked_backups_lists->{$current_list};

    return $list if !defined($vmid);

    return [ grep { $_->{vmid} eq $vmid } @{$list} ];
});

sub generate_expected {
    my ($vmids, $type, $marks) = @_;

    my @expected;
    foreach my $vmid (@{$vmids}) {
	push @expected, (
	    {
		'volid' => "$storeid:backup/vzdump-qemu-$vmid-2018_05_26-11_18_21.tar.zst",
		'type'  => 'qemu',
		'ctime' => $basetime - 585*24*60*60 - 60*60,
		'mark'  => $marks->[0],
		'vmid'  => $vmid,
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-$vmid-2019_12_31-11_18_21.tar.zst",
		'type'  => 'qemu',
		'ctime' => $basetime - 24*60*60 - 60*60,
		'mark'  => $marks->[1],
		'vmid'  => $vmid,
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-$vmid-2019_12_31-11_19_21.tar.zst",
		'type'  => 'qemu',
		'ctime' => $basetime - 24*60*60 - 60*60 + 60,
		'mark'  => $marks->[2],
		'vmid'  => $vmid,
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-$vmid-2020_01_01-11_18_21.tar.zst",
		'type'  => 'qemu',
		'ctime' => $basetime - 60*60,
		'mark'  => $marks->[3],
		'vmid'  => $vmid,
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-$vmid-2020_01_01-12_18_21.tar.zst",
		'type'  => 'qemu',
		'ctime' => $basetime,
		'mark'  => $marks->[4],
		'vmid'  => $vmid,
	    },
	) if !defined($type) || $type eq 'qemu';
	push @expected, (
	    {
		'volid' => "$storeid:backup/vzdump-lxc-$vmid-2020_01_01-12_18_21.tar.zst",
		'type'  => 'lxc',
		'ctime' => $basetime,
		'mark'  => $marks->[5],
		'vmid'  => $vmid,
	    },
	) if !defined($type) || $type eq 'lxc';
	push @expected, (
	    {
		'volid' => "$storeid:backup/vzdump-$vmid-renamed.tar.zst",
		'type'  => 'unknown',
		'ctime' => 1234,
		'mark'  => 'protected',
		'vmid'  => $vmid,
	    },
	) if !defined($type);
    }
    return [ sort { $a->{volid} cmp $b->{volid} } @expected ];
}

# an array of test cases, each test is comprised of the following keys:
# description   => to identify a single test
# vmid          => VMID or undef for all
# type          => 'qemu' or 'lxc' or undef for all
# keep          => options describing what to keep
# list          => backups list to use. defaults to 'default'
# expected      => what prune_backups should return
#
# most of them are created further below
my $tests = [
    {
	description => 'last=3, multiple IDs',
	keep => {
	    'keep-last' => 3,
	},
	expected => generate_expected(\@vmids, undef, ['remove', 'remove', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'weekly=2, one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-weekly' => 2,
	},
	expected => generate_expected([$vmids[0]], undef, ['keep', 'remove', 'remove', 'remove', 'keep', 'keep']),
    },
    {
	description => 'daily=weekly=monthly=1, multiple IDs',
	keep => {
	    'keep-hourly' => 0,
	    'keep-daily' => 1,
	    'keep-weekly' => 1,
	    'keep-monthly' => 1,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'hourly=4, one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-hourly' => 4,
	    'keep-daily' => 0,
	},
	expected => generate_expected([$vmids[0]], undef, ['keep', 'remove', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'yearly=2, multiple IDs',
	keep => {
	    'keep-hourly' => 0,
	    'keep-daily' => 0,
	    'keep-weekly' => 0,
	    'keep-monthly' => 0,
	    'keep-yearly' => 2,
	},
	expected => generate_expected(\@vmids, undef, ['remove', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'last=2,hourly=2 one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-last' => 2,
	    'keep-hourly' => 2,
	},
	expected => generate_expected([$vmids[0]], undef, ['keep', 'remove', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'last=1,monthly=2, multiple IDs',
	keep => {
	    'keep-last' => 1,
	    'keep-monthly' => 2,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'monthly=3, one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-monthly' => 3,
	},
	expected => generate_expected([$vmids[0]], undef, ['keep', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'last=daily=weekly=1, multiple IDs',
	keep => {
	    'keep-last' => 1,
	    'keep-daily' => 1,
	    'keep-weekly' => 1,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'last=daily=weekly=1, others zero, multiple IDs',
	keep => {
	    'keep-hourly' => 0,
	    'keep-last' => 1,
	    'keep-daily' => 1,
	    'keep-weekly' => 1,
	    'keep-monthly' => 0,
	    'keep-yearly' => 0,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'daily=2, one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-daily' => 2,
	},
	expected => generate_expected([$vmids[0]], undef, ['remove', 'remove', 'keep', 'remove', 'keep', 'keep']),
    },
    {
	description => 'weekly=monthly=1, multiple IDs',
	keep => {
	    'keep-weekly' => 1,
	    'keep-monthly' => 1,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'remove', 'remove', 'remove', 'keep', 'keep']),
    },
    {
	description => 'weekly=yearly=1, one ID',
	vmid => $vmids[0],
	keep => {
	    'keep-weekly' => 1,
	    'keep-yearly' => 1,
	},
	expected => generate_expected([$vmids[0]], undef, ['keep', 'remove', 'remove', 'remove', 'keep', 'keep']),
    },
    {
	description => 'weekly=yearly=1, one ID, type qemu',
	vmid => $vmids[0],
	type => 'qemu',
	keep => {
	    'keep-weekly' => 1,
	    'keep-yearly' => 1,
	},
	expected => generate_expected([$vmids[0]], 'qemu', ['keep', 'remove', 'remove', 'remove', 'keep', '']),
    },
    {
	description => 'week=yearly=1, one ID, type lxc',
	vmid => $vmids[0],
	type => 'lxc',
	keep => {
	    'keep-last' => 1,
	},
	expected => generate_expected([$vmids[0]], 'lxc', ['', '', '', '', '', 'keep']),
    },
    {
	description => 'yearly=1, year before 2000',
	keep => {
	    'keep-yearly' => 1,
	},
	list => 'year1970',
	expected => [
	    {
		'volid' => "$storeid:backup/vzdump-lxc-321-1970_01_01-00_01_23.tar.zst",
		'ctime' => 83,
		'mark'  => 'remove',
		'type'  => 'lxc',
		'vmid'  => 321,
	    },
	    {
		'volid' => "$storeid:backup/vzdump-lxc-321-2070_01_01-00_01_00.tar.zst",
		'ctime' => 60*60*24 * (365*100 + 25) + 60,
		'mark'  => 'keep',
		'type'  => 'lxc',
		'vmid'  => 321,
	    },
	],
    },
    {
	description => 'last=1, ne ID, year before 2000',
	keep => {
	    'keep-last' => 1,
	},
	list => 'novmid',
	expected => [
	    {
		'volid' => "$storeid:backup/vzdump-lxc-novmid.tar.gz",
		'ctime' => 1234,
		'mark'  => 'protected',
		'type'  => 'lxc',
	    },
	],
    },
    {
	description => 'all missing, multiple IDs',
	keep => {},
	expected => generate_expected(\@vmids, undef, ['keep', 'keep', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'all zero, multiple IDs',
	keep => {
	    'keep-last' => 0,
	    'keep-hourly' => 0,
	    'keep-daily' => 0,
	    'keep-weekly' => 0,
	    'keep-monthyl' => 0,
	    'keep-yearly' => 0,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'keep', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'some zero, some missing, multiple IDs',
	keep => {
	    'keep-last' => 0,
	    'keep-hourly' => 0,
	    'keep-daily' => 0,
	    'keep-monthyl' => 0,
	    'keep-yearly' => 0,
	},
	expected => generate_expected(\@vmids, undef, ['keep', 'keep', 'keep', 'keep', 'keep', 'keep']),
    },
    {
	description => 'daily=weekly=monthly=1',
	keep => {
	    'keep-daily' => 1,
	    'keep-weekly' => 1,
	    'keep-monthly' => 1,
	},
	list => 'threeway',
	expected => [
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2019_12_25-12_18_21.tar.zst",
		'ctime' => $basetime - 7*24*60*60,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'keep',
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2019_12_31-12_18_21.tar.zst",
		'ctime' => $basetime - 24*60*60,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'remove', # month is already covered by the backup kept by keep-weekly!
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2020_01_01-12_18_21.tar.zst",
		'ctime' => $basetime,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'keep',
	    },
	],
    },
    {
	description => 'daily=weekly=1,weekboundary',
	keep => {
	    'keep-daily' => 1,
	    'keep-weekly' => 1,
	},
	list => 'weekboundary',
	expected => [
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_03-12_18_21.tar.zst",
		'ctime' => $basetime + (366-31+2)*24*60*60,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'remove',
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_04-12_18_21.tar.zst",
		'ctime' => $basetime + (366-31+3)*24*60*60,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'keep',
	    },
	    {
		'volid' => "$storeid:backup/vzdump-qemu-7654-2020_12_07-12_18_21.tar.zst",
		'ctime' => $basetime + (366-31+6)*24*60*60,
		'type'  => 'qemu',
		'vmid'  => 7654,
		'mark'  => 'keep',
	    },
	],
    },
];

plan tests => scalar @$tests;

for my $tt (@$tests) {

    my $got = eval {
	$current_list = $tt->{list} // 'default';
	my $res = PVE::Storage::Plugin->prune_backups($tt->{scfg}, $storeid, $tt->{keep}, $tt->{vmid}, $tt->{type}, 1);
	return [ sort { $a->{volid} cmp $b->{volid} } @{$res} ];
    };
    $got = $@ if $@;

    is_deeply($got, $tt->{expected}, $tt->{description}) || diag(explain($got));
}

done_testing();

1;
