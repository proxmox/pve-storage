package PVE::Storage::TestArchiveInfo;

use strict;
use warnings;

use lib qw(..);

use PVE::Storage;
use Test::More;

my $vmid = 16110;

# an array of test cases, each test is comprised of the following keys:
# description => to identify a single test
# archive     => the input filename for archive_info
# expected    => the hash that archive_info returns
#
# most of them are created further below
my $tests = [
    # backup archives
    {
	description => 'Backup archive, lxc, tgz',
	archive     => "backup/vzdump-lxc-$vmid-2020_03_30-21_39_30.tgz",
	expected    => {
	    'type'         => 'lxc',
	    'format'       => 'tar',
	    'decompressor' => ['tar', '-z'],
	    'compression'  => 'gz',
	},
    },
    {
	description => 'Backup archive, openvz, tgz',
	archive     => "backup/vzdump-openvz-$vmid-2020_03_30-21_39_30.tgz",
	expected    => {
	    'type'         => 'openvz',
	    'format'       => 'tar',
	    'decompressor' => ['tar', '-z'],
	    'compression'  => 'gz',
	},
    },
];

# add new compression fromats to test
my $decompressor = {
    tar => {
	gz  => ['tar', '-z'],
	lzo => ['tar', '--lzop'],
    },
    vma => {
	gz  => ['zcat'],
	lzo => ['lzop', '-d', '-c'],
    },
};

my $bkp_suffix = {
    qemu   => [ 'vma', $decompressor->{vma}, ],
    lxc    => [ 'tar', $decompressor->{tar}, ],
    openvz => [ 'tar', $decompressor->{tar}, ],
};

# create more test cases for backup files matches
foreach my $virt (keys %$bkp_suffix) {
    my ($format, $decomp) = @{ $bkp_suffix->{$virt} };

    foreach my $suffix (keys %$decomp) {
	my @arr = (
	    {
		description => "Backup archive, $virt, $format.$suffix",
		archive     => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$format.$suffix",
		expected    => {
		    'type'         => "$virt",
		    'format'       => "$format",
		    'decompressor' => $decomp->{$suffix},
		    'compression'  => "$suffix",
		},
	    },
	);

	push @$tests, @arr;
    }
}


# add compression formats to test failed matches
my $non_bkp_suffix = {
    'openvz' => [ 'zip', 'tgz.lzo', 'tar.bz2', 'zip.gz', '', ],
    'lxc'    => [ 'zip', 'tgz.lzo', 'tar.bz2', 'zip.gz', '', ],
    'qemu'   => [ 'vma.xz', 'vms.gz', '', ],
    'none'   => [ 'tar.gz', ],
};

# create tests for failed matches
foreach my $virt (keys %$non_bkp_suffix) {
    my $suffix = $non_bkp_suffix->{$virt};
    foreach my $s (@$suffix) {
	my @arr = (
	    {
		description => "Failed match: Backup archive, $virt, $s",
		archive     => "backup/vzdump-$virt-$vmid-2020_03_30-21_12_40.$s",
		expected    => "ERROR: couldn't determine format and compression type\n",
	    },
	);

	push @$tests, @arr;
    }
}


plan tests => scalar @$tests;

# run through tests array
foreach my $tt (@$tests) {
    my $description = $tt->{description};
    my $archive = $tt->{archive};
    my $expected = $tt->{expected};
    my $got;
    eval { $got = PVE::Storage::archive_info($archive) };
    $got = $@ if $@;

    is_deeply($got, $expected, $description) || diag(explain($got));
}

done_testing();

1;
