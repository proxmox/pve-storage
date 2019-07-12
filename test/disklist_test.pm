package PVE::Diskmanage::Test;

use strict;
use warnings;

use lib qw(..);

use PVE::Diskmanage;
use PVE::Tools;

use Test::MockModule;
use Test::More;
use JSON;
use Data::Dumper;

my $testcasedir; # current case directory
my $testcount = 0; # testcount for TAP::Harness
my $diskmanage_module; # mockmodule for PVE::Diskmanage
my $print = 0;

sub mocked_run_command {
    my ($cmd, %param) = @_;

    my $outputlines = [];
    if (my $ref = ref($cmd)) {
	if ($cmd->[0] =~ m/udevadm/i) {
	    # simulate udevadm output
	    my $dev = $cmd->[3];
	    $dev =~ s|/sys/block/||;
	    @$outputlines = split(/\n/, read_test_file("${dev}_udevadm"));

	} elsif ($cmd->[0] =~ m/smartctl/i) {
	    # simulate smartctl output
	    my $dev;
	    my $type;
	    if (@$cmd > 3) {
		$dev = $cmd->[5];
		$type = 'smart';
	    } else {
		$dev = $cmd->[2];
		$type = 'health';
	    }
	    $dev =~ s|/dev/||;
	    @$outputlines = split(/\n/, read_test_file("${dev}_${type}"));
	} elsif ($cmd->[0] =~ m/sgdisk/i) {
	    # simulate sgdisk
	    die "implement me: @$cmd\n";
	} elsif ($cmd->[0] =~ m/zpool/i) {
	    # simulate zpool output
	    @$outputlines = split(/\n/, read_test_file('zpool'));

	} elsif ($cmd->[0] =~ m/pvs/i) {
	    # simulate lvs output
	    @$outputlines = split(/\n/, read_test_file('pvs'));
	} elsif ($cmd->[0] =~ m/lvs/i) {
	    @$outputlines = split(/\n/, read_test_file('lvs'));
	} elsif ($cmd->[0] =~ m/lsblk/i) {
	    my $content = read_test_file('lsblk');
	    if ($content eq '') {
		$content = '{}';
	    }
	    @$outputlines = split(/\n/, $content);
	} else {
	    die "unexpected run_command call: '@$cmd', aborting\n";
	}
    } else {
	print "unexpected run_command call: '@$cmd', aborting\n";
	die;
    }

    my $outfunc;
    if ($param{outfunc}) {
	$outfunc = $param{outfunc};
	map { &$outfunc(($_)) } @$outputlines;

	return 0;
    }
}

sub mocked_get_sysdir_info {
    my ($param) = @_;

    my $originalsub = $diskmanage_module->original('get_sysdir_info');

    $param =~ s|/sys/block|disk_tests/$testcasedir|;

    return &$originalsub($param);
}

sub mocked_is_iscsi {
    return 0;
}

sub mocked_dir_glob_foreach {
    my ($dir, $regex, $sub) = @_;

    my $lines = [];

    # read lines in from file
    if ($dir =~ m{^/sys/block$} ) {
	@$lines = split(/\n/, read_test_file('disklist'));
    } elsif ($dir =~ m{^/sys/block/([^/]+)}) {
	@$lines = split(/\n/, read_test_file('partlist'));
    }

    foreach my $line (@$lines) {
	if ($line =~ m/$regex/) {
	    &$sub($line);
	}
    }
}

sub mocked_parse_proc_mounts {
    my $text = read_test_file('mounts');

    my $mounts = [];

    foreach my $line(split(/\n/, $text)) {
	push @$mounts, [split(/\s+/, $line)];
    }

    return $mounts;
}

sub read_test_file {
    my ($filename) = @_;

    if (!-f  "disk_tests/$testcasedir/$filename") {
	print "file '$testcasedir/$filename' not found\n";
	return '';
    }
    open (my $fh, '<', "disk_tests/$testcasedir/$filename")
	or die "Cannot open disk_tests/$testcasedir/$filename: $!";

    my $output = <$fh> // '';
    chomp $output if $output;
    while (my $line = <$fh>) {
	chomp $line;
	$output .= "\n$line";
    }

    return $output;
}


sub test_disk_list {
    my ($testdir) = @_;
    subtest "Test '$testdir'" => sub {
	my $testcount = 0;
	$testcasedir = $testdir;

	my $disks;
	my $expected_disk_list;
	eval {
	    $disks = PVE::Diskmanage::get_disks();
	};
	warn $@ if $@;
	$expected_disk_list = decode_json(read_test_file('disklist_expected.json'));

	print Dumper($disks) if $print;
	$testcount++;
	is_deeply($disks, $expected_disk_list, 'disk list should be the same');

	foreach my $disk (sort keys %$disks) {
	    my $smart;
	    my $expected_smart;
	    eval {
		$smart = PVE::Diskmanage::get_smart_data("/dev/$disk");
		print Dumper($smart) if $print;
		$expected_smart = decode_json(read_test_file("${disk}_smart_expected.json"));
	    };

	    if ($smart && $expected_smart) {
		$testcount++;
		is_deeply($smart, $expected_smart, "smart data for '$disk' should be the same");
	    } elsif ($smart && -f  "disk_tests/$testcasedir/${disk}_smart_expected.json") {
		$testcount++;
		ok(0,  "could not parse expected smart for '$disk'\n");
	    }
	    my $disk_tmp = {};

	    # test single disk parameter
	    $disk_tmp = PVE::Diskmanage::get_disks($disk);
	    warn $@ if $@;
	    $testcount++;
	    print Dumper $disk_tmp if $print;
	    is_deeply($disk_tmp->{$disk}, $expected_disk_list->{$disk}, "disk $disk should be the same");


	    # test wrong parameter
	    eval {
		PVE::Diskmanage::get_disks( { test => 1 } );
	    };
	    my $err = $@;
	    $testcount++;
	    is_deeply($err, "disks is not a string or array reference\n", "error message should be the same");

	}
	    # test multi disk parameter
	    $disks = PVE::Diskmanage::get_disks( [ keys %$disks ] );
	    $testcount++;
	    is_deeply($disks, $expected_disk_list, 'disk list should be the same');

	done_testing($testcount);
    };
}

# start reading tests:

if (@ARGV && $ARGV[0] eq 'print') {
    $print = 1;
}

print("Setting up Mocking\n");
$diskmanage_module =  new Test::MockModule('PVE::Diskmanage', no_auto => 1);
$diskmanage_module->mock('run_command' => \&mocked_run_command);
print("\tMocked run_command\n");
$diskmanage_module->mock('dir_glob_foreach' => \&mocked_dir_glob_foreach);
print("\tMocked dir_glob_foreach\n");
$diskmanage_module->mock('get_sysdir_info' => \&mocked_get_sysdir_info);
print("\tMocked get_sysdir_info\n");
$diskmanage_module->mock('is_iscsi' => \&mocked_is_iscsi);
print("\tMocked is_iscsi\n");
$diskmanage_module->mock('assert_blockdev' => sub { return 1; });
print("\tMocked assert_blockdev\n");
$diskmanage_module->mock('dir_is_empty' => sub {
	# all partitions have a holder dir
	my $val = shift;
	if ($val =~ m|^/sys/block/.+/.+/|) {
	    return 0;
	}
	return 1;
    });
print("\tMocked dir_is_empty\n");
my $tools_module= new Test::MockModule('PVE::ProcFSTools', no_auto => 1);
$tools_module->mock('parse_proc_mounts' => \&mocked_parse_proc_mounts);
print("\tMocked parse_proc_mounts\n");
print("Done Setting up Mocking\n\n");

print("Beginning Tests:\n\n");
opendir (my $dh, 'disk_tests')
    or die "Cannot open disk_tests: $!";

while (readdir $dh) {
    my $dir = $_;
    next if $dir eq '.' or $dir eq '..';
    $testcount++;
    test_disk_list($dir);
}

done_testing($testcount);
