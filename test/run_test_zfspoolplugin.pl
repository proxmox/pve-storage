#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper qw(Dumper);
use PVE::Storage;
use PVE::Cluster;
use PVE::Tools qw(run_command);
use Cwd;
$Data::Dumper::Sortkeys = 1;

my $verbose = undef;

my $storagename = "zfstank99";
my $subvol = 'regressiontest';

#volsize in GB
my $volsize = 1;
my $vmdisk = "vm-102-disk-1";
my $vmbase = "base-100-disk-1";
my $vmlinked = "vm-101-disk-1";
my $ctdisk = "subvol-202-disk-1";
my $ctbase = "basevol-200-disk-1";
my $ctlinked = "subvol-201-disk-1";

my $basesnap = '@__base__';
my $tests = {};

#create zfs subvol for testing
my $pool = undef;
my $zpath = undef;
my $cfg = undef;
my $scfg = undef;
my $count = 0;
my $testnum = 19;
my $end_test = $testnum;
my $start_test = 1;

if (@ARGV == 2) {
    $end_test = $ARGV[1];
    $start_test = $ARGV[0];
} elsif (@ARGV == 1) {
    $start_test = $ARGV[0];
    $end_test = $ARGV[0];
}

my $test19 = sub {

    print "\nrun test19 \"path\"\n";

    my @res;
    my $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$vmdisk");
	if ($res[0] ne "\/dev\/zvol\/regressiontest\/$vmdisk") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 a: path is not correct: expected \'\/dev\/zvol\/regressiontest\/$vmdisk'\  get \'$res[0]\'";
	}
	if ($res[1] ne "102") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 a: owner is not correct: expected \'102\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 a: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 a: $@";
    }

    @res = undef;
    $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$vmbase");
	if ($res[0] ne "\/dev\/zvol\/regressiontest\/$vmbase") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 b: path is not correct: expected \'\/dev\/zvol\/regressiontest\/$vmbase'\  get \'$res[0]\'";
	}
	if ($res[1] ne "100") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 b: owner is not correct: expected \'100\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 b: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 b: $@";
    }

    @res = undef;
    $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$vmbase\/$vmlinked");
	if ($res[0] ne "\/dev\/zvol\/regressiontest\/$vmlinked") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 c: path is not correct: expected \'\/dev\/zvol\/regressiontest\/$vmlinked'\  get \'$res[0]\'";
	}
	if ($res[1] ne "101") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 c: owner is not correct: expected \'101\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 c: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 c: $@";
    }

    @res = undef;
    $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$ctdisk");
	if ($res[0] ne "\/regressiontest\/$ctdisk") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 d: path is not correct: expected \'/regressiontest\/$ctdisk'\  get \'$res[0]\'";
	}
	if ($res[1] ne "202") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 d: owner is not correct: expected \'202\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 d: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 d: $@";
    }

    @res = undef;
    $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$ctbase");
	if ($res[0] ne "\/regressiontest\/$ctbase") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 e: path is not correct: expected \'\/regressiontest\/$ctbase'\  get \'$res[0]\'";
	}
	if ($res[1] ne "200") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 e: owner is not correct: expected \'200\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 e: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 e: $@";
    }

    @res = undef;
    $fail = 0;
    eval {
	@res = PVE::Storage::path($cfg, "$storagename:$ctbase\/$ctlinked");
	if ($res[0] ne "\/regressiontest\/$ctlinked") {
	    $count++;
	    $fail = 1;
	    warn "Test 19 f: path is not correct: expected \'\/regressiontest\/$ctlinked'\  get \'$res[0]\'";
	}
	if ($res[1] ne "201") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 f: owner is not correct: expected \'201\'  get \'$res[1]\'";
	}
	if ($res[2] ne "images") {
	    if (!$fail) {
		$count++;
		$fail = 1;
	    }
	    warn "Test 19 f: owner is not correct: expected \'images\'  get \'$res[2]\'";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 19 f: $@";
    }
};
$tests->{19} = $test19;

my $test18 = sub {

    print "\nrun test18 \"scan_zfs\"\n";
    my $res;

    eval {
	$res = PVE::Storage::scan_zfs($cfg, $storagename);

	my $exists = 0;
	foreach my $subvol (@$res){
	    if ($subvol->{pool} eq 'regressiontest') {
		$exists++;
	    }
	}
	if (!$exists) {
	    $count++;
	    warn "Test 18 a: not pool";
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 18 a: $@";
    }
    $res = undef;

    eval {
	$res = PVE::Storage::scan_zfs($cfg, $storagename);
	
	foreach my $subvol (@$res){
	    if ($subvol->{pool} eq 'zfspool/subvol') {
		$count++;
		warn "Test 18 b:";
	    }
	}

	foreach my $subvol (@$res){
	    if ($subvol->{pool} eq 'zfspool/basevol') {
		$count++;
		warn "Test 18 c";
	    }
	}
    };
    if ( $@ ) {
	$count++;
	warn "Test 18 a: $@";
    }
};
$tests->{18} = $test18;

my $test17 = sub {

    print "\nrun test17 \"deactivate_storage\"\n";

    eval {
	PVE::Storage::activate_storage($cfg, $storagename);
	PVE::Storage::deactivate_storage($cfg, $storagename);
    };
    if ($@) {
	$count++;
	warn "Test 17 a: $@";
    }
};
$tests->{17} = $test17;

my $test16 = sub {

    print "\nrun test16 \"activate_storage\"\n";

    eval {
	PVE::Storage::activate_storage($cfg, $storagename);
    };
    if ($@) {
	$count++;
	warn "Test 16 a: $@";
    }
};
$tests->{16} = $test16;

my $test15 = sub {

    print "\nrun test15 \"template_list and vdisk_list\"\n";

    my $hash = Dumper {};

    my $res = Dumper PVE::Storage::template_list($cfg, $storagename, "vztmpl");
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 a failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "iso");
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 b failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "backup");
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 c failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => undef,
			    'volid' => 'zfstank99:base-100-disk-1',
			    'name' => 'base-100-disk-1',
			    'vmid' => '100',
			    'size' => 1073741824,
			    'format' => 'raw'
			}
			]};

    $res = Dumper PVE::Storage::vdisk_list($cfg, $storagename, 100, ["$storagename:$vmbase"]);

    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 d failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => undef,
			    'volid' => 'zfstank99:vm-102-disk-1',
			    'name' => 'vm-102-disk-1',
			    'vmid' => '102',
			    'size' => 1073741824,
			    'format' => 'raw'
			}
			]};

    $res = Dumper PVE::Storage::vdisk_list($cfg, $storagename, 102, ["$storagename:$vmdisk"]);
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 e failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => 'base-100-disk-1@__base__',
			    'volid' => "$storagename:$vmbase\/$vmlinked",
			    'name' => 'vm-101-disk-1',
			    'vmid' => '101',
			    'size' => 1073741824,
			    'format' => 'raw'
			}
			]};

    $res =  Dumper PVE::Storage::vdisk_list($cfg, $storagename, 101, ["$storagename:$vmbase\/$vmlinked"]);
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 f failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => undef,
			    'volid' => 'zfstank99:basevol-200-disk-1',
			    'name' => 'basevol-200-disk-1',
			    'vmid' => '200',
			    'size' => 1073741824,
			    'format' => 'subvol'
			}
			]};

    $res =  Dumper PVE::Storage::vdisk_list($cfg, $storagename, 200, ["$storagename:$ctbase"]);
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 g failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => undef,
			    'volid' => 'zfstank99:subvol-202-disk-1',
			    'name' => 'subvol-202-disk-1',
			    'vmid' => '202',
			    'size' => 1073741824,
			    'format' => 'subvol'
			}
			]};

    $res = Dumper PVE::Storage::vdisk_list($cfg, $storagename, 202, ["$storagename:$ctdisk"]);
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 h failed\n";
    }
    $res = undef;

    $hash = Dumper {'zfstank99' => [
			{
			    'parent' => 'basevol-200-disk-1@__base__',
			    'volid' => "$storagename:$ctbase\/$ctlinked",
			    'name' => 'subvol-201-disk-1',
			    'vmid' => '201',
			    'size' => 1073741824,
			    'format' => 'subvol'
			}
			]};
    $res = Dumper PVE::Storage::vdisk_list($cfg, $storagename, 201, ["$storagename:$ctbase\/$ctlinked"]);
    if ( $hash ne $res ) {
	$count++;
	warn "Test 15 i failed\n";
    }
};
$tests->{15} = $test15;

my $test14 = sub {

    print "\nrun test14 \"vdisk_free\"\n";

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$vmdisk");

	eval {
	    run_command("zfs list $zpath\/$vmdisk", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 a: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 a: $@";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$vmbase");
    };
    if (!$@) {
	$count++;
	warn "Test14 b: free vdisk should not work\n";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$vmbase\/$vmlinked");

	eval {
	    run_command("zfs list $zpath\/$vmlinked", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 c: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 c: $@";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$ctdisk");

	eval {
	    run_command("zfs list $zpath\/$ctdisk", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 d: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 d: $@";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$ctbase");
    };
    if (!$@) {
	$count++;
	warn "Test14 e: free vdisk should not work\n";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$ctbase\/$ctlinked");

	eval {
	    run_command("zfs list $zpath\/$ctlinked", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 f: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 f: $@";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$vmbase");

	eval {
	    run_command("zfs list $zpath\/$vmbase", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 g: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 g: $@";
    }

    eval {
	PVE::Storage::vdisk_free($cfg, "$storagename:$ctbase");

	eval {
	    run_command("zfs list $zpath\/$ctbase", outfunc => sub {}, errfunc => sub {});
	};
	if (!$@) {
	    $count++;
	    warn "Test14 h: vdisk still exists\n";
	}
    };
    if ($@) {
	$count++;
	warn "Test14 h: $@";
    }
};
$tests->{14} = $test14;

my $test13 = sub {

    print "\nrun test13 \"vdisk_alloc\"\n";

    eval {
	my $tmp_volid = PVE::Storage::vdisk_alloc($cfg, $storagename, "112", "raw", undef ,1024 * 1024);

	if ($tmp_volid ne "$storagename:vm-112-disk-1") {
	    die "volname:$tmp_volid don't match\n";
	}
	eval {
	    run_command("zfs get -H volsize $zpath\/vm-112-disk-1", outfunc =>
			sub { my $tmp = shift;
			      if ($tmp !~ m/^$zpath\/vm-112-disk-1.*volsize.*1G.*$/) {
				  die "size don't match\n";
			      }
			});
	};
	if ($@) {
	    $count++;
	    warn "Test13 a: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test13 a: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_alloc($cfg, $storagename, "112", "raw", undef ,2048 * 1024);

	if ($tmp_volid ne "$storagename:vm-112-disk-2") {
	    die "volname:$tmp_volid don't match\n";
	}
	eval {
	    run_command("zfs get -H volsize $zpath\/vm-112-disk-2", outfunc =>
			sub { my $tmp = shift;
			      if ($tmp !~ m/^$zpath\/vm-112-disk-2.*volsize.*2G.*$/) {
				  die "size don't match\n";
			      }
			});
	};
	if ($@) {
	    $count++;
	    warn "Test13 b: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test13 b: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_alloc($cfg, $storagename, "113", "subvol", undef ,1024 * 1024);

	if ($tmp_volid ne "$storagename:subvol-113-disk-1") {
	    die "volname:$tmp_volid  don't match\n";
	}
	eval {
	    run_command("zfs get -H refquota $zpath\/subvol-113-disk-1", outfunc =>
			sub { my $tmp = shift;
			      if ($tmp !~ m/^$zpath\/subvol-113-disk-1.*refquota.*1G.*$/) {
				  die "size don't match\n";
			      }
			});
	};
	if ($@) {
	    $count++;
	    warn "Test13 c: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test13 c: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_alloc($cfg, $storagename, "113", "subvol", undef ,2048 * 1024);

	if ($tmp_volid ne "$storagename:subvol-113-disk-2") {
	    die "volname:$tmp_volid  don't match\n";
	}
	eval {
	    run_command("zfs get -H refquota $zpath\/subvol-113-disk-2", outfunc =>
			sub { my $tmp = shift;
			      if ($tmp !~ m/^$zpath\/subvol-113-disk-2.*refquota.*G.*$/) {
				  die "size don't match\n";
			      }
			});
	};
	if ($@) {
	    $count++;
	    warn "Test13 d: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test13 d: $@";
    }
};
$tests->{13} = $test13;

my $test12 = sub {

    print "\nrun test12 \"vdisk_create_base\"\n";

    eval {
	my $tmp_volid = PVE::Storage::vdisk_create_base($cfg, "$storagename:$vmdisk");

	if ($tmp_volid ne "$storagename:base-102-disk-1") {
	    die;
	}
	eval {
	    run_command("zfs list $zpath\/base-102-disk-1", outfunc => sub {});
	};
	if ($@) {
	    $count++;
	    warn "Test12 a: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test12 a: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_create_base($cfg, "$storagename:$vmlinked");

	if ($tmp_volid ne "$storagename:base-101-disk-1") {
	    die;
	}
	eval {
	    run_command("zfs list $zpath\/base-101-disk-1", outfunc => sub {});
	};
	if ($@) {
	    $count++;
	    warn "Test12 b: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test12 b: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_create_base($cfg, "$storagename:$ctdisk");

	if ($tmp_volid ne "$storagename:basevol-202-disk-1") {
	    die ;
	}
	eval {
	    run_command("zfs list $zpath\/basevol-202-disk-1", outfunc => sub {});
	};
	if ($@) {
	    $count++;
	    warn "Test12 c: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test12 c: $@";
    }

    eval {
	my $tmp_volid = PVE::Storage::vdisk_create_base($cfg, "$storagename:$ctlinked");

	if ($tmp_volid ne "$storagename:basevol-201-disk-1") {
	    die;
	}
	eval {
	    run_command("zfs list $zpath\/basevol-201-disk-1", outfunc => sub {});
	};
	if ($@) {
	    $count++;
	    warn "Test12 d: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test12 d: $@";
    }
};
$tests->{12} = $test12;

my $test11 = sub {

    print "\nrun test11 \"volume_is_base\"\n";

    eval {
	PVE::Storage::vdisk_clone($cfg, "$storagename:$vmdisk", 110);
    };
    if (!$@) {
	$count++;
	warn "Test11 a: clone_image only works on base images";
    }

    eval {
	if ("$storagename:$vmbase\/vm-110-disk-1" ne
	    PVE::Storage::vdisk_clone($cfg, "$storagename:$vmbase", 110, '__base__')){
	    $count++;
	    warn  "Test11 b";
	}
	run_command("zfs list -H -o volsize $zpath\/vm-110-disk-1", outfunc => sub {
	    my $line = shift;

	    chomp($line);
	    warn "Test11 b not correct volsize" if $line !~ m/$volsize/; 
		    });
    };
    if ($@) {
	$count++;
	warn "Test11 b: $@";
    }

    eval {
	PVE::Storage::vdisk_clone($cfg, "$storagename:$vmbase\/$vmlinked", 111);
    };
    if (!$@) {
	$count++;
	warn "Test11 c: clone_image only works on base images";
    }

    eval {
	PVE::Storage::vdisk_clone($cfg, "$storagename:$ctdisk", 110);
    };
    if (!$@) {
	$count++;
	warn "Test11 d: clone_image only works on base images";
    }

    eval {
	if ( "$storagename:$ctbase\/subvol-210-disk-1" ne
	     PVE::Storage::vdisk_clone($cfg, "$storagename:$ctbase", 210, '__base__')){
	    $count++;
	    warn  "Test11 e";
	}
	run_command("zfs list -H -o refquota $zpath\/subvol-210-disk-1", outfunc => sub {
	    my $line = shift;

	    chomp($line);
	    warn "Test11 e not correct volsize" if $line !~ m/$volsize/; 
		    });
    };
    if ($@) {
	$count++;
	warn "Test11 e: $@";
    }

    eval {
	PVE::Storage::vdisk_clone($cfg, "$storagename:$ctbase\/$ctlinked", 211);
    };
    if (!$@) {
	$count++;
	warn "Test11 f: clone_image only works on base images";
    }
};
$tests->{11} = $test11;

my $test10 =sub {

    print "\nrun test10 \"volume_is_base\"\n";

    eval {
	if (1 == volume_is_base($cfg, "$storagename:$vmdisk")) {
	    $count++;
	    warn "Test10 a: is no base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 a: $@";
    }

    eval {
	if (0 == volume_is_base($cfg, "$storagename:$vmbase")) {
	    $count++;
	    warn "Test10 b: is base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 b: $@";
    }

    eval {
	if (1 == volume_is_base($cfg, "$storagename:$vmbase\/$vmlinked")) {
	    $count++;
	    warn "Test10 c: is no base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 c: $@";
    }

    eval {
	if (1 == volume_is_base($cfg, "$storagename:$ctdisk")) {
	    $count++;
	    warn "Test10 d: is no base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 d: $@";
    }

    eval {
	if (0 == volume_is_base($cfg, "$storagename:$ctbase")) {
	    $count++;
	    warn "Test10 e: is base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 e: $@";
    }

    eval {
	if (1 == volume_is_base($cfg, "$storagename:$ctbase\/$ctlinked")) {
	    $count++;
	    warn "Test10 f: is no base";
	}

    };
    if ($@) {
	$count++;
	warn "Test10 f: $@";
    }
};
$tests->{10} = $test10;

my $test9 =sub {

    print "\nrun test9 \"parse_volume_id\"\n";

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$vmdisk");

	if ($store ne $storagename || $disk ne $vmdisk) {
	    $count++;
	    warn "Test9 a: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 a: $@";
    }

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$vmbase");

	if ($store ne $storagename || $disk ne $vmbase) {
	    $count++;
	    warn "Test9 b: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 b: $@";
    }

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$vmbase\/$vmlinked");

	if ($store ne $storagename || $disk ne "$vmbase\/$vmlinked") {
	    $count++;
	    warn "Test9 c: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 c: $@";
    }

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$ctdisk");

	if ($store ne $storagename || $disk ne $ctdisk) {
	    $count++;
	    warn "Test9 d: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 d: $@";
    }

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$ctbase");

	if ($store ne $storagename || $disk ne $ctbase) {
	    $count++;
	    warn "Test9 e: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 e: $@";
    }

    eval {
	my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$ctbase\/$ctlinked");

	if ($store ne $storagename || $disk ne "$ctbase\/$ctlinked") {
	    $count++;
	    warn "Test9 f: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test9 f: $@";
    }
};
$tests->{9} = $test9;

my $test8 = sub {

    print "\nrun test8 \"parse_volname\"\n";

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$vmdisk");

	if ($vtype ne 'images' || $vmid ne '102' ||  $name ne $vmdisk ||
	    defined($basename) || defined($basevmid) || $isBase ||
	    $format ne 'raw') {
	    $count++;
	    warn "Test8 a: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test8 a: $@";
    }

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$vmbase");

	if ($vtype ne 'images' || $vmid ne '100' ||  $name ne $vmbase ||
	    defined($basename) || defined($basevmid) || !$isBase ||
	    $format ne 'raw') {
	    $count++;
	    warn "Test8 b: parsing wrong";
	}
    };
    if ($@) {
	$count++;
	warn "Test8 b: $@";
    }

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$vmbase\/$vmlinked");

	if ($vtype ne 'images' ||  $name ne $vmlinked || $vmid ne '101' ||
	    $basename ne $vmbase || $basevmid ne '100' || $isBase ||
	    $format ne 'raw') {
	    $count++;
	    warn "Test8 c: parsing wrong";
	}
    };
    if ($@) {
	$count++;
	warn "Test8 c: $@";
    }

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$ctdisk");

	if ($vtype ne 'images' || $vmid ne '202' ||  $name ne $ctdisk ||
	    defined($basename) || defined($basevmid) || $isBase ||
	    $format ne 'subvol') {
	    $count++;
	    warn "Test8 d: parsing wrong";
	}

    };
    if ($@) {
	$count++;
	warn "Test8 d: $@";
    }

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$ctbase");
	if ($vtype ne 'images' || $vmid ne '200' ||  $name ne $ctbase ||
	    defined($basename) || defined($basevmid) || !$isBase ||
	    $format ne 'subvol') {
	    $count++;
	    warn "Test8 e: parsing wrong";
	}
    };
    if ($@) {
	$count++;
	warn "Test8 e: $@";
    }

    eval {
	my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = PVE::Storage::parse_volname($cfg, "$storagename:$ctbase\/$ctlinked");

	if ($vtype ne 'images' ||  $name ne $ctlinked || $vmid ne '201' ||
	    $basename ne $ctbase || $basevmid ne '200' || $isBase ||
	    $format ne 'subvol') {
	    $count++;
	    warn "Test8 f: parsing wrong";
	}
    };
    if ($@) {
	$count++;
	warn "Test8 f: $@";
    }
};
$tests->{8} = $test8;

my $test7 = sub {

    print "\nrun test7 \"volume_rollback\"\n";

    my $tmp_guid;
    my $parse_guid = sub {
	my ($line) = shift;

	if ( $line =~ m/^Disk identifier \(GUID\)\: (.*)$/ ) {
	    $tmp_guid = $1;
	}
    };

    eval {
	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmdisk", outfunc => $parse_guid);
	run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmdisk", outfunc => $parse_guid);

	my $old_guid = $tmp_guid;
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmdisk", 'snap1');

	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmdisk", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmdisk", 'snap1');
	    $tmp_guid = undef;
	    run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmdisk", outfunc => $parse_guid);
	    if ($old_guid ne $tmp_guid) {
		$count++;
		warn "Test7 a: Zvol makes no rollback";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 a: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 a: $@";
    }
    $tmp_guid = undef;

    eval {
	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmbase", outfunc => $parse_guid);
	run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmbase", outfunc => $parse_guid);

	my $old_guid = $tmp_guid;
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase", 'snap1');

	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmbase", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmbase", 'snap1');
	    $tmp_guid = undef;
	    run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmbase", outfunc => $parse_guid);
	    if ($old_guid ne $tmp_guid) {
		$count++;
		warn "Test7 b: Zvol makes no rollback";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 b: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 b: $@";
    }
    $tmp_guid = undef;

    eval {
	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmlinked", outfunc => $parse_guid);
	run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmlinked", outfunc => $parse_guid);

	my $old_guid = $tmp_guid;
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase\/$vmlinked", 'snap1');

	run_command("sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$vmlinked", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmbase\/$vmlinked", 'snap1');
	    $tmp_guid = undef;
	    run_command("sgdisk -p \/dev\/zvol\/$zpath\/$vmlinked", outfunc => $parse_guid);
	    if ($old_guid ne $tmp_guid) {
		$count++;
		warn "Test7 c: Zvol makes no rollback";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 c: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 c: $@";
    }
    $tmp_guid = undef;

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctdisk", 'snap1');

	run_command("touch \/$zpath\/$ctdisk\/test.txt", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctdisk", 'snap1');
	    eval {
		run_command("ls \/$zpath\/$ctdisk\/test.txt", errofunc => sub {});
	    };
	    if (!$@) {
		$count++;
		warn "Test7 d: $@";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 d: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 d: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase", 'snap1');

	run_command("touch \/$zpath\/$ctbase\/test.txt", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctbase", 'snap1');
	    eval {
		run_command("ls \/$zpath\/$ctbase\/test.txt", errofunc => sub {});
	    };
	    if (!$@) {
		$count++;
		warn "Test7 e: $@";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 e: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 f: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase/$ctlinked", 'snap1');

	run_command("touch \/$zpath\/$ctlinked\/test.txt", outfunc => $parse_guid);
	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctbase/$ctlinked", 'snap1');
	    eval {
		run_command("ls \/$zpath\/$ctlinked\/test.txt", errofunc => sub {});
	    };
	    if (!$@) {
		$count++;
		warn "Test7 g: $@";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test7 g: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 g: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmdisk", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmdisk", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 h: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 h: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmbase", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 i: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 i: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase\/$vmlinked", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$vmbase\/$vmlinked", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 j: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 j: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctdisk", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctdisk", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 k: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 k: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctbase", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 l: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 l: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase/$ctlinked", 'snap2');

	eval {
	    PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$ctbase/$ctlinked", 'snap1');
	};
	if (!$@) {
	    $count++;
	    warn "Test7 m: Not allowed to rollback";
	}
    };
    if ($@) {
	$count++;
	warn "Test7 m: $@";
    }
};
$tests->{7} = $test7;

my $test6 = sub {

    print "\nrun test6 \"volume_rollback_is_possible\"\n";

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmdisk", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmdisk", 'snap1')) {
	    $count++;
	    warn "Test6 a: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 a: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmbase", 'snap1')) {
	    $count++;
	    warn "Test6 b: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 b: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmlinked", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmbase\/$vmlinked", 'snap1')) {
	    $count++;
	    warn "Test6 c: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 c: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctdisk", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctdisk", 'snap1')) {
	    $count++;
	    warn "Test6 d: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 d: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctbase", 'snap1')) {
	    $count++;
	    warn "Test6 e: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 e: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctlinked", 'snap1');
	if ( 1 !=
	     PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctbase\/$ctlinked", 'snap1')) {
	    $count++;
	    warn "Test6 f: Rollback sould possible"
	}
    };
    if ($@) {
	$count++;
	warn "Test6 f: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmdisk", 'snap2');
	PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmdisk", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 g: Rollback should not possible";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase", 'snap2');
	PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmbase", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 h: Rollback should not possible";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmlinked", 'snap2');
	PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$vmbase\/$vmlinked", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 j: Rollback should not possible";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctdisk", 'snap2');
	PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctdisk", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 k: Rollback should not possible";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase", 'snap2');
        PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctbase", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 l: Rollback should not possible";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctlinked", 'snap2');
	PVE::Storage::volume_rollback_is_possible($cfg, "$storagename:$ctbase\/$ctlinked", 'snap1');
    };
    if (!$@) {
	$count++;
	warn "Test6 m: Rollback should not possible";
    }
};
$tests->{6} = $test6;

my $test5 = sub {

    print "\nrun test5 \"volume_snapshot_delete\"\n";
    my $out = sub{my $tmp = shift;};

    eval {
	run_command("zfs snapshot $zpath\/$vmdisk\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$vmdisk", 'snap');
	    eval{
		run_command("zfs list $zpath\/$vmdisk\@snap", errfunc => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 a: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE a: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 a: $@";
    }

    eval {
	run_command("zfs snapshot $zpath\/$vmbase\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$vmbase", 'snap');
	    eval{
		run_command("zfs list $zpath\/$vmbase\@snap", errmsg => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 b: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE b: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 b: $@";
    }

    eval {
	run_command("zfs snapshot $zpath\/$vmlinked\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$vmbase\/$vmlinked", 'snap');
	    eval{
		run_command("zfs list $zpath\/$vmlinked\@snap", errmsg => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 c: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE c: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 c: $@";
    }

    eval {
	run_command("zfs snapshot $zpath\/$ctdisk\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$ctdisk", 'snap');
	    eval{
		run_command("zfs list $zpath\/$ctdisk\@snap", errmsg => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 d: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE d: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 d: $@";
    }

    eval {
	run_command("zfs snapshot $zpath\/$ctbase\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$ctbase", 'snap');
	    eval{
		run_command("zfs list $zpath\/$ctbase\@snap", errmsg => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 e: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE e: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 e: $@";
    }

    eval {
	run_command("zfs snapshot $zpath\/$ctlinked\@snap");
	eval{
	    PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$ctbase\/$ctlinked", 'snap');
	    eval{
		run_command("zfs list $zpath\/$ctlinked\@snap", errmsg => $out, outfunc => $out);
	    };
	    if (!$@) {
		$count++;
		warn "Test5 f: snapshot still exists";
	    }
	};
	if ($@) {
	    $count++;
	    warn "Test5 PVE f: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test5 f: $@";
    }
    print "######Ignore Output if no Test5 g: is included######\n";
    eval{
	PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$vmbase", '__base__');
	eval{
	    run_command("zfs list $zpath\/$vmbase\@__base__", outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test5 g: $@";
	}
    };
    if (!$@) {
	$count++;
	warn "Test5 PVE g: snapshot __base__ can be erased";
    }
    print "######End Ignore#######\n";
};
$tests->{5} = $test5;

my $test4 = sub {

    print "\nrun test4 \"volume_snapshot\"\n";
    my $out = sub{};

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmdisk", 'snap');
	eval{
	    run_command("zfs list $zpath\/$vmdisk\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 a: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 a: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase", 'snap');
	eval{
	    run_command("zfs list $zpath\/$vmbase\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 b: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 c: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$vmbase\/$vmlinked", 'snap');
	eval{
	    run_command("zfs list $zpath\/$vmdisk\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 c: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 c: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctdisk", 'snap');
	eval{
	    run_command("zfs list $zpath\/$ctdisk\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 d: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 d: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase", 'snap');
	eval{
	    run_command("zfs list $zpath\/$ctbase\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 e: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 e: $@";
    }

    eval {
	PVE::Storage::volume_snapshot($cfg, "$storagename:$ctbase\/$ctlinked", 'snap');
	eval{
	    run_command("zfs list $zpath\/$ctdisk\@snap", errmsg => $out, outfunc => $out);
	};
	if ($@) {
	    $count++;
	    warn "Test4 f: $@";
	}
    };
    if ($@) {
	$count++;
	warn "Test4 f: $@";
    }
};
$tests->{4} = $test4;

my $test3 = sub {

    print "\nrun test3 \"volume_has_feature\"\n";

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmdisk", undef, 0)) {
	    $count++;
	    warn "Test3 a failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 a: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmbase", undef, 0)) {
	    $count++;
	    warn "Test3 b failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 b: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmbase\/$vmlinked", undef, 0)) {
	    $count++;
	    warn "Test3 c failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 c: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctdisk", undef, 0)) {
	    $count++;
	    warn "Test3 d failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 d: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctbase", undef, 0)) {
	    $count++;
	    warn "Test3 e failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 e: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctbase\/$ctlinked", undef, 0)) {
	    $count++;
	    warn "Test3 f failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 f: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmdisk", undef, 0)) {
	    $count++;
	    warn "Test3 g failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 g: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmbase", undef, 0)) {
	    $count++;
	    warn "Test3 h failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 h: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmbase\/$vmlinked", undef, 0)) {
	    $count++;
	    warn "Test3 h failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 h: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctdisk", undef, 0)) {
	    $count++;
	    warn "Test3 i failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 i: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctbase", undef, 0)) {
	    $count++;
	    warn "Test3 j failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 j: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctbase\/$ctlinked", undef, 0)) {
	    $count++;
	    warn "Test3 k failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 k: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmdisk", undef, 0)) {
	    $count++;
	    warn "Test3 l failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 l: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmbase", undef, 0)) {
	    $count++;
	    warn "Test3 m failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 m: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmbase\/$vmlinked", undef, 0)) {
	    $count++;
	    warn "Test3 n failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 n: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctdisk", undef, 0)) {
	    $count++;
	    warn "Test3 o failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 o: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctbase", undef, 0)) {
	    $count++;
	    warn "Test3 p failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 p: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctbase\/$ctlinked", undef, 0)) {
	    $count++;
	    warn "Test3 q failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 q: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmdisk", undef, 0)) {
	    $count++;
	    warn "Test3 r failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 r: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmbase", undef, 0)) {
	    $count++;
	    warn "Test3 s failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 s: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmbase\/$vmlinked", undef, 0)) {
	    $count++;
	    warn "Test3 t failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 t: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctdisk", undef, 0)) {
	    $count++;
	    warn "Test3 u failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 u: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctbase", undef, 0)) {
	    $count++;
	    warn "Test3 v failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 v: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctbase\/$ctlinked", undef, 0)) {
	    $count++;
	    warn "Test3 w failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 w: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmdisk", undef, 0)) {
	    $count++;
	    warn "Test3 x failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 x: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmbase", undef, 0)) {
	    $count++;
	    warn "Test3 y failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 y: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmbase\/$vmlinked", undef, 0)) {
	    $count++;
	    warn "Test3 z failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 z: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctdisk", undef, 0)) {
	    $count++;
	    warn "Test3 A failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 A: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctbase", undef, 0)) {
	    $count++;
	    warn "Test3 B failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 B: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctbase\/$ctlinked", undef, 0)) {
	    $count++;
	    warn "Test3 C failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 C: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 a1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 a1: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmbase", 'test', 0)) {
	    $count++;
	    warn "Test3 b1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 b1: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$vmbase\/$vmlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 c1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 c1: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 d1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 d1: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctbase", 'test', 0)) {
	    $count++;
	    warn "Test3 e1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 e1: $@";
    }

    eval {
	if (!PVE::Storage::volume_has_feature($cfg, 'snapshot', "$storagename:$ctbase\/$ctlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 f1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 f1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 g1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 g1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmbase", 'test', 0)) {
	    $count++;
	    warn "Test3 h1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 h1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$vmbase\/$vmlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 h1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 h1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 i1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 i1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctbase", 'test', 0)) {
	    $count++;
	    warn "Test3 j1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 j1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'clone', "$storagename:$ctbase\/$ctlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 k1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 k1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 l1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 l1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmbase", 'test', 0)) {
	    $count++;
	    warn "Test3 m1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 m1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$vmbase\/$vmlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 n1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 n1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 o1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 o1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctbase", 'test', 0)) {
	    $count++;
	    warn "Test3 p1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 p1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'template', "$storagename:$ctbase\/$ctlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 q1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 q1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 r1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 r1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmbase", 'test', 0)) {
	    $count++;
	    warn "Test3 s1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 s1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$vmbase\/$vmlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 t1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 t1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 u1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 u1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctbase", 'test', 0)) {
	    $count++;
	    warn "Test3 v1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 v1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'copy', "$storagename:$ctbase\/$ctlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 w1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 w1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 x1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 x1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmbase", 'test', 0)) {
	    $count++;
	    warn "Test3 y1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 y1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$vmbase\/$vmlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 z1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 z1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctdisk", 'test', 0)) {
	    $count++;
	    warn "Test3 A1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 A1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctbase", 'test', 0)) {
	    $count++;
	    warn "Test3 B1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 B1: $@";
    }

    eval {
	if (PVE::Storage::volume_has_feature($cfg, 'sparseinit', "$storagename:$ctbase\/$ctlinked", 'test', 0)) {
	    $count++;
	    warn "Test3 C1 failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test3 C1: $@";
    }
};
$tests->{3} = $test3;

my $test2 = sub {

    print "\nrun test2 \"volume_resize\"\n";
    my $newsize = ($volsize + 1) * 1024 * 1024 * 1024;

    eval {
	if (($newsize/1024) !=
	    PVE::Storage::volume_resize($cfg, "$storagename:$vmdisk", $newsize, 0)) {
	    $count++;
	    warn "Test2 a failed";
	}
	if ($newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$vmdisk")) {
	    $count++;
	    warn "Test2 a failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test2 a: $@";
    }

    eval {
	warn "Test2 b failed" if ($newsize/1024) != PVE::Storage::volume_resize($cfg, "$storagename:$vmbase", $newsize, 0);
	warn "Test2 b failed" if $newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$vmbase");
    };
    if ($@) {
	$count++;
	warn "Test2 b: $@";
    }

    eval {
	if (($newsize/1024) != PVE::Storage::volume_resize($cfg, "$storagename:$vmbase\/$vmlinked", $newsize, 0)) {
	    $count++;
	    warn "Test2 c failed";
	}
	if ($newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$vmbase\/$vmlinked")) {
	    $count++;
	    warn "Test2 c failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test2 c: $@";
    }

    eval {
	if (($newsize/1024) != PVE::Storage::volume_resize($cfg, "$storagename:$ctdisk", $newsize, 0)) {
	    $count++;
	    warn "Test2 d failed";
	}
	if ($newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$ctdisk")) {
	    $count++;
	    warn "Test2 d failed"
	}
    };
    if ($@) {
	$count++;
	warn "Test2 d: $@";
    }

    eval {
	if (($newsize/1024) !=
	    PVE::Storage::volume_resize($cfg, "$storagename:$ctbase", $newsize, 0)) {
	    $count++;
	    warn "Test2 e failed";
	}
	if ($newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$ctbase")) {
	    $count++;
	    warn "Test2 e failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test2 e: $@";
    }

    eval {
	if (($newsize/1024) !=
	    PVE::Storage::volume_resize($cfg, "$storagename:$ctbase\/$ctlinked", $newsize, 0)) {
	    $count++;
	    warn "Test2 f failed";
	}
	if ($newsize  !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$ctbase\/$ctlinked")) {
	    $count++;
	    warn "Test2 f failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test2 f: $@";
    }
};
$tests->{2} = $test2;

my $test1 = sub {

    print "\nrun test1 \"volume_size_info\"\n";
    my $size = ($volsize * 1024 * 1024 * 1024);

    eval {
	if ($size != PVE::Storage::volume_size_info($cfg, "$storagename:$vmdisk")) {
	    $count++;
	    warn "Test1 a failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test1 a : $@";
    }

    eval {
	if ($size != PVE::Storage::volume_size_info($cfg, "$storagename:$vmbase")) {
	    $count++;
	    warn "Test1 b failed";
	}

    };
    if ($@) {
	$count++;
	warn "Test1 b : $@";
    }

    eval {
	if ($size !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$vmbase\/$vmlinked")) {
	    $count++;
	    warn "Test1 c failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test1 c : $@";
    }

    eval {
	if ($size !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$ctdisk")) {
	    $count++;
	    warn "Test1 d failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test1 d : $@";
    }

    eval {
	if ($size !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$ctbase")) {
	    $count++;
	    warn "Test1 e failed";
	}
    };
    if ($@) {
	$count++;
	warn "Test1 e : $@";
    }

    eval {
	if ($size !=
	    PVE::Storage::volume_size_info($cfg, "$storagename:$vmbase\/$vmlinked")) {
	    $count++;
	    warn "Test1 f failed"
	}
    };
    if ($@) {
	$count++;
	warn "Test1 f : $@";
    }

};
$tests->{1} = $test1;

sub setup_zfs {

    #create VM zvol
    print "create zvol $vmdisk\n" if $verbose;
    run_command("zfs create -V${volsize}G $zpath\/$vmdisk");

    print "create zvol $vmbase\n" if $verbose;
    run_command("zfs create -V${volsize}G $zpath\/$vmbase");
    run_command("zfs snapshot $zpath\/$vmbase$basesnap");

    print "create linked clone $vmlinked\n" if $verbose;
    run_command("zfs clone $zpath\/$vmbase$basesnap $zpath\/$vmlinked");

    #create CT subvol
    print "create subvol $ctdisk\n" if $verbose;
    run_command("zfs create -o refquota=${volsize}G $zpath\/$ctdisk");

    print "create subvol $vmbase\n" if $verbose;
    run_command("zfs create -o refquota=${volsize}G $zpath\/$ctbase");
    run_command("zfs snapshot $zpath\/$ctbase$basesnap");

    print "create linked clone $vmlinked\n" if $verbose;
    run_command("zfs clone $zpath\/$ctbase$basesnap $zpath\/$ctlinked -o refquota=${volsize}G");
    run_command("udevadm trigger --subsystem-match block");
    run_command("udevadm settle --timeout 10 --exit-if-exists=/dev/zvol/$zpath\/$ctlinked");
}

sub cleanup_zfs {

    print "destroy $pool\/$subvol\n" if $verbose;
    eval { run_command("zfs destroy $zpath -r"); };
    if ($@) {
	print "cleanup failed: $@\nretrying once\n" if $verbose;
	eval { run_command("zfs destroy $zpath -r"); };
	if ($@) {
	    clean_up_zpool();
	    setup_zpool();
	}
    }
}

sub setup_zpool {

    unlink 'zpool.img';
    eval {
	run_command("truncate -s 8G zpool.img");
    };
    if ($@) {
	clean_up_zpool();
    }
    my $pwd = cwd();
    eval {
	run_command("zpool create $subvol $pwd\/zpool.img");
    };
    if ($@) {
	clean_up_zpool();
    }
}

sub clean_up_zpool {

    eval {
	run_command("zpool destroy $subvol");
    };
    if ($@) {
	warn $@;}
    unlink 'zpool.img';
}

sub volume_is_base {
    my ($cfg, $volid) = @_;

    my (undef, undef, undef, undef, undef, $isBase, undef) = PVE::Storage::parse_volname($cfg, $volid);

    return $isBase;
}


setup_zpool();

my $time = time;
print "Start tests for ZFSPoolPlugin\n";

$cfg = {'ids' => {
    $storagename => {
	'content' => {
	    'images' => 1,
	    'rootdir' => 1
	},
		'pool' => $subvol,
		'type' => 'zfspool'
    },
	},
		'order' => {'zfstank99' => 1,}
};

$zpath = $subvol;

for (my $i = $start_test; $i <= $end_test; $i++) {
    setup_zfs();

    eval {
	$tests->{$i}();
    };
    warn $@ if $@;
    cleanup_zfs();
}

clean_up_zpool();

$time = time - $time;

print "Stop tests for ZFSPoolPlugin\n";
print "$count tests failed\n";
print "Time: ${time}s\n";
