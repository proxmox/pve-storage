#!/usr/bin/perl

use lib '..';

use strict;
use warnings;

use Data::Dumper qw(Dumper);
use PVE::Storage;
use PVE::Cluster;
use PVE::Tools qw(run_command);
use Cwd;
$Data::Dumper::Sortkeys = 1;

my $verbose = undef;

my $storagename = "lvmregression";
my $vgname = 'regressiontest';

#volsize in GB
my $volsize = 1;
my $vmdisk = "vm-102-disk-1";

my $tests = {};

my $cfg = undef;
my $count = 0;
my $testnum = 12;
my $end_test = $testnum;
my $start_test = 1;

if (@ARGV == 2) {
    $end_test = $ARGV[1];
    $start_test = $ARGV[0];
} elsif (@ARGV == 1) {
    $start_test = $ARGV[0];
    $end_test = $ARGV[0];
}

my $test12 = sub {

    print "\nrun test12 \"path\"\n";

    my @res;
    my $fail = 0;
    eval {
        @res = PVE::Storage::path($cfg, "$storagename:$vmdisk");
        if ($res[0] ne "\/dev\/regressiontest\/$vmdisk") {
            $count++;
            $fail = 1;
            warn
                "Test 12 a: path is not correct: expected \'\/dev\/regressiontest\/$vmdisk'\  get \'$res[0]\'";
        }
        if ($res[1] ne "102") {
            if (!$fail) {
                $count++;
                $fail = 1;
            }
            warn "Test 12 a: owner is not correct: expected \'102\'  get \'$res[1]\'";
        }
        if ($res[2] ne "images") {
            if (!$fail) {
                $count++;
                $fail = 1;
            }
            warn "Test 12 a: owner is not correct: expected \'images\'  get \'$res[2]\'";
        }
    };
    if ($@) {
        $count++;
        warn "Test 12 a: $@";
    }

};
$tests->{12} = $test12;

my $test11 = sub {

    print "\nrun test11 \"deactivate_storage\"\n";

    eval {
        PVE::Storage::activate_storage($cfg, $storagename);
        PVE::Storage::deactivate_storage($cfg, $storagename);
    };
    if ($@) {
        $count++;
        warn "Test 11 a: $@";
    }
};
$tests->{11} = $test11;

my $test10 = sub {

    print "\nrun test10 \"activate_storage\"\n";

    eval { PVE::Storage::activate_storage($cfg, $storagename); };
    if ($@) {
        $count++;
        warn "Test 10 a: $@";
    }
};
$tests->{10} = $test10;

my $test9 = sub {

    print "\nrun test15 \"template_list and vdisk_list\"\n";

    my $hash = Dumper {};

    my $res = Dumper PVE::Storage::template_list($cfg, $storagename, "vztmpl");
    if ($hash ne $res) {
        $count++;
        warn "Test 9 a failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "iso");
    if ($hash ne $res) {
        $count++;
        warn "Test 9 b failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "backup");
    if ($hash ne $res) {
        $count++;
        warn "Test 9 c failed\n";
    }

};
$tests->{9} = $test9;

my $test8 = sub {

    print "\nrun test8 \"vdisk_free\"\n";

    eval {
        PVE::Storage::vdisk_free($cfg, "$storagename:$vmdisk");

        eval {
            run_command("lvs $vgname/$vmdisk", outfunc => sub { }, errfunc => sub { });
        };
        if (!$@) {
            $count++;
            warn "Test8 a: vdisk still exists\n";
        }
    };
    if ($@) {
        $count++;
        warn "Test8 a: $@";
    }

};
$tests->{8} = $test8;

my $test7 = sub {

    print "\nrun test7 \"vdisk_alloc\"\n";

    eval {
        my $tmp_volid =
            PVE::Storage::vdisk_alloc($cfg, $storagename, "112", "raw", undef, 1024 * 1024);

        if ($tmp_volid ne "$storagename:vm-112-disk-0") {
            die "volname:$tmp_volid don't match\n";
        }
        eval {
            run_command(
                "lvs --noheadings -o lv_size $vgname/vm-112-disk-0",
                outfunc => sub {
                    my $tmp = shift;
                    if ($tmp !~ m/1\.00g/) {
                        die "size don't match\n";
                    }
                },
            );
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

    eval {
        my $tmp_volid =
            PVE::Storage::vdisk_alloc($cfg, $storagename, "112", "raw", undef, 2048 * 1024);

        if ($tmp_volid ne "$storagename:vm-112-disk-1") {
            die "volname:$tmp_volid don't match\n";
        }
        eval {
            run_command(
                "lvs --noheadings -o lv_size $vgname/vm-112-disk-1",
                outfunc => sub {
                    my $tmp = shift;
                    if ($tmp !~ m/2\.00g/) {
                        die "size don't match\n";
                    }
                },
            );
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

};
$tests->{7} = $test7;

my $test6 = sub {

    print "\nrun test6 \"parse_volume_id\"\n";

    eval {
        my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$vmdisk");

        if ($store ne $storagename || $disk ne $vmdisk) {
            $count++;
            warn "Test6 a: parsing wrong";
        }

    };
    if ($@) {
        $count++;
        warn "Test6 a: $@";
    }

};
$tests->{6} = $test6;

my $test5 = sub {

    print "\nrun test5 \"parse_volname\"\n";

    eval {
        my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
            PVE::Storage::parse_volname($cfg, "$storagename:$vmdisk");

        if (
            $vtype ne 'images'
            || $vmid ne '102'
            || $name ne $vmdisk
            || defined($basename)
            || defined($basevmid)
            || $isBase
            || $format ne 'raw'
        ) {
            $count++;
            warn "Test5 a: parsing wrong";
        }

    };
    if ($@) {
        $count++;
        warn "Test5 a: $@";
    }

};
$tests->{5} = $test5;

my $test4 = sub {

    print "\nrun test4 \"volume_rollback_is_possible\"\n";

    eval {
        my $blockers = [];
        my $res = undef;
        eval {
            $res = PVE::Storage::volume_rollback_is_possible(
                $cfg, "$storagename:$vmdisk", 'snap1', $blockers,
            );
        };
        if (!$@) {
            $count++;
            warn "Test4 a: Rollback shouldn't be possible";
        }
    };
    if ($@) {
        $count++;
        warn "Test4 a: $@";
    }

};
$tests->{4} = $test4;

my $test3 = sub {

    print "\nrun test3 \"volume_has_feature\"\n";

    eval {
        if (PVE::Storage::volume_has_feature(
            $cfg, 'snapshot', "$storagename:$vmdisk", undef, 0,
        )) {
            $count++;
            warn "Test3 a failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 a: $@";
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
        if (PVE::Storage::volume_has_feature(
            $cfg, 'template', "$storagename:$vmdisk", undef, 0,
        )) {
            $count++;
            warn "Test3 l failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 l: $@";
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
        if (PVE::Storage::volume_has_feature(
            $cfg, 'sparseinit', "$storagename:$vmdisk", undef, 0,
        )) {
            $count++;
            warn "Test3 x failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 x: $@";
    }

    eval {
        if (PVE::Storage::volume_has_feature(
            $cfg, 'snapshot', "$storagename:$vmdisk", 'test', 0,
        )) {
            $count++;
            warn "Test3 a1 failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 a1: $@";
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
        if (PVE::Storage::volume_has_feature(
            $cfg, 'template', "$storagename:$vmdisk", 'test', 0,
        )) {
            $count++;
            warn "Test3 l1 failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 l1: $@";
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
        if (PVE::Storage::volume_has_feature(
            $cfg, 'sparseinit', "$storagename:$vmdisk", 'test', 0,
        )) {
            $count++;
            warn "Test3 x1 failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test3 x1: $@";
    }

};
$tests->{3} = $test3;

my $test2 = sub {

    print "\nrun test2 \"volume_resize\"\n";
    my $newsize = ($volsize + 1) * 1024 * 1024 * 1024;

    eval {
        eval { PVE::Storage::volume_resize($cfg, "$storagename:$vmdisk", $newsize, 0); };
        if ($@) {
            $count++;
            warn "Test2 a failed";
        }
        if ($newsize != PVE::Storage::volume_size_info($cfg, "$storagename:$vmdisk")) {
            $count++;
            warn "Test2 a failed";
        }
    };
    if ($@) {
        $count++;
        warn "Test2 a: $@";
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

};
$tests->{1} = $test1;

sub setup_lvm_volumes {
    eval { run_command("vgcreate $vgname /dev/loop1"); };

    print "create lvm volume $vmdisk\n" if $verbose;
    run_command("lvcreate -L${volsize}G -n $vmdisk $vgname");

    my $vollist = [
        "$storagename:$vmdisk",
    ];

    PVE::Storage::activate_volumes($cfg, $vollist);
}

sub cleanup_lvm_volumes {

    print "destroy $vgname\n" if $verbose;
    eval { run_command("vgremove $vgname -y"); };
    if ($@) {
        print "cleanup failed: $@\nretrying once\n" if $verbose;
        eval { run_command("vgremove $vgname -y"); };
        if ($@) {
            clean_up_lvm();
            setup_lvm();
        }
    }
}

sub setup_lvm {

    unlink 'lvm.img';
    eval { run_command("dd if=/dev/zero of=lvm.img bs=1M count=8000"); };
    if ($@) {
        clean_up_lvm();
    }
    my $pwd = cwd();
    eval { run_command("losetup /dev/loop1 $pwd\/lvm.img"); };
    if ($@) {
        clean_up_lvm();
    }
    eval { run_command("pvcreate /dev/loop1"); };
    if ($@) {
        clean_up_lvm();
    }
}

sub clean_up_lvm {

    eval { run_command("pvremove /dev/loop1 -ff -y"); };
    if ($@) {
        warn $@;
    }
    eval { run_command("losetup -d /dev/loop1"); };
    if ($@) {
        warn $@;
    }

    unlink 'lvm.img';
}

sub volume_is_base {
    my ($cfg, $volid) = @_;

    my (undef, undef, undef, undef, undef, $isBase, undef) =
        PVE::Storage::parse_volname($cfg, $volid);

    return $isBase;
}

if ($> != 0) { #EUID
    warn "not root, skipping lvm tests\n";
    exit 0;
}

my $time = time;
print "Start tests for LVMPlugin\n";

$cfg = {
    'ids' => {
        $storagename => {
            'content' => {
                'images' => 1,
                'rootdir' => 1,
            },
            'vgname' => $vgname,
            'type' => 'lvm',
        },
    },
    'order' => { 'lvmregression' => 1 },
};

setup_lvm();
for (my $i = $start_test; $i <= $end_test; $i++) {
    setup_lvm_volumes();

    eval { $tests->{$i}(); };
    if (my $err = $@) {
        warn $err;
        $count++;
    }
    cleanup_lvm_volumes();

}
clean_up_lvm();

$time = time - $time;

print "Stop tests for LVMPlugin\n";
print "$count tests failed\n";
print "Time: ${time}s\n";

exit -1 if $count > 0;
