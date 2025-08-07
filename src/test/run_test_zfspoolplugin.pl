#!/usr/bin/perl

use lib '..';

use v5.36;

use Test::More;

use Data::Dumper qw(Dumper);
use PVE::Storage;
use PVE::Cluster;
use PVE::Tools qw(file_get_contents file_set_contents run_command);
use Cwd;
$Data::Dumper::Sortkeys = 1;

my $verbose = undef;

my $storagename = "zfstank99";
my $subvol = 'regressiontest';
my $mountpoint = "${subvol}_mnt";
my $devbase = "/dev/zvol/$subvol";

#volsize in GB
my $volsize = 1;

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

my $test_vols = {
    vmdisk => {
        kind => 'zvol',
        volname => 'vm-102-disk-1',
        filename => 'vm-102-disk-1',
        type => 'images',
        vmid => 102,
    },
    vmbase => {
        isbase => 1,
        kind => 'zvol',
        volname => 'base-100-disk-0',
        filename => 'base-100-disk-0',
        type => 'images',
        vmid => 100,
    },
    vmlinked => {
        kind => 'zvol',
        base => 'vmbase',
        volname => 'base-100-disk-0/vm-101-disk-0',
        filename => 'vm-101-disk-0',
        type => 'images',
        vmid => 101,
    },
    ctdisk => {
        kind => 'subvol',
        volname => 'subvol-202-disk-1',
        filename => 'subvol-202-disk-1',
        type => 'images',
        vmid => 202,
    },
    ctbase => {
        isbase => 1,
        kind => 'subvol',
        volname => 'basevol-200-disk-0',
        filename => 'basevol-200-disk-0',
        type => 'images',
        vmid => 200,
    },
    ctlinked => {
        kind => 'subvol',
        base => 'ctbase',
        volname => 'basevol-200-disk-0/subvol-201-disk-0',
        filename => 'subvol-201-disk-0',
        type => 'images',
        vmid => 201,
    },
};

my @base_list = grep { $test_vols->{$_}->{isbase} } sort keys %$test_vols;

sub foreach_testvol($code) {
    for my $name (sort keys %$test_vols) {
        my $vol = $test_vols->{$name};
        my $base = $vol->{base};
        my $base_vol;
        if (defined($base)) {
            $base_vol = $test_vols->{$base} or die "missing base vol for '$name'\n";
        }

        eval { $code->($name, $vol, $base, $base_vol); };

        if (my $err = $@) {
            $count++;
            warn "test died: $err";
        }
    }
}

sub foreach_basevol($code) {
    for my $name (@base_list) {
        my $vol = $test_vols->{$name};

        eval { $code->($name, $vol); };

        if (my $err = $@) {
            $count++;
            warn "test died: $err";
        }
    }
}

foreach_testvol sub($name, $vol, $base, $basevol) {
    if ($vol->{kind} eq 'zvol') {
        $vol->{format} = 'raw';
    } elsif ($vol->{kind} eq 'subvol') {
        $vol->{format} = 'subvol';
    } else {
        die "invalid 'kind' in testvol '$name'\n";
    }
};

sub path_test {
    my ($testname, $volid, $exp_path, $exp_vmid, $exp_vtype) = @_;

    my $fail = 0;
    eval {
        my @res = PVE::Storage::path($cfg, $volid);
        if ($res[0] ne $exp_path) {
            $count++;
            $fail = 1;
            warn "$testname: path is not correct: expected '$exp_path', got '$res[0]'";
        }
        if ($res[1] ne $exp_vmid) {
            if (!$fail++) {
                $count++;
            }
            warn "$testname: owner is not correct: expected '$exp_vmid', got '$res[1]'";
        }
        if ($res[2] ne $exp_vtype) {
            if (!$fail++) {
                $count++;
            }
            warn "$testname: type is not correct: expected '$exp_vtype', got '$res[2]'";
        }
    };
    if ($@) {
        $count++;
        warn "$testname: $@";
    }
}

my $test19 = sub {

    print "\nrun test19 \"path\"\n";

    foreach_testvol sub($name, $vol, $basename, $base) {
        my ($volname, $kind, $filename) = $vol->@{qw(volname kind filename)};
        path_test(
            "Test 19 $name",
            "$storagename:$volname",
            $kind eq 'zvol' ? "$devbase/$filename" : "/$mountpoint/$filename",
            $vol->{vmid},
            $vol->{type},
        );
    };
};
$tests->{19} = $test19;

my $test18 = sub {

    print "\nrun test18 \"scan_zfs\"\n";
    my $res;

    eval {
        $res = PVE::Storage::scan_zfs($cfg, $storagename);

        my $exists = 0;
        foreach my $subvol (@$res) {
            if ($subvol->{pool} eq 'regressiontest') {
                $exists++;
            }
        }
        if (!$exists) {
            $count++;
            warn "Test 18 a: not pool";
        }
    };
    if ($@) {
        $count++;
        warn "Test 18 a: $@";
    }
    $res = undef;

    eval {
        $res = PVE::Storage::scan_zfs($cfg, $storagename);

        foreach my $subvol (@$res) {
            if ($subvol->{pool} eq 'zfspool/subvol') {
                $count++;
                warn "Test 18 b:";
            }
        }

        foreach my $subvol (@$res) {
            if ($subvol->{pool} eq 'zfspool/basevol') {
                $count++;
                warn "Test 18 c";
            }
        }
    };
    if ($@) {
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

    eval { PVE::Storage::activate_storage($cfg, $storagename); };
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
    if ($hash ne $res) {
        $count++;
        warn "Test 15 a failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "iso");
    if ($hash ne $res) {
        $count++;
        warn "Test 15 b failed\n";
    }
    $res = undef;

    $res = Dumper PVE::Storage::template_list($cfg, $storagename, "backup");
    if ($hash ne $res) {
        $count++;
        warn "Test 15 c failed\n";
    }
    $res = undef;

    foreach_testvol sub($name, $vol, $basename, $base) {
        my $res = PVE::Storage::vdisk_list(
            $cfg,
            $storagename,
            $vol->{vmid},
            ["$storagename:$vol->{volname}"],
        );
        my $expected = {
            'parent' => $base ? "$base->{filename}\@__base__" : undef,
            'volid' => "$storagename:$vol->{volname}",
            'name' => $vol->{filename},
            'vmid' => $vol->{vmid},
            'size' => 1073741824,
            'format' => $vol->{format},
        };
        my $vtype = $vol->{type};
        $expected->{vtype} = $vtype if $vtype ne 'images' && $vtype ne 'rootdir';
        if (!is_deeply($res, { $storagename => [$expected] })) {
            ++$count;
            warn "Test 15 $name failed\n";
        }
    };
};
$tests->{15} = $test15;

my $test14 = sub {

    print "\nrun test14 \"vdisk_free\"\n";

    foreach_basevol sub($name, $vol) {
        eval { PVE::Storage::vdisk_free($cfg, "$storagename:$vol->{volname}"); };
        if (!$@) {
            $count++;
            warn "Test14 $name: free vdisk should not work\n";
        }
    };

    foreach_testvol sub($name, $vol, $base, $basevol) {
        return if $vol->{isbase};
        eval {
            PVE::Storage::vdisk_free($cfg, "$storagename:$vol->{volname}");

            eval {
                run_command(
                    "zfs list $zpath\/$vol->{filename}",
                    outfunc => sub { },
                    errfunc => sub { },
                );
            };
            if (!$@) {
                $count++;
                warn "Test14 $name: vdisk still exists\n";
            }
        };
        if ($@) {
            $count++;
            warn "Test14 a: $@";
        }
    };

    foreach_basevol sub($name, $vol) {
        eval {
            PVE::Storage::vdisk_free($cfg, "$storagename:$vol->{volname}");

            eval {
                run_command(
                    "zfs list $zpath\/$vol->{filename}",
                    outfunc => sub { },
                    errfunc => sub { },
                );
            };
            if (!$@) {
                $count++;
                warn "Test14 $name: vdisk still exists\n";
            }
        };
        if ($@) {
            $count++;
            warn "Test14 a: $@";
        }
    };
};
$tests->{14} = $test14;

my $test13 = sub {

    print "\nrun test13 \"vdisk_alloc\"\n";

    eval {
        my $tmp_volid = PVE::Storage::vdisk_alloc(
            $cfg, $storagename, "112", "raw", undef, 1024 * 1024,
        );

        if ($tmp_volid ne "$storagename:vm-112-disk-0") {
            die "volname:$tmp_volid don't match\n";
        }

        run_command(
            "zfs get -H volsize $zpath\/vm-112-disk-0",
            outfunc => sub {
                my $tmp = shift;
                if ($tmp !~ m/^$zpath\/vm-112-disk-0.*volsize.*1G.*$/) {
                    die "size don't match\n";
                }
            },
        );
    };
    if ($@) {
        $count++;
        warn "Test13 a: $@";
    }

    eval {
        my $tmp_volid = PVE::Storage::vdisk_alloc(
            $cfg, $storagename, "112", "raw", undef, 512 * 1024,
        );

        if ($tmp_volid ne "$storagename:vm-112-disk-1") {
            die "volname:$tmp_volid don't match\n";
        }

        run_command(
            "zfs get -H volsize $zpath\/vm-112-disk-1",
            outfunc => sub {
                my $tmp = shift;
                if ($tmp !~ m/^$zpath\/vm-112-disk-1.*volsize.*512M.*$/) {
                    die "size don't match\n";
                }
            },
        );
    };
    if ($@) {
        $count++;
        warn "Test13 b: $@";
    }

    eval {
        my $tmp_volid = PVE::Storage::vdisk_alloc(
            $cfg, $storagename, "113", "subvol", undef, 1024 * 1024,
        );

        if ($tmp_volid ne "$storagename:subvol-113-disk-0") {
            die "volname:$tmp_volid  don't match\n";
        }

        run_command(
            "zfs get -H refquota $zpath\/subvol-113-disk-0",
            outfunc => sub {
                my $tmp = shift;
                if ($tmp !~ m/^$zpath\/subvol-113-disk-0.*refquota.*1G.*$/) {
                    die "size don't match\n";
                }
            },
        );
    };
    if ($@) {
        $count++;
        warn "Test13 c: $@";
    }

    eval {
        my $tmp_volid = PVE::Storage::vdisk_alloc(
            $cfg, $storagename, "113", "subvol", undef, 2048 * 1024,
        );

        if ($tmp_volid ne "$storagename:subvol-113-disk-1") {
            die "volname:$tmp_volid  don't match\n";
        }

        run_command(
            "zfs get -H refquota $zpath\/subvol-113-disk-1",
            outfunc => sub {
                my $tmp = shift;
                if ($tmp !~ m/^$zpath\/subvol-113-disk-1.*refquota.*G.*$/) {
                    die "size don't match\n";
                }
            },
        );
    };
    if ($@) {
        $count++;
        warn "Test13 d: $@";
    }
};
$tests->{13} = $test13;

my $test12 = sub {

    print "\nrun test12 \"vdisk_create_base\"\n";

    foreach_testvol sub($name, $vol, $base, $basevol) {
        return if $vol->{isbase} || $base;

        my ($vmid, $volname, $filename) = $vol->@{qw(vmid volname filename)};

        my $basename = $filename;
        if ($basename =~ /(sub)?vol-(vm|ct)-/) {
            $basename = "base-$basename";
        } elsif ($basename =~ /^subvol-/) {
            $basename =~ s/^subvol-/basevol-/;
        } else {
            $basename =~ s/^vm-/base-/;
        }

        eval {
            my $tmp_volid = PVE::Storage::vdisk_create_base($cfg, "$storagename:$volname");

            die "returned volid '$tmp_volid' is not the expected '$storagename:$basename'"
                if $tmp_volid ne "$storagename:$basename";

            run_command("zfs list $zpath\/$basename", outfunc => sub { });
        };
        if ($@) {
            $count++;
            warn "Test12 $name: $@";
        }
    };
};
$tests->{12} = $test12;

my $test11 = sub {

    print "\nrun test11 \"volume_is_base\"\n";

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        return if $vol->{base};

        my ($vmid, $volname, $format) = $vol->@{qw(vmid volname format)};

        my $clone_vmid = $vmid + 50;

        if (!$vol->{isbase}) {
            eval {
                PVE::Storage::vdisk_clone(
                    $cfg, "$storagename:$volname", $clone_vmid, undef, undef,
                );
            };
            if (!$@) {
                $count++;
                warn "Test11 $name: clone_image only works on base images";
            }
            return;
        }

        print STDERR "Creating $clone_vmid for $name\n";
        eval {
            my $exp_filename =
                $format eq 'raw'
                ? "vm-$clone_vmid-disk-0"
                : "subvol-$clone_vmid-disk-0";
            my $exp_volid = "$storagename:$volname/$exp_filename";

            my $got_volid = PVE::Storage::vdisk_clone(
                $cfg, "$storagename:$volname", $clone_vmid, '__base__',
            );
            if ($exp_volid ne $got_volid) {
                $count++;
                warn "Test11 $name - expected '$exp_volid', got '$got_volid'";
            }

            if ($format eq 'raw') {
                run_command(
                    "zfs list -H -o volsize $zpath\/$exp_filename",
                    outfunc => sub {
                        my $line = shift;

                        chomp($line);
                        warn "Test11 $name: incorrect volsize" if $line ne "${volsize}G";
                    },
                );
            } else {
                run_command(
                    "zfs list -H -o refquota $zpath\/$exp_filename",
                    outfunc => sub {
                        my $line = shift;

                        chomp($line);
                        warn "Test11 $name: incorrect volsize" if $line ne "${volsize}G";
                    },
                );
            }
        };
        if ($@) {
            $count++;
            warn "Test11 $name: $@";
        }
    };
};
$tests->{11} = $test11;

my $test10 = sub {

    print "\nrun test10 \"volume_is_base\"\n";

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        my $isbase = !!$vol->{isbase};
        eval {
            if (!!$isbase != !!volume_is_base($cfg, "$storagename:$vol->{volname}")) {
                $count++;
                warn "Test10 $name: isbase=$isbase, but volume_is_base did not match";
            }

        };
        if ($@) {
            $count++;
            warn "Test10 $name: $@";
        }
    };
};
$tests->{10} = $test10;

my $test9 = sub {

    print "\nrun test9 \"parse_volume_id\"\n";

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        eval {
            my ($store, $disk) = PVE::Storage::parse_volume_id("$storagename:$vol->{volname}");

            if ($store ne $storagename || $disk ne $vol->{volname}) {
                $count++;
                warn "Test9 $name: parsing wrong";
            }

        };
        if ($@) {
            $count++;
            warn "Test9 $name: $@";
        }
    };
};
$tests->{9} = $test9;

my $test8 = sub {

    print "\nrun test8 \"parse_volname\"\n";

    foreach_testvol sub($name, $vol, $test_basename, $test_basevol) {
        eval {
            my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
                PVE::Storage::parse_volname($cfg, "$storagename:$vol->{volname}");
            my @foo = PVE::Storage::parse_volname($cfg, "$storagename:$vol->{volname}");

            my $got = {
                vtype => $vtype,
                name => $name,
                vmid => $vmid,
                basename => $basename,
                basevmid => $basevmid,
                isBase => !!$isBase,
                format => $format,
            };
            my $expected = {
                vtype => $vol->{type},
                name => $vol->{filename},
                vmid => $vol->{vmid},
                basename => $test_basevol->{volname},
                basevmid => $test_basevol->{vmid},
                isBase => !!$vol->{isbase},
                format => $vol->{format},
            };
            if (!is_deeply($got, $expected)) {
                $count++;
                warn "Test8 $name: parsing wrong for $storagename:$vol->{volname}";
            }

            if (defined($test_basename)) {
                if ($basename ne $test_basevol->{volname}) {
                    $count++;
                    warn "Test8 $name: parsed wrong basename,"
                        . " expected '$test_basevol->{volname}', got '$basename'";
                }
            }
        };
        if ($@) {
            $count++;
            warn "Test8 a: $@";
        }
    };
};
$tests->{8} = $test8;

my $test7 = sub {

    print "\nrun test7 \"volume_rollback\"\n";

    my $tmp_guid;
    my $parse_guid = sub {
        my ($line) = shift;

        if ($line =~ m/^Disk identifier \(GUID\)\: (.*)$/) {
            $tmp_guid = $1;
        }
    };

    my sub test_zvol($name, $vol) {
        $tmp_guid = undef;

        print "Testing snapshot/rollback on $name\n";

        my ($volname, $filename) = $vol->@{qw(volname filename)};

        eval {
            PVE::Storage::activate_volumes($cfg, ["$storagename:$volname"]);
            run_command(
                "sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$filename",
                outfunc => $parse_guid,
            );
            run_command("sgdisk -p \/dev\/zvol\/$zpath\/$filename", outfunc => $parse_guid);

            my $old_guid = $tmp_guid;
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap1');

            run_command(
                "sgdisk --randomize-guids \/dev\/zvol\/$zpath\/$filename",
                outfunc => $parse_guid,
            );

            PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$volname", 'snap1');
            PVE::Storage::activate_volumes($cfg, ["$storagename:$volname"]);
            $tmp_guid = undef;
            run_command("sgdisk -p \/dev\/zvol\/$zpath\/$filename", outfunc => $parse_guid);
            die "zvol wasn't rolled back\n" if $old_guid ne $tmp_guid;
        };
        if ($@) {
            $count++;
            warn "Test7 $name: $@";
        }
    }

    my sub test_subvol($name, $vol) {
        $tmp_guid = undef;

        print "Testing snapshot/rollback on $name\n";

        my ($volname, $filename) = $vol->@{qw(volname filename)};

        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap1');

            file_set_contents("/$mountpoint/$filename/test.txt", "hello\n");
            PVE::Storage::volume_snapshot_rollback($cfg, "$storagename:$volname", 'snap1');
            die "rollback was not performed\n" if -e "/$mountpoint/$filename/test.txt";
        };
        if ($@) {
            $count++;
            warn "Test7 $name: $@";
        }
    }

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        if ($vol->{kind} eq 'zvol') {
            test_zvol($name, $vol);
        } elsif ($vol->{kind} eq 'subvol') {
            test_subvol($name, $vol);
        } else {
            die "no tests for volume kind '$vol->{kind}'\n";
        }

        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$vol->{volname}", 'snap2');
            eval {
                PVE::Storage::volume_snapshot_rollback(
                    $cfg, "$storagename:$vol->{volname}", 'snap1',
                );
            };
            die "not allowed to rollback, but did anyway" if !$@;
        };
        if ($@) {
            $count++;
            warn "Test7 $name: $@";
        }
    };
};
$tests->{7} = $test7;

my $test6 = sub {

    print "\nrun test6 \"volume_rollback_is_possible\"\n";

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        my ($volname, $filename) = $vol->@{qw(volname filename)};

        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap1');

            my $blockers = [];
            my $res = PVE::Storage::volume_rollback_is_possible(
                $cfg, "$storagename:$volname", 'snap1', $blockers,
            );
            if (!$res) {
                $count++;
                warn "Test6 $name: Rollback should be possible";
            }
            if (scalar($blockers->@*) != 0) {
                $count++;
                warn "Test6 $name: 'blockers' should be empty";
            }
        };
        if ($@) {
            $count++;
            warn "Test6 $name: $@";
        }

        my $blockers = [];
        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap2');
            PVE::Storage::volume_rollback_is_possible(
                $cfg, "$storagename:$volname", 'snap1', $blockers,
            );
        };
        if (!$@) {
            $count++;
            warn "Test6 $name: Rollback should not be possible";
        } elsif (scalar($blockers->@*) != 1 || $blockers->[0] ne 'snap2') {
            $count++;
            warn "Test6 $name: 'blockers' should be ['snap2']";
        }

        $blockers = [];
        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap3');
            PVE::Storage::volume_rollback_is_possible(
                $cfg, "$storagename:$volname", 'snap1', $blockers,
            );
        };
        if (!$@) {
            $count++;
            warn "Test6 $name: Rollback should not be possible";
        }

        $blockers = [sort @$blockers];
        if (!is_deeply([sort @$blockers], [qw(snap2 snap3)])) {
            $count++;
            warn "Test6 $name: 'blockers' should contain 'snap2' and 'snap3'";
        }
    };
};
$tests->{6} = $test6;

my $test5 = sub {

    print "\nrun test5 \"volume_snapshot_delete\"\n";
    my $out = sub { return; };

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        my ($volname, $filename) = $vol->@{qw(volname filename)};

        eval {
            run_command("zfs snapshot $zpath\/$filename\@snap");

            PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$volname", 'snap');
            eval {
                run_command(
                    "zfs list $zpath\/$filename\@snap",
                    errfunc => $out,
                    outfunc => $out,
                );
            };
            if (!$@) {
                $count++;
                warn "Test5 $name: snapshot still exists";
            }
        };
        if ($@) {
            $count++;
            warn "Test5 a: $@";
        }

        return if !$vol->{isbase};

        print "###### Ignore Output if no 'Test5 $name:' is included ######\n";
        eval {
            PVE::Storage::volume_snapshot_delete($cfg, "$storagename:$volname", '__base__');
            eval { run_command("zfs list $zpath\/$filename\@__base__", outfunc => $out); };
            if ($@) {
                $count++;
                warn "Test5 $name: $@";
            }
        };
        if (!$@) {
            $count++;
            warn "Test5 $name: snapshot __base__ can be erased";
        }
        print "###### End Ignore #######\n";
    };
};
$tests->{5} = $test5;

my $test4 = sub {

    print "\nrun test4 \"volume_snapshot\"\n";
    my $out = sub { };

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        my ($volname, $filename) = $vol->@{qw(volname filename)};

        eval {
            PVE::Storage::volume_snapshot($cfg, "$storagename:$volname", 'snap');
            run_command("zfs list $zpath\/$filename\@snap", errmsg => $out, outfunc => $out);
        };
        if ($@) {
            $count++;
            warn "Test4 $name: $@";
        }
    };
};
$tests->{4} = $test4;

my $test3 = sub {

    print "\nrun test3 \"volume_has_feature\"\n";

    # snapshot: NOT for base

    my sub check_feature($name, $volname, $feature, $snapshot, $expected) {
        eval {
            if (
                !PVE::Storage::volume_has_feature(
                    $cfg, $feature, "$storagename:$volname", $snapshot, 0,
                ) != !$expected
            ) {
                my $res_msg = $expected ? 'available' : 'unavailable';
                my $snap_msg = $snapshot ? ' on snapshots' : '';
                die "'$feature' feature should be $res_msg$snap_msg";
            }
        };
        if ($@) {
            $count++;
            warn "Test3 $name [$feature]: $@";
        }
    }

    foreach_testvol sub($name, $vol, $basename, $basevol) {
        my $volname = $vol->{volname};

        my $snap_expect = !$vol->{isbase};
        check_feature($name, $volname, 'snapshot', undef, !$vol->{isbase});
        check_feature($name, $volname, 'snapshot', 'test', !!1);
        check_feature($name, $volname, 'clone', undef, $vol->{isbase});
        check_feature($name, $volname, 'clone', 'test', !!0);
        check_feature($name, $volname, 'template', undef, !$vol->{isbase});
        check_feature($name, $volname, 'template', 'test', !!0);
        check_feature($name, $volname, 'copy', undef, !!1);
        check_feature($name, $volname, 'copy', 'test', !!0);
        check_feature($name, $volname, 'sparseinit', undef, !!1);
        check_feature($name, $volname, 'sparseinit', 'test', !!0);
    };
};
$tests->{3} = $test3;

my $test2 = sub {

    print "\nrun test2 \"volume_resize\"\n";
    my $newsize = ($volsize + 1) * 1024 * 1024 * 1024;

    foreach_testvol sub($name, $vol, $base, $basevol) {
        my $volname = $vol->{volname};

        eval {
            if (($newsize / 1024) !=
                PVE::Storage::volume_resize($cfg, "$storagename:$volname", $newsize, 0)
            ) {
                $count++;
                warn "Test2 $name failed: volume_resize returned wrong size";
            }
            if ($newsize != PVE::Storage::volume_size_info($cfg, "$storagename:$volname")) {
                $count++;
                warn "Test2 $name failed: volume_size_info did not return the new size";
            }
        };
        if ($@) {
            $count++;
            warn "Test2 $name failed with an error: $@";
        }
    };
};
$tests->{2} = $test2;

my $test1 = sub {

    print "\nrun test1 \"volume_size_info\"\n";
    my $size = ($volsize * 1024 * 1024 * 1024);

    foreach_testvol sub($name, $vol, $base, $basevol) {
        my $volname = $vol->{volname};

        eval {
            my $got = PVE::Storage::volume_size_info($cfg, "$storagename:$volname");
            if ($size != $got) {
                die "volume_size_info() returned unexpected size (got $got, expected $size)";
            }
        };
        if ($@) {
            $count++;
            warn "Test1 $name: $@";
        }
    };
};
$tests->{1} = $test1;

sub setup_zfs {
    my $volume_list;

    my sub create_vol($name) {
        my $vol = $test_vols->{$name};

        my ($volname, $filename, $kind, $base, $isbase) =
            $vol->@{qw(volname filename kind base isbase)};

        my $base_filename;
        if (defined($base)) {
            $base_filename = $test_vols->{$base}->{filename}
                or die "missing base ('$base') for '$name'\n";
        }

        if ($kind eq 'zvol') {
            if (defined($base)) {
                print "[$name] create linked $kind $filename\n" if $verbose;
                run_command("zfs clone $zpath\/$base_filename$basesnap $zpath\/$filename");
            } else {
                print "[$name] create $kind $filename\n" if $verbose;
                run_command("zfs create -V${volsize}G $zpath\/$filename");

                if ($isbase) {
                    run_command("zfs snapshot $zpath\/$filename$basesnap");
                }
            }
        } elsif ($kind eq 'subvol') {
            if (defined($base)) {
                print "[$name] create linked $kind $filename\n" if $verbose;
                run_command(
                    "zfs clone $zpath\/$base_filename$basesnap $zpath\/$filename -o refquota=${volsize}G"
                );
            } else {
                print "[$name] create $kind $filename\n" if $verbose;
                run_command("zfs create -o refquota=${volsize}G $zpath\/$filename");

                if ($isbase) {
                    run_command("zfs snapshot $zpath\/$filename$basesnap");
                }
            }
        } else {
            die "unrecognized kind in test volume set: $kind\n";
        }

        push @$volume_list, "$storagename:$volname";
    }
    foreach_basevol sub($name, $vol) {
        create_vol($name);
    };
    foreach_testvol sub($name, $vol, $basename, $basevol) {
        return if $vol->{isbase};
        create_vol($name);
    };
    PVE::Storage::activate_volumes($cfg, $volume_list);
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
    eval { run_command("truncate -s 12G zpool.img"); };
    if ($@) {
        clean_up_zpool();
    }
    my $pwd = cwd();
    eval { run_command("zpool create -m \/$mountpoint $subvol $pwd\/zpool.img"); };
    if ($@) {
        clean_up_zpool();
    }
}

sub clean_up_zpool {

    eval { run_command("zpool destroy -f $subvol"); };
    if ($@) {
        warn $@;
    }
    unlink 'zpool.img';
}

sub volume_is_base {
    my ($cfg, $volid) = @_;

    my (undef, undef, undef, undef, undef, $isBase, undef) =
        PVE::Storage::parse_volname($cfg, $volid);

    return $isBase;
}

if ($> != 0) { #EUID
    warn "not root, skipping zfs tests\n";
    exit 0;
}

eval { run_command("zpool status"); };
if ($@) {
    warn "zpool status failed, not running tests: $@\n";
    exit 0;
}

setup_zpool();

my $time = time;
print "Start tests for ZFSPoolPlugin\n";

$cfg = {
    'ids' => {
        $storagename => {
            'content' => {
                'images' => 1,
                'rootdir' => 1,
            },
            'pool' => $subvol,
            'mountpoint' => "\/$mountpoint",
            'type' => 'zfspool',
        },
    },
    'order' => { 'zfstank99' => 1 },
};

$zpath = $subvol;

for (my $i = $start_test; $i <= $end_test; $i++) {
    setup_zfs();

    eval { $tests->{$i}(); };
    if (my $err = $@) {
        warn $err;
        $count++;
    }
    cleanup_zfs();
}

clean_up_zpool();

$time = time - $time;

done_testing();
print "Stop tests for ZFSPoolPlugin\n";
print "$count tests failed\n";
print "Time: ${time}s\n";

exit -1 if $count > 0;
