package PVE::Diskmanage;

use strict;
use warnings;
use PVE::ProcFSTools;
use Data::Dumper;
use Cwd qw(abs_path);
use Fcntl ':mode';

use PVE::Tools qw(extract_param run_command file_get_contents file_read_firstline dir_glob_regex dir_glob_foreach trim);

my $SMARTCTL = "/usr/sbin/smartctl";
my $ZPOOL = "/sbin/zpool";
my $SGDISK = "/sbin/sgdisk";
my $PVS = "/sbin/pvs";
my $UDEVADM = "/bin/udevadm";

sub verify_blockdev_path {
    my ($rel_path) = @_;

    die "missing path" if !$rel_path;
    my $path = abs_path($rel_path);
    die "failed to get absolute path to $rel_path\n" if !$path;

    die "got unusual device path '$path'\n" if $path !~  m|^/dev/(.*)$|;

    $path = "/dev/$1"; # untaint

    assert_blockdev($path);

    return $path;
}

sub assert_blockdev {
    my ($dev, $noerr) = @_;

    if ($dev !~ m|^/dev/| || !(-b $dev)) {
	return undef if $noerr;
	die "not a valid block device\n";
    }

    return 1;
}

sub init_disk {
    my ($disk, $uuid) = @_;

    assert_blockdev($disk);

    # we should already have checked if it is in use in the api call
    # but we check again for safety
    die "disk $disk is already in use\n" if disk_is_used($disk);

    my $id = $uuid || 'R';
    run_command([$SGDISK, $disk, '-U', $id]);
    return 1;
}

sub disk_is_used {
    my ($disk) = @_;

    my $dev = $disk;
    $dev =~ s|^/dev/||;

    my $disklist = get_disks($dev, 1);

    die "'$disk' is not a valid local disk\n" if !defined($disklist->{$dev});
    return 1 if $disklist->{$dev}->{used};

    return 0;
}

sub get_smart_data {
    my ($disk, $healthonly) = @_;

    assert_blockdev($disk);
    my $smartdata = {};
    my $type;

    my $returncode = 0;

    $disk =~ s/n\d+$//
        if $disk =~ m!^/dev/nvme\d+n\d+$!;

    my $cmd = [$SMARTCTL, '-H'];
    push @$cmd, '-A', '-f', 'brief' if !$healthonly;
    push @$cmd, $disk;

    eval {
	$returncode = run_command($cmd, noerr => 1, outfunc => sub{
	    my ($line) = @_;

# ATA SMART attributes, e.g.:
# ID# ATTRIBUTE_NAME          FLAGS    VALUE WORST THRESH FAIL RAW_VALUE
#   1 Raw_Read_Error_Rate     POSR-K   100   100   000    -    0
#
# SAS and NVME disks, e.g.:
# Data Units Written:                 5,584,952 [2.85 TB]
# Accumulated start-stop cycles:  34

	    if (defined($type) && $type eq 'ata' && $line =~ m/^([ \d]{2}\d)\s+(\S+)\s+(\S{6})\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(.*)$/) {
		my $entry = {};


		$entry->{name} = $2 if defined $2;
		$entry->{flags} = $3 if defined $3;
		# the +0 makes a number out of the strings
		$entry->{value} = $4+0 if defined $4;
		$entry->{worst} = $5+0 if defined $5;
		# some disks report the default threshold as --- instead of 000
		if (defined($6) && $6 eq '---') {
		    $entry->{threshold} = 0;
		} else {
		    $entry->{threshold} = $6+0 if defined $6;
		}
		$entry->{fail} = $7 if defined $7;
		$entry->{raw} = $8 if defined $8;
		$entry->{id} = $1 if defined $1;
		push @{$smartdata->{attributes}}, $entry;
	    } elsif ($line =~ m/(?:Health Status|self\-assessment test result): (.*)$/ ) {
		$smartdata->{health} = $1;
	    } elsif ($line =~ m/Vendor Specific SMART Attributes with Thresholds:/) {
		$type = 'ata';
		delete $smartdata->{text};
	    } elsif ($line =~ m/=== START OF (READ )?SMART DATA SECTION ===/) {
		$type = 'text';
	    } elsif (defined($type) && $type eq 'text') {
		$smartdata->{text} = '' if !defined $smartdata->{text};
		$smartdata->{text} .= "$line\n";
	    } elsif ($line =~ m/SMART Disabled/) {
		$smartdata->{health} = "SMART Disabled";
	    }
	});
    };
    my $err = $@;

    # bit 0 and 1 mark an severe smartctl error
    # all others are for disk status, so ignore them
    # see smartctl(8)
    if ((defined($returncode) && ($returncode & 0b00000011)) || $err) {
	die "Error getting S.M.A.R.T. data: Exit code: $returncode\n";
    }

    $smartdata->{type} = $type;

    return $smartdata;
}

sub get_zfs_devices {
    my $list = {};

    # use zpool and parttype uuid,
    # because log and cache do not have
    # zfs type uuid
    eval {
	run_command([$ZPOOL, 'list', '-HPLv'], outfunc => sub {
	     my ($line) = @_;

	     if ($line =~ m|^\t([^\t]+)\t|) {
		$list->{$1} = 1;
	     }
	});
    };

    # only warn here,
    # because maybe zfs tools are not installed
    warn "$@\n" if $@;

    my $applezfsuuid = "6a898cc3-1dd2-11b2-99a6-080020736631";
    my $bsdzfsuuid = "516e7cba-6ecf-11d6-8ff8-00022d09712b";

    dir_glob_foreach('/dev/disk/by-parttypeuuid', "($applezfsuuid|$bsdzfsuuid)\..+", sub {
	my ($entry) = @_;
	my $real_dev = abs_path("/dev/disk/by-parttypeuuid/$entry");
	$list->{$real_dev} = 1;
    });

    return $list;
}

sub get_lvm_devices {
    my $list = {};
    eval {
	run_command([$PVS, '--noheadings', '--readonly', '-o', 'pv_name'], outfunc => sub{
	    my ($line) = @_;
	    $line = trim($line);
	    if ($line =~ m|^/dev/|) {
		$list->{$line} = 1;
	    }
	});
    };

    # if something goes wrong, we do not want
    # to give up, but indicate an error has occured
    warn "$@\n" if $@;

    my $lvmuuid = "e6d6d379-f507-44c2-a23c-238f2a3df928";

    dir_glob_foreach('/dev/disk/by-parttypeuuid', "$lvmuuid\..+", sub {
	my ($entry) = @_;
	my $real_dev = abs_path("/dev/disk/by-parttypeuuid/$entry");
	$list->{$real_dev} = 1;
    });

    return $list;
}

sub get_ceph_journals {
    my $journalhash = {};

    my $journal_uuid = '45b0969e-9b03-4f30-b4c6-b4b80ceff106';
    my $db_uuid = '30cd0809-c2b2-499c-8879-2d6b78529876';
    my $wal_uuid = '5ce17fce-4087-4169-b7ff-056cc58473f9';
    my $block_uuid = 'cafecafe-9b03-4f30-b4c6-b4b80ceff106';

    dir_glob_foreach('/dev/disk/by-parttypeuuid', "($journal_uuid|$db_uuid|$wal_uuid|$block_uuid)\..+", sub {
	my ($entry, $type) = @_;
	my $real_dev = abs_path("/dev/disk/by-parttypeuuid/$entry");
	if ($type eq $journal_uuid) {
	    $journalhash->{$real_dev} = 1;
	} elsif ($type eq $db_uuid) {
	    $journalhash->{$real_dev} = 2;
	} elsif ($type eq $wal_uuid) {
	    $journalhash->{$real_dev} = 3;
	} elsif ($type eq $block_uuid) {
	    $journalhash->{$real_dev} = 4;
	}
    });

    return $journalhash;
}

sub get_udev_info {
    my ($dev) = @_;

    my $info = "";
    my $data = {};
    eval {
	run_command([$UDEVADM, 'info', '-p', $dev, '--query', 'all'], outfunc => sub {
	    my ($line) = @_;
	    $info .= "$line\n";
	});
    };
    warn $@ if $@;
    return undef if !$info;

    return undef if $info !~ m/^E: DEVTYPE=disk$/m;
    return undef if $info =~ m/^E: ID_CDROM/m;

    # we use this, because some disks are not simply in /dev
    # e.g. /dev/cciss/c0d0
    if ($info =~ m/^E: DEVNAME=(\S+)$/m) {
	$data->{devpath} = $1;
    }
    return if !defined($data->{devpath});

    $data->{serial} = 'unknown';
    if ($info =~ m/^E: ID_SERIAL_SHORT=(\S+)$/m) {
	$data->{serial} = $1;
    }

    $data->{gpt} = 0;
    if ($info =~ m/^E: ID_PART_TABLE_TYPE=gpt$/m) {
	$data->{gpt} = 1;
    }

    # detect SSD
    $data->{rpm} = -1;
    if ($info =~ m/^E: ID_ATA_ROTATION_RATE_RPM=(\d+)$/m) {
	$data->{rpm} = $1;
    }

    if ($info =~ m/^E: ID_BUS=usb$/m) {
	$data->{usb} = 1;
    }

    if ($info =~ m/^E: ID_MODEL=(.+)$/m) {
	$data->{model} = $1;
    }

    $data->{wwn} = 'unknown';
    if ($info =~ m/^E: ID_WWN=(.*)$/m) {
	$data->{wwn} = $1;
    }

    return $data;
}

sub get_sysdir_info {
    my ($sysdir) = @_;

    return undef if ! -d "$sysdir/device";

    my $data = {};

    my $size = file_read_firstline("$sysdir/size");
    return undef if !$size;

    # linux always considers sectors to be 512 bytes,
    # independently of real block size
    $data->{size} = $size * 512;

    # dir/queue/rotational should be 1 for hdd, 0 for ssd
    $data->{rotational} = file_read_firstline("$sysdir/queue/rotational") // -1;

    $data->{vendor} = file_read_firstline("$sysdir/device/vendor") || 'unknown';
    $data->{model} = file_read_firstline("$sysdir/device/model") || 'unknown';

    return $data;
}

sub get_wear_leveling_info {
    my ($attributes, $model) = @_;

    my $wearout;

    my $vendormap = {
	'kingston' => 231,
	'samsung' => 177,
	'intel' => 233,
	'sandisk' => 233,
	'crucial' => 202,
	'default' => 233,
    };

    # find target attr id

    my $attrid;

    foreach my $vendor (keys %$vendormap) {
	if ($model =~ m/$vendor/i) {
	    $attrid = $vendormap->{$vendor};
	    # found the attribute
	    last;
	}
    }

    if (!$attrid) {
	$attrid = $vendormap->{default};
    }

    foreach my $attr (@$attributes) {
	next if $attr->{id} != $attrid;
	$wearout = $attr->{value};
	last;
    }

    return $wearout;
}

sub dir_is_empty {
    my ($dir) = @_;

    my $dh = IO::Dir->new ($dir);
    return 1 if !$dh;

    while (defined(my $tmp = $dh->read)) {
	next if $tmp eq '.' || $tmp eq '..';
	$dh->close;
	return 0;
    }
    $dh->close;
    return 1;
}

sub get_disks {
    my ($disk, $nosmart) = @_;
    my $disklist = {};

    my $mounted = {};

    my $mounts = PVE::ProcFSTools::parse_proc_mounts();

    foreach my $mount (@$mounts) {
	next if $mount->[0] !~ m|^/dev/|;
	$mounted->{abs_path($mount->[0])} = $mount->[1];
    };

    my $dev_is_mounted = sub {
	my ($dev) = @_;
	return $mounted->{$dev};
    };

    my $journalhash = get_ceph_journals();

    my $zfslist = get_zfs_devices();

    my $lvmlist = get_lvm_devices();

    # we get cciss/c0d0 but need cciss!c0d0
    if (defined($disk) && $disk =~ m|^cciss/|) {
	$disk =~ s|cciss/|cciss!|;
    }

    dir_glob_foreach('/sys/block', '.*', sub {
	my ($dev) = @_;
	return if defined($disk) && $disk ne $dev;
	# whitelisting following devices
	# hdX: ide block device
	# sdX: sd block device
	# vdX: virtual block device
	# xvdX: xen virtual block device
	# nvmeXnY: nvme devices
	# cciss!cXnY: cciss devices
	return if $dev !~ m/^(h|s|x?v)d[a-z]+$/ &&
		  $dev !~ m/^nvme\d+n\d+$/ &&
		  $dev !~ m/^cciss\!c\d+d\d+$/;

	my $data = get_udev_info("/sys/block/$dev");
	return if !defined($data);
	my $devpath = $data->{devpath};

	my $sysdir = "/sys/block/$dev";

	# we do not want iscsi devices
	return if -l $sysdir && readlink($sysdir) =~ m|host[^/]*/session[^/]*|;

	my $sysdata = get_sysdir_info($sysdir);
	return if !defined($sysdata);

	my $type = 'unknown';

	if ($sysdata->{rotational} == 0) {
	    $type = 'ssd';
	    $data->{rpm} = 0;
	} elsif ($sysdata->{rotational} == 1) {
	    if ($data->{rpm} != -1) {
		$type = 'hdd';
	    } elsif ($data->{usb}) {
		$type = 'usb';
		$data->{rpm} = 0;
	    }
	}

	my $health = 'UNKNOWN';
	my $wearout = 'N/A';

	if (!$nosmart) {
	    eval {
		my $smartdata = get_smart_data($devpath, ($type ne 'ssd'));
		$health = $smartdata->{health} if $smartdata->{health};

		if ($type eq 'ssd') {
		    # if we have an ssd we try to get the wearout indicator
		    my $wearval = get_wear_leveling_info($smartdata->{attributes}, $data->{model} || $sysdir->{model});
		    $wearout = $wearval if $wearval;
		}
	    };
	}

	my $used;

	$used = 'LVM' if $lvmlist->{$devpath};

	$used = 'mounted' if &$dev_is_mounted($devpath);

	$used = 'ZFS' if $zfslist->{$devpath};

	# we replaced cciss/ with cciss! above
	# but in the result we need cciss/ again
	# because the caller might want to check the
	# result again with the original parameter
	if ($dev =~ m|^cciss!|) {
	    $dev =~ s|^cciss!|cciss/|;
	}

	$disklist->{$dev} = {
	    vendor => $sysdata->{vendor},
	    model => $data->{model} || $sysdata->{model},
	    size => $sysdata->{size},
	    serial => $data->{serial},
	    gpt => $data->{gpt},
	    rpm => $data->{rpm},
	    type =>  $type,
	    wwn => $data->{wwn},
	    health => $health,
	    devpath => $devpath,
	    wearout => $wearout,
	};

	my $osdid = -1;
	my $bluestore = 0;

	my $journal_count = 0;
	my $db_count = 0;
	my $wal_count = 0;

	my $found_partitions;
	my $found_lvm;
	my $found_mountpoints;
	my $found_zfs;
	my $found_dm;
	my $partpath = $devpath;

	# remove part after last / to
	# get the base path for the partitions
	# e.g. from /dev/cciss/c0d0 get /dev/cciss
	$partpath =~ s/\/[^\/]+$//;

	dir_glob_foreach("$sysdir", "$dev.+", sub {
	    my ($part) = @_;

	    $found_partitions = 1;

	    if (my $mp = &$dev_is_mounted("$partpath/$part")) {
		$found_mountpoints = 1;
		if ($mp =~ m|^/var/lib/ceph/osd/ceph-(\d+)$|) {
		    $osdid = $1;
		}
	    }

	    if ($lvmlist->{"$partpath/$part"}) {
		$found_lvm = 1;
	    }

	    if ($zfslist->{"$partpath/$part"}) {
		$found_zfs = 1;
	    }

	    if ($journalhash->{"$partpath/$part"}) {
		$journal_count++ if $journalhash->{"$partpath/$part"} == 1;
		$db_count++ if $journalhash->{"$partpath/$part"} == 2;
		$wal_count++ if $journalhash->{"$partpath/$part"} == 3;
		$bluestore = 1 if $journalhash->{"$partpath/$part"} == 4;
	    }

	    if (!dir_is_empty("$sysdir/$part/holders") && !$found_lvm)  {
		$found_dm = 1;
	    }
	});

	$used = 'mounted' if $found_mountpoints && !$used;
	$used = 'LVM' if $found_lvm && !$used;
	$used = 'ZFS' if $found_zfs && !$used;
	$used = 'Device Mapper' if $found_dm && !$used;
	$used = 'partitions' if $found_partitions && !$used;

	# multipath, software raid, etc.
	# this check comes in last, to show more specific info
	# if we have it
	$used = 'Device Mapper' if !$used && !dir_is_empty("$sysdir/holders");

	$disklist->{$dev}->{used} = $used if $used;
	$disklist->{$dev}->{osdid} = $osdid;
	$disklist->{$dev}->{journals} = $journal_count if $journal_count;
	$disklist->{$dev}->{bluestore} = $bluestore if $osdid != -1;
	$disklist->{$dev}->{db} = $db_count if $db_count;
	$disklist->{$dev}->{wal} = $wal_count if $wal_count;
    });

    return $disklist;

}

sub get_partnum {
    my ($part_path) = @_;

    my ($mode, $rdev) = (stat($part_path))[2,6];

    next if !$mode || !S_ISBLK($mode) || !$rdev;
    my $major = int($rdev / 0x100);
    my $minor = $rdev % 0x100;
    my $partnum_path = "/sys/dev/block/$major:$minor/";

    my $partnum;

    $partnum = file_read_firstline("${partnum_path}partition");

    die "Partition does not exists\n" if !defined($partnum);

    #untaint and ensure it is a int
    if ($partnum =~ m/(\d+)/) {
	$partnum = $1;
	die "Partition number $partnum is invalid\n" if $partnum > 128;
    } else {
	die "Failed to get partition number\n";
    }

    return $partnum;
}

sub get_blockdev {
    my ($part_path) = @_;

    my $dev = $1 if $part_path =~ m|^/dev/(.*)$|;
    my $link = readlink "/sys/class/block/$dev";
    my $block_dev = $1 if $link =~ m|([^/]*)/$dev$|;

    die "Can't parse parent device\n" if !defined($block_dev);
    die "No valid block device\n" if index($dev, $block_dev) == -1;

    $block_dev = "/dev/$block_dev";
    die "Block device does not exsists\n" if !(-b $block_dev);

    return $block_dev;
}

1;
