package PVE::Diskmanage;

use strict;
use warnings;
use PVE::ProcFSTools;
use Data::Dumper;
use Cwd qw(abs_path);

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

    my $disklist = get_disks($dev);

    die "'$disk' is not a valid local disk\n" if !defined($disklist->{$dev});
    return 1 if $disklist->{$dev}->{used};

    return 0;
}

sub get_smart_data {
    my ($disk) = @_;

    assert_blockdev($disk);
    my $smartdata = {};
    my $datastarted = 0;

    my $returncode = 0;
    eval {
	$returncode = run_command([$SMARTCTL, '-H', '-A', '-f', 'brief', $disk], noerr => 1, outfunc => sub{
	    my ($line) = @_;

# ATA SMART attributes, e.g.:
# ID# ATTRIBUTE_NAME          FLAGS    VALUE WORST THRESH FAIL RAW_VALUE
#   1 Raw_Read_Error_Rate     POSR-K   100   100   000    -    0
	    if ($datastarted && $line =~ m/^([ \d]{2}\d)\s+(\S+)\s+(\S{6})\s+(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/) {
		my $entry = {};
		$entry->{name} = $2 if defined $2;
		$entry->{flags} = $3 if defined $3;
		# the +0 makes a number out of the strings
		$entry->{value} = $4+0 if defined $4;
		$entry->{worst} = $5+0 if defined $5;
		$entry->{threshold} = $6+0 if defined $6;
		$entry->{fail} = $7 if defined $7;
		$entry->{raw} = $8 if defined $8;
		$entry->{id} = $1 if defined $1;
		push @{$smartdata->{attributes}}, $entry;
	    } elsif ($line =~ m/self\-assessment test result: (.*)$/) {
		$smartdata->{health} = $1;
	    } elsif ($line =~ m/Vendor Specific SMART Attributes with Thresholds:/) {
		$datastarted = 1;
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
    return $smartdata;
}

sub get_smart_health {
    my ($disk) = @_;

    return "NOT A DEVICE" if !assert_blockdev($disk, 1);

    my $message;

    run_command([$SMARTCTL, '-H', $disk], noerr => 1, outfunc => sub {
	my ($line) = @_;

	if ($line =~ m/test result: (.*)$/) {
	    $message = $1;
	} elsif ($line =~ m/open device: (.*) failed: (.*)$/) {
	    $message = "FAILED TO OPEN";
	} elsif ($line =~ m/^SMART Disabled/) {
	    $message = "SMART DISABLED";
	}
    });

    return $message;
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

    dir_glob_foreach('/dev/disk/by-parttypeuuid', "$journal_uuid\..+", sub {
	my ($entry) = @_;
	my $real_dev = abs_path("/dev/disk/by-parttypeuuid/$entry");
	$journalhash->{$real_dev} = 1;
    });

    return $journalhash;
}

sub get_udev_info {
    my ($dev) = @_;

    my $info = "";
    my $data = {};
    eval {
	run_command([$UDEVADM, 'info', '-n', $dev, '--query', 'all'], outfunc => sub {
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

    $data->{wwn} = 'unknown';
    if ($info =~ m/^E: ID_WWN=(.*)$/m) {
	$data->{wwn} = $1;
    }

    return $data;
}

sub get_sysdir_info {
    my ($sysdir) = @_;

    my $data = {};

    my $size = file_read_firstline("$sysdir/size");
    return undef if !$size;

    # linux always considers sectors to be 512 bytes,
    # independently of real block size
    $data->{size} = $size * 512;

    # dir/queue/rotational should be 1 for hdd, 0 for ssd
    $data->{rotational} = file_read_firstline("$sysdir/queue/rotational");

    $data->{vendor} = file_read_firstline("$sysdir/device/vendor") || 'unknown';
    $data->{model} = file_read_firstline("$sysdir/device/model") || 'unknown';

    return $data;
}

sub get_disks {
    my ($disk) = @_;
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

    my $dir_is_empty = sub {
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
    };

    my $journalhash = get_ceph_journals();

    my $zfslist = get_zfs_devices();

    my $lvmlist = get_lvm_devices();

    dir_glob_foreach('/sys/block', '.*', sub {
	my ($dev) = @_;
	return if defined($disk) && $disk ne $dev;
	# whitelisting following devices
	# hdX: ide block device
	# sdX: sd block device
	# vdX: virtual block device
	# xvdX: xen virtual block device
	# nvmeXnY: nvme devices
	# cXnY: cciss devices
	return if $dev !~ m/^(h|s|x?v)d[a-z]+$/ &&
		  $dev !~ m/^nvme\d+n\d+$/ &&
		  $dev !~ m/^c\d+d\d+$/;

	my $data = get_udev_info($dev);
	return if !defined($data);
	my $devpath = $data->{devpath};

	my $sysdir = "/sys/block/$dev";

	return if ! -d "$sysdir/device";

	# we do not want iscsi devices
	return if readlink($sysdir) =~ m|host[^/]*/session[^/]*|;

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
	my $wearout;
	eval {
	    if ($type eq 'ssd' && !defined($disk)) {
		# if we have an ssd we try to get the wearout indicator
		$wearout = 'N/A';
		my $smartdata = get_smart_data($devpath);
		$health = $smartdata->{health};
		foreach my $attr (@{$smartdata->{attributes}}) {
		    # ID 233 is media wearout indicator on intel and sandisk
		    # ID 177 is media wearout indicator on samsung
		    next if ($attr->{id} != 233 && $attr->{id} != 177);
		    next if ($attr->{name} !~ m/wear/i);
		    $wearout = $attr->{value};

		    # prefer the 233 value
		    last if ($attr->{id} == 233);
		}
	    } elsif (!defined($disk)) {
		# we do not need smart data if we check a single disk
		# because this functionality is only for disk_is_used
		$health = get_smart_health($devpath) if !defined($disk);
	    }
	};

	my $used;

	$used = 'LVM' if $lvmlist->{$devpath};

	$used = 'mounted' if &$dev_is_mounted($devpath);

	$used = 'ZFS' if $zfslist->{$devpath};

	$disklist->{$dev} = {
	    vendor => $sysdata->{vendor},
	    model => $sysdata->{model},
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

	my $journal_count = 0;

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

	    $journal_count++ if $journalhash->{"$partpath/$part"};

	    if (!&$dir_is_empty("$sysdir/$part/holders") && !$found_lvm)  {
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
	$used = 'Device Mapper' if !$used && !&$dir_is_empty("$sysdir/holders");

	$disklist->{$dev}->{used} = $used if $used;
	$disklist->{$dev}->{osdid} = $osdid;
	$disklist->{$dev}->{journals} = $journal_count;
    });

    return $disklist;

}

1;
