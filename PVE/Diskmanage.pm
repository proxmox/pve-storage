package PVE::Diskmanage;

use strict;
use warnings;

use PVE::ProcFSTools;
use Data::Dumper;
use Cwd qw(abs_path);
use Fcntl ':mode';
use File::stat;
use JSON;

use PVE::Tools qw(extract_param run_command file_get_contents file_read_firstline dir_glob_regex dir_glob_foreach trim);

my $SMARTCTL = "/usr/sbin/smartctl";
my $ZPOOL = "/sbin/zpool";
my $SGDISK = "/sbin/sgdisk";
my $PVS = "/sbin/pvs";
my $LVS = "/sbin/lvs";
my $LSBLK = "/bin/lsblk";

sub check_bin {
    my ($path) = @_;

    return -x $path;
}

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

    if ($disk =~ m!^/dev/(nvme\d+n\d+)$!) {
	my $info = get_sysdir_info("/sys/block/$1");
	$disk = "/dev/".($info->{device}
	    or die "failed to get nvme controller device for $disk\n");
    }

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
		# extract wearout from nvme/sas text, allow for decimal values
		if ($line =~ m/Percentage Used(?: endurance indicator)?:\s*(\d+(?:\.\d+)?)\%/i) {
		    $smartdata->{wearout} = 100 - $1;
		}
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

sub get_lsblk_info() {
    my $cmd = [$LSBLK, '--json', '-o', 'path,parttype,fstype'];
    my $output = "";
    my $res = {};
    eval {
	run_command($cmd, outfunc => sub {
	    my ($line) = @_;
	    $output .= "$line\n";
	});
    };
    warn "$@\n" if $@;
    return $res if $output eq '';

    my $parsed = eval { decode_json($output) };
    warn "$@\n" if $@;
    my $list = $parsed->{blockdevices} // [];

    $res = { map {
	$_->{path} => {
	    parttype => $_->{parttype},
	    fstype => $_->{fstype}
	}
    } @{$list} };

    return $res;
}

my $get_devices_by_partuuid = sub {
    my ($lsblk_info, $uuids, $res) = @_;

    $res = {} if !defined($res);

    foreach my $dev (sort keys %{$lsblk_info}) {
	my $uuid = $lsblk_info->{$dev}->{parttype};
	next if !defined($uuid) || !defined($uuids->{$uuid});
	$res->{$dev} = $uuids->{$uuid};
    }

    return $res;
};

sub get_zfs_devices {
    my ($lsblk_info) = @_;
    my $res = {};

    return {} if !check_bin($ZPOOL);

    # use zpool and parttype uuid,
    # because log and cache do not have
    # zfs type uuid
    eval {
	run_command([$ZPOOL, 'list', '-HPLv'], outfunc => sub {
	     my ($line) = @_;

	     if ($line =~ m|^\t([^\t]+)\t|) {
		$res->{$1} = 1;
	     }
	});
    };

    # only warn here,
    # because maybe zfs tools are not installed
    warn "$@\n" if $@;

    my $uuids = {
	"6a898cc3-1dd2-11b2-99a6-080020736631" => 1, # apple
	"516e7cba-6ecf-11d6-8ff8-00022d09712b" => 1, # bsd
    };


    $res = $get_devices_by_partuuid->($lsblk_info, $uuids, $res);

    return $res;
}

sub get_lvm_devices {
    my ($lsblk_info) = @_;
    my $res = {};
    eval {
	run_command([$PVS, '--noheadings', '--readonly', '-o', 'pv_name'], outfunc => sub{
	    my ($line) = @_;
	    $line = trim($line);
	    if ($line =~ m|^/dev/|) {
		$res->{$line} = 1;
	    }
	});
    };

    # if something goes wrong, we do not want
    # to give up, but indicate an error has occurred
    warn "$@\n" if $@;

    my $uuids = {
	"e6d6d379-f507-44c2-a23c-238f2a3df928" => 1,
    };

    $res = $get_devices_by_partuuid->($lsblk_info, $uuids, $res);

    return $res;
}

sub get_ceph_journals {
    my ($lsblk_info) = @_;
    my $res = {};

    my $uuids = {
	'45b0969e-9b03-4f30-b4c6-b4b80ceff106' => 1, # journal
	'30cd0809-c2b2-499c-8879-2d6b78529876' => 2, # db
	'5ce17fce-4087-4169-b7ff-056cc58473f9' => 3, # wal
	'cafecafe-9b03-4f30-b4c6-b4b80ceff106' => 4, # block
    };

    $res = $get_devices_by_partuuid->($lsblk_info, $uuids, $res);

    return $res;
}

# reads the lv_tags and matches them with the devices
sub get_ceph_volume_infos {
    my $result = {};

    my $cmd = [ $LVS, '-S', 'lv_name=~^osd-', '-o', 'devices,lv_name,lv_tags',
	       '--noheadings', '--readonly', '--separator', ';' ];

    run_command($cmd, outfunc => sub {
	my $line = shift;
	$line =~ s/(?:^\s+)|(?:\s+$)//g; # trim whitespaces

	my $fields = [ split(';', $line) ];

	# lvs syntax is /dev/sdX(Y) where Y is the start (which we do not need)
	my ($dev) = $fields->[0] =~ m|^(/dev/[a-z]+[^(]*)|;
	if ($fields->[1] =~ m|^osd-([^-]+)-|) {
	    my $type = $1;
	    # $result autovivification is wanted, to not creating empty hashes
	    if (($type eq 'block' || $type eq 'data') && $fields->[2] =~ m/ceph.osd_id=([^,]+)/) {
		$result->{$dev}->{osdid} = $1;
		$result->{$dev}->{bluestore} = ($type eq 'block');
		if ($fields->[2] =~ m/ceph\.encrypted=1/) {
		    $result->{$dev}->{encrypted} = 1;
		}
	    } else {
		# undef++ becomes '1' (see `perldoc perlop`: Auto-increment)
		$result->{$dev}->{$type}++;
	    }
	}
    });

    return $result;
}

sub get_udev_info {
    my ($dev) = @_;

    my $info = "";
    my $data = {};
    eval {
	run_command(['udevadm', 'info', '-p', $dev, '--query', 'all'], outfunc => sub {
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

    if ($info =~ m/^E: DEVLINKS=(.+)$/m) {
	my @devlinks = grep(m#^/dev/disk/by-id/(ata|scsi|nvme(?!-eui))#, split (/ /, $1));
	$data->{by_id_link} = $devlinks[0] if defined($devlinks[0]);
    }

    return $data;
}

sub get_sysdir_size {
    my ($sysdir) = @_;

    my $size = file_read_firstline("$sysdir/size");
    return if !$size;

    # linux always considers sectors to be 512 bytes,
    # independently of real block size
    return $size * 512;
}

sub get_sysdir_info {
    my ($sysdir) = @_;

    return undef if ! -d "$sysdir/device";

    my $data = {};

    $data->{size} = get_sysdir_size($sysdir) or return;

    # dir/queue/rotational should be 1 for hdd, 0 for ssd
    $data->{rotational} = file_read_firstline("$sysdir/queue/rotational") // -1;

    $data->{vendor} = file_read_firstline("$sysdir/device/vendor") || 'unknown';
    $data->{model} = file_read_firstline("$sysdir/device/model") || 'unknown';

    if (defined(my $device = readlink("$sysdir/device"))) {
	# strip directory and untaint:
	($data->{device}) = $device =~ m!([^/]+)$!;
    }

    return $data;
}

sub get_wear_leveling_info {
    my ($smartdata) = @_;
    my $attributes = $smartdata->{attributes};

    if (defined($smartdata->{wearout})) {
	return $smartdata->{wearout};
    }

    my $wearout;

    # Common register names that represent percentage values of potential
    # failure indicators used in drivedb.h of smartmontool's. Order matters,
    # as some drives may have multiple definitions
    my @wearoutregisters = (
	"Media_Wearout_Indicator",
	"SSD_Life_Left",
	"Wear_Leveling_Count",
	"Perc_Write\/Erase_Ct_BC",
	"Perc_Rated_Life_Remain",
	"Remaining_Lifetime_Perc",
	"Percent_Lifetime_Remain",
	"Lifetime_Left",
	"PCT_Life_Remaining",
	"Lifetime_Remaining",
	"Percent_Life_Remaining",
	"Percent_Lifetime_Used",
	"Perc_Rated_Life_Used"
    );

    # Search for S.M.A.R.T. attributes for known register
    foreach my $register (@wearoutregisters) {
	last if defined $wearout;
	foreach my $attr (@$attributes) {
	   next if $attr->{name} !~ m/$register/;
	   $wearout = $attr->{value};
	   last;
	}
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

sub is_iscsi {
    my ($sysdir) = @_;

    if (-l $sysdir && readlink($sysdir) =~ m|host[^/]*/session[^/]*|) {
	return 1;
    }

    return 0;
}

my sub is_ssdlike {
    my ($type) = @_;
    return $type eq 'ssd' || $type eq 'nvme';
}

sub get_disks {
    my ($disks, $nosmart, $include_partitions) = @_;
    my $disklist = {};

    my $mounted = {};

    my $mounts = PVE::ProcFSTools::parse_proc_mounts();

    foreach my $mount (@$mounts) {
	next if $mount->[0] !~ m|^/dev/|;
	$mounted->{abs_path($mount->[0])} = $mount->[1];
    };

    my $lsblk_info = get_lsblk_info();

    my $journalhash = get_ceph_journals($lsblk_info);
    my $ceph_volume_infos = get_ceph_volume_infos();

    my $zfshash = get_zfs_devices($lsblk_info);

    my $lvmhash = get_lvm_devices($lsblk_info);

    my $disk_regex = ".*";
    if (defined($disks)) {
	if (!ref($disks)) {
	    $disks = [ $disks ];
	} elsif (ref($disks) ne 'ARRAY') {
	    die "disks is not a string or array reference\n";
	}
	# we get cciss/c0d0 but need cciss!c0d0
	$_ =~ s|cciss/|cciss!| for @$disks;

	$disk_regex = "(?:" . join('|', @$disks) . ")";
    }

    dir_glob_foreach('/sys/block', $disk_regex, sub {
	my ($dev) = @_;
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
	return if is_iscsi($sysdir);

	my $sysdata = get_sysdir_info($sysdir);
	return if !defined($sysdata);

	my $type = 'unknown';

	if ($sysdata->{rotational} == 0) {
	    $type = 'ssd';
	    $type = 'nvme' if $dev =~ m/^nvme\d+n\d+$/;
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
		my $smartdata = get_smart_data($devpath, !is_ssdlike($type));
		$health = $smartdata->{health} if $smartdata->{health};

		if (is_ssdlike($type)) {
		    # if we have an ssd we try to get the wearout indicator
		    my $wearval = get_wear_leveling_info($smartdata);
		    $wearout = $wearval if defined($wearval);
		}
	    };
	}

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

	my $by_id_link = $data->{by_id_link};
	$disklist->{$dev}->{by_id_link} = $by_id_link if defined($by_id_link);

	my $osdid = -1;
	my $bluestore = 0;
	my $osdencrypted = 0;

	my $journal_count = 0;
	my $db_count = 0;
	my $wal_count = 0;

	my $partpath = $devpath;

	# remove part after last / to
	# get the base path for the partitions
	# e.g. from /dev/cciss/c0d0 get /dev/cciss
	$partpath =~ s/\/[^\/]+$//;

	my $determine_usage = sub {
	    my ($devpath, $sysdir, $is_partition) = @_;

	    return 'LVM' if $lvmhash->{$devpath};
	    return 'ZFS' if $zfshash->{$devpath};

	    my $info = $lsblk_info->{$devpath} // {};

	    my $parttype = $info->{parttype};
	    if (defined($parttype)) {
		return 'BIOS boot'
		    if $parttype eq '21686148-6449-6e6f-744e-656564454649';
		return 'EFI'
		    if $parttype eq 'c12a7328-f81f-11d2-ba4b-00a0c93ec93b';
		return 'ZFS reserved'
		    if $parttype eq '6a945a3b-1dd2-11b2-99a6-080020736631';
	    }

	    my $fstype = $info->{fstype};
	    if (defined($fstype)) {
		return "${fstype} (mounted)" if $mounted->{$devpath};
		return "${fstype}";
	    }
	    return 'mounted' if $mounted->{$devpath};

	    return if !$is_partition;

	    # for devices, this check is done explicitly later
	    return 'Device Mapper' if !dir_is_empty("$sysdir/holders");

	    return 'partition';
	};

	my $collect_ceph_info = sub {
	    my ($devpath) = @_;

	    my $ceph_volume = $ceph_volume_infos->{$devpath} or return;
	    $journal_count += $ceph_volume->{journal} // 0;
	    $db_count += $ceph_volume->{db} // 0;
	    $wal_count += $ceph_volume->{wal} // 0;
	    if (defined($ceph_volume->{osdid})) {
		$osdid = $ceph_volume->{osdid};
		$bluestore = 1 if $ceph_volume->{bluestore};
		$osdencrypted = 1 if $ceph_volume->{encrypted};
	    }

	    my $result = { %{$ceph_volume} };
	    $result->{journals} = delete $result->{journal}
		if $result->{journal};
	    return $result;
	};

	my $partitions = {};

	dir_glob_foreach("$sysdir", "$dev.+", sub {
	    my ($part) = @_;

	    $partitions->{$part} = $collect_ceph_info->("$partpath/$part");
	    my $lvm_based_osd = defined($partitions->{$part});

	    $partitions->{$part}->{devpath} = "$partpath/$part";
	    $partitions->{$part}->{parent} = "$devpath";
	    $partitions->{$part}->{gpt} = $data->{gpt};
	    $partitions->{$part}->{type} = 'partition';
	    $partitions->{$part}->{size} =
		get_sysdir_size("$sysdir/$part") // 0;
	    $partitions->{$part}->{used} =
		$determine_usage->("$partpath/$part", "$sysdir/$part", 1);
	    $partitions->{$part}->{osdid} //= -1;

	    # Avoid counting twice (e.g. partition on which the LVM for the
	    # DB OSD resides is present in the $journalhash)
	    return if $lvm_based_osd;

	    # Legacy handling for non-LVM based OSDs

	    if (my $mp = $mounted->{"$partpath/$part"}) {
		if ($mp =~ m|^/var/lib/ceph/osd/ceph-(\d+)$|) {
		    $osdid = $1;
		    $partitions->{$part}->{osdid} = $osdid;
		}
	    }

	    if (my $journal_part = $journalhash->{"$partpath/$part"}) {
		$journal_count++ if $journal_part == 1;
		$db_count++ if $journal_part == 2;
		$wal_count++ if $journal_part == 3;
		$bluestore = 1 if $journal_part == 4;

		$partitions->{$part}->{journals} = 1 if $journal_part == 1;
		$partitions->{$part}->{db} = 1 if $journal_part == 2;
		$partitions->{$part}->{wal} = 1 if $journal_part == 3;
		$partitions->{$part}->{bluestore} = 1 if $journal_part == 4;
	    }
	});

	my $used = $determine_usage->($devpath, $sysdir, 0);
	if (!$include_partitions) {
	    foreach my $part (sort keys %{$partitions}) {
		next if $partitions->{$part}->{used} eq 'partition';
		$used //= $partitions->{$part}->{used};
	    }
	} else {
	    # fstype might be set even if there are partitions, but showing that is confusing
	    $used = 'partitions' if scalar(keys %{$partitions});
	}
	$used //= 'partitions' if scalar(keys %{$partitions});
	# multipath, software raid, etc.
	# this check comes in last, to show more specific info
	# if we have it
	$used //= 'Device Mapper' if !dir_is_empty("$sysdir/holders");

	$disklist->{$dev}->{used} = $used if $used;

	$collect_ceph_info->($devpath);

	$disklist->{$dev}->{osdid} = $osdid;
	$disklist->{$dev}->{journals} = $journal_count if $journal_count;
	$disklist->{$dev}->{bluestore} = $bluestore if $osdid != -1;
	$disklist->{$dev}->{osdencrypted} = $osdencrypted if $osdid != -1;
	$disklist->{$dev}->{db} = $db_count if $db_count;
	$disklist->{$dev}->{wal} = $wal_count if $wal_count;

	if ($include_partitions) {
	    foreach my $part (keys %{$partitions}) {
		$disklist->{$part} = $partitions->{$part};
	    }
	}
    });

    return $disklist;

}

sub get_partnum {
    my ($part_path) = @_;

    my $st = stat($part_path);

    die "error detecting block device '$part_path'\n"
	if !$st || !$st->mode || !S_ISBLK($st->mode) || !$st->rdev;

    my $major = PVE::Tools::dev_t_major($st->rdev);
    my $minor = PVE::Tools::dev_t_minor($st->rdev);
    my $partnum_path = "/sys/dev/block/$major:$minor/";

    my $partnum;

    $partnum = file_read_firstline("${partnum_path}partition");

    die "Partition does not exist\n" if !defined($partnum);

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

    my ($dev, $block_dev);
    if ($part_path =~ m|^/dev/(.*)$|) {
	$dev = $1;
	my $link = readlink "/sys/class/block/$dev";
	$block_dev = $1 if $link =~ m|([^/]*)/$dev$|;
    }

    die "Can't parse parent device\n" if !defined($block_dev);
    die "No valid block device\n" if index($dev, $block_dev) == -1;

    $block_dev = "/dev/$block_dev";
    die "Block device does not exists\n" if !(-b $block_dev);

    return $block_dev;
}

sub locked_disk_action {
    my ($sub) = @_;
    my $res = PVE::Tools::lock_file('/run/lock/pve-diskmanage.lck', undef, $sub);
    die $@ if $@;
    return $res;
}

sub assert_disk_unused {
    my ($dev) = @_;

    die "device '$dev' is already in use\n" if disk_is_used($dev);

    return undef;
}

sub append_partition {
    my ($dev, $size) = @_;

    my $devname = $dev;
    $devname =~ s|^/dev/||;

    my $newpartid = 1;
    dir_glob_foreach("/sys/block/$devname", qr/\Q$devname\E.*?(\d+)/, sub {
	my ($part, $partid) = @_;

	if ($partid >= $newpartid) {
	    $newpartid = $partid + 1;
	}
    });

    $size = PVE::Tools::convert_size($size, 'b' => 'mb');

    run_command([ $SGDISK, '-n', "$newpartid:0:+${size}M", $dev ],
		errmsg => "error creating partition '$newpartid' on '$dev'");

    my $partition;

    # loop again to detect the real partition device which does not always follow
    # a strict $devname$partition scheme like /dev/nvme0n1 -> /dev/nvme0n1p1
    dir_glob_foreach("/sys/block/$devname", qr/\Q$devname\E.*$newpartid/, sub {
	my ($part) = @_;

	$partition = "/dev/$part";
    });

    return $partition;
}

1;
