package PVE::Storage::SAN;

use v5.36;

use Cwd qw(abs_path);
use Fcntl qw(O_RDWR O_EXCL);
use File::Basename qw(basename);

use PVE::Cluster;
use PVE::INotify;
use PVE::Tools qw(file_read_firstline dir_glob_foreach run_command);

use PVE::Diskmanage;
use PVE::ISCSI::Topology;
use PVE::Multipath;
use PVE::Multipath::Config;
use PVE::Multipath::Generator;
use PVE::Multipath::Identity;
use PVE::Storage;
use PVE::Storage::LVMPlugin;

sub scan_luns($cfg) {
    my $ids = PVE::Multipath::Identity::collect(all => 1);
    my $map_info = PVE::Multipath::map_info();
    my $members = PVE::Multipath::member_map($map_info);
    # A map stands for its paths, of which a node with many LUNs has thousands. Leave them out of
    # the disk list, which reads the sysfs attributes and partitions of each disk it returns.
    my @names;
    dir_glob_foreach(
        '/sys/block',
        '(?:sd[a-z]+|nvme\d+n\d+)',
        sub($name, @) { push @names, $name if !$members->{$name} },
    );
    my $disks = PVE::Diskmanage::get_disks(\@names, 1, 0, 1);
    my $maps = PVE::Diskmanage::get_multipath_disks($map_info);
    my $sessions = PVE::ISCSI::Topology::device_map();
    my $status = eval { PVE::Multipath::path_status_by_wwid() } // {};
    my $nvme = PVE::Diskmanage::get_nvme_path_status();
    my $pv_vgs = PVE::Storage::LVMPlugin::cached_pv_volume_groups() // {};
    my $mpcfg = PVE::Multipath::Config::read_config();
    my %explicit = map { $_ => 1 }
        PVE::Multipath::Config::wwids_for_node($mpcfg, PVE::INotify::nodename())->@*;
    my %listed = map { $_ => 1 } PVE::Multipath::list_etc_multipath_wwids()->@*;
    my $derived = PVE::Multipath::Generator::derived_wwids($cfg, $ids);
    my (%storage, %expected, %vg_owner, %target_owner, %base_owner, %targets, %paths, %unmapped);

    my $wanted =
        PVE::Multipath::Generator::wanted_wwids($mpcfg, PVE::INotify::nodename(), $derived);
    for my $wwid (keys %$wanted) {
        my $info = $wanted->{$wwid};
        $storage{$wwid} = $info->{storages}->[0] if $info->{storages};
        $expected{$wwid} = $info->{'paths-expected'} if defined($info->{'paths-expected'});
    }
    for my $sid (sort keys $cfg->{ids}->%*) {
        my $scfg = $cfg->{ids}->{$sid};
        $vg_owner{ $scfg->{vgname} } //= $sid if defined($scfg->{vgname});
        $target_owner{ $scfg->{target} } //= $sid
            if defined($scfg->{target}) && PVE::Storage::san_storage_serves_images($scfg);
        my $base = PVE::Multipath::parse_base_volume($scfg->{base});
        $base_owner{ $base->{wwid} } //= $sid if $base;
    }

    my %rows;
    my $add = sub($disk, $map = 0) {
        my $name = basename(abs_path($disk->{devpath}) // $disk->{devpath});
        my $info =
            { map => $map, transport => $disk->{transport}, session => $sessions->{$name} };
        return if !is_san_device($disk->{devpath}, $info);
        my $wwid = $map ? $disk->{wwn} : PVE::Multipath::Identity::path_wwid($ids, $name);
        $wwid //= file_read_firstline("/sys/class/block/$name/wwid") if $name =~ /^nvme/;
        return if !defined($wwid) || !length($wwid);

        my $row = $rows{$wwid} //= {
            wwid => $wwid,
            devpath => $disk->{devpath},
            size => $disk->{size},
            usage => 'unused',
            multipath => $listed{$wwid}
                || $explicit{$wwid}
                || $storage{$wwid} ? 'pending' : 'none',
        };
        my $link = stable_path($disk->{devpath}, $wwid, $map);
        # The device and its by-id name come from one disk. A map replaces a single path.
        if ($map || (defined($link) && !$row->{'by-id'})) {
            delete $row->{'by-id'};
            $row->{devpath} = $disk->{devpath};
            $row->{size} = $disk->{size};
            $row->{'by-id'} = $link if defined($link);
        }
        if ($map) {
            my $alias = basename($disk->{devpath});
            $row->{name} = $alias if $alias ne $wwid;
            $row->{multipath} =
                $explicit{$wwid} ? 'manual'
                : $storage{$wwid} ? 'storage'
                : 'unmanaged';
        }
        if ($name =~ /^nvme\d+n\d+$/) {
            $row->{multipath} = 'native' if $nvme->{$name};
            my $nqn = PVE::Diskmanage::read_subsysnqn("/sys/block/$name/device/subsysnqn");
            if (defined($nqn)) {
                $row->{'nvme-subsystem'} = $nqn;
                my $nsid = file_read_firstline("/sys/block/$name/nsid");
                $row->{'nvme-nsid'} = int($nsid) if defined($nsid) && $nsid =~ /^\d+$/;
            }
        }
        for my $key (qw(transport vendor model serial)) {
            my $value = $disk->{$key};
            $row->{$key} //= $value if defined($value) && $value ne 'unknown';
        }
        for my $member (map { basename($_) } ($disk->{slaves} // [$disk->{devpath}])->@*) {
            $paths{$wwid}->{"/dev/$member"} = 1;
            my $session = $sessions->{$member} // next;
            $targets{$wwid}->{ $session->{target} } = 1;
            $row->{'iscsi-target'} //= $session->{target};
            $row->{'iscsi-lun'} //= $session->{lun};
            $row->{transport} = 'iscsi';
        }
        my $used = $disk->{used};
        # the signature of a path that multipath claimed, not data on the LUN
        if (defined($used) && $used ne 'mpath_member') {
            if ($used eq 'LVM' || $used eq 'LVM2_member') {
                $row->{usage} = 'lvm';
                delete $row->{detail};
            } elsif ($row->{usage} ne 'lvm') {
                $row->{usage} = 'other';
                $row->{detail} = $used;
            }
        }
        my $vg = $pv_vgs->{ abs_path($disk->{devpath}) // $disk->{devpath} };
        if (defined($vg)) {
            $row->{vgname} //= $vg;
            $row->{usage} = 'lvm';
            delete $row->{detail};
        }
        my $paths = $map ? $status->{$wwid} : $nvme->{$name};
        if ($paths) {
            $row->{'path-list'} = $paths->{paths};
            $row->{'paths-total'} = scalar($paths->{paths}->@*);
            $row->{'paths-active'} = $paths->{active};
        } elsif ($map) {
            $row->{'paths-total'} = $disk->{paths};
        }
        $unmapped{$wwid}->{$name} = 1 if !$map && $name =~ /^sd[a-z]+$/;
        $row->{'multipath-storage'} = $storage{$wwid} if defined($storage{$wwid});
        # multipath entries and iSCSI portals are for maps, not for native NVMe multipath
        $row->{'paths-expected'} = $expected{$wwid}
            if defined($expected{$wwid}) && $row->{multipath} ne 'native';
    };
    for my $name (sort keys %$disks) {
        my $disk = $disks->{$name};
        next if $members->{$name};
        $add->($disk);
    }
    $add->($maps->{$_}, 1) for sort keys %$maps;
    # Without a map, the paths of a LUN are disks of their own, each in the state that the kernel
    # keeps for it, which is 'running' while it takes I/O.
    for my $wwid (sort keys %unmapped) {
        my $row = $rows{$wwid};
        next if defined($row->{'paths-total'});
        my @paths = map { { device => "/dev/$_", state => device_state($_) // 'unknown' } }
            sort keys $unmapped{$wwid}->%*;
        $row->{'path-list'} = \@paths;
        $row->{'paths-total'} = scalar(@paths);
        $row->{'paths-active'} = scalar(grep { $_->{state} eq 'running' } @paths);
    }
    # the portal of its session tells an iSCSI path apart, as the address does for NVMe
    for my $row (values %rows) {
        for my $path (($row->{'path-list'} // [])->@*) {
            my $portal = ($sessions->{ basename($path->{device}) } // {})->{portal};
            $path->{address} //= $portal if length($portal // '');
        }
    }
    # A guest can have a LUN as disk by its device path or from a third-party storage plugin, also
    # by one of its paths or partitions. The scans of LVM and udev only read a device, and are gone
    # again a moment later.
    for my $wwid (sort keys %rows) {
        my $row = $rows{$wwid};
        my %seen;
        for my $device ($row->{devpath}, sort keys(($paths{$wwid} // {})->%*)) {
            next if $seen{$device}++;
            my $parts = eval { lun_partitions({ devpath => $device, wwid => $wwid }) } // [];
            $paths{$wwid}->{$_} = 1 for @$parts;
            if (@$parts && $row->{usage} eq 'unused') {
                # a path that multipath claimed shows its signature instead of its partitions
                $row->{usage} = 'other';
                $row->{detail} = 'partitions';
            }
        }
    }
    my %devices = map { $_ => 1 } (map { $_->{devpath} } values %rows),
        map { keys %$_ } values %paths;
    my $openers = PVE::Diskmanage::device_openers([sort keys %devices], writable => 1);
    for my $row (values %rows) {
        my ($opener) = map { $openers->{$_} // () } abs_path($row->{devpath}) // $row->{devpath},
            sort keys(($paths{ $row->{wwid} } // {})->%*);
        if ($opener) {
            $row->{'used-by-process'} = "$opener->{pid} ($opener->{command})";
            # a virtual machine runs in a control group of its own that is named after its ID
            my $cgroup = file_read_firstline("/proc/$opener->{pid}/cgroup") // '';
            $row->{'used-by-guest'} = int($1) if $cgroup =~ m{^0::/qemu\.slice/(\d+)\.scope$};
        }
        # the devices of a LUN whose requested map is missing do not tell the state of the LUN
        if (defined($row->{'paths-active'}) && $row->{multipath} ne 'pending') {
            $row->{state} = PVE::Multipath::derive_health(
                $row->{'paths-active'},
                $row->{'paths-total'},
                $row->{'paths-expected'},
            );
        }
        my ($target) =
            map { $target_owner{$_} // () } sort keys(($targets{ $row->{wwid} } // {})->%*);
        my $owner = $base_owner{ $row->{wwid} } // $vg_owner{ $row->{vgname} // '' } // $target;
        $row->{'used-by-storage'} = $owner if defined($owner);
    }
    return [map { $rows{$_} } sort keys %rows];
}

sub device_state($name) {
    return file_read_firstline("/sys/block/$name/device/state");
}

sub stable_path($device, $wwid, $map = 0) {
    my $prefix = $map ? 'dm-uuid-mpath' : basename($device) =~ /^nvme/ ? 'nvme' : 'scsi';
    my ($link) = "/dev/disk/by-id/$prefix-$wwid" =~ m{\A(/dev/disk/by-id/[A-Za-z0-9_.:+-]+)\z};
    return undef if !defined($link);
    return $link if (abs_path($link) // '') eq (abs_path($device) // $device);
    return undef;
}

sub is_san_device($device, $info = undef) {
    if (!$info) {
        my $path = abs_path($device) // $device;
        my $map = PVE::Multipath::device_map($path);
        # the partitions of a LUN are handled like local devices, also the ones that kpartx maps
        return 0 if PVE::Diskmanage::is_partition($path) || ($map && $map->{partition});
        my $name = basename($path);
        my $transport;
        if ($name =~ /^nvme/) {
            $transport = 'nvme-' . (PVE::Diskmanage::get_nvme_transport("/sys/block/$name") // '');
        } elsif ((readlink("/sys/block/$name") // '') =~ m{/rport-\d+}) {
            $transport = 'fc';
        }
        $info = {
            map => $map,
            transport => $transport,
            session => PVE::Diskmanage::is_iscsi_device($name),
        };
    }
    return
        $info->{map}
        || $info->{session}
        || PVE::Diskmanage::is_remote_transport($info->{transport}) ? 1 : 0;
}

sub lun_for_device($name) {
    my $device = PVE::Diskmanage::verify_blockdev_path($name);
    return undef if !is_san_device($device);
    my $rows = scan_luns(PVE::Storage::config());
    # the device behind a name such as a by-id one can pass to another LUN during the scan
    die "cannot use '$device' - the device behind '$name' changed\n"
        if (abs_path($name) // '') ne $device;
    for my $lun (@$rows) {
        next if (abs_path($lun->{devpath}) // '') ne $device;
        die "cannot use '$device' - no link under /dev/disk/by-id names the WWID of the LUN\n"
            if !$lun->{'by-id'};
        # read from sysfs, but used in command lines and in the name of the cluster lock
        ($lun->{devpath}) = $lun->{devpath} =~ m{\A(/dev/[A-Za-z0-9_.:+/-]+)\z}
            or die "cannot use '$device' - unexpected device path\n";
        ($lun->{wwid}) = $lun->{wwid} =~ m/\A([A-Za-z0-9_.:+=-]+)\z/
            or die "cannot use '$device' - unexpected WWID\n";
        return $lun;
    }
    die "cannot use '$device' - use the device that the SAN LUN list shows for the LUN, or its"
        . " multipath map\n";
}

sub lun_partitions($lun) {
    my $device = PVE::Diskmanage::verify_blockdev_path($lun->{devpath});
    my $name = basename($device);
    my @parts;
    dir_glob_foreach(
        "/sys/class/block/$name",
        "\Q$name\E.+",
        sub($part, @) {
            my ($path) = "/dev/$part" =~ m{\A(/dev/[A-Za-z0-9_.+-]+)\z};
            push @parts, $path if defined($path) && -e "/sys/class/block/$name/$part/partition";
        },
    );
    dir_glob_foreach(
        "/sys/class/block/$name/holders",
        'dm-\\d+',
        sub($part, @) {
            my $map = PVE::Multipath::parse_dm_uuid(
                file_read_firstline("/sys/class/block/$part/dm/uuid"));
            my ($path) = "/dev/$part" =~ m{\A(/dev/dm-\d+)\z};
            push @parts, $path
                if defined($path) && $map && $map->{partition} && $map->{wwid} eq $lun->{wwid};
        },
    );
    return \@parts;
}

sub assert_lun_unreferenced($cfg, $lun, $exempt = undef) {
    my %vgs = map { $_ => 1 } PVE::Storage::LVMPlugin::lun_volume_groups(
        [$lun->{devpath}, lun_partitions($lun)->@*],
    )->@*;
    # the paths of a LUN can belong to more than one target
    my $sessions = PVE::ISCSI::Topology::device_map();
    my %targets =
        map { $_->{target} => 1 } map { $sessions->{ basename($_) } // () } lun_paths($lun)->@*;
    $targets{ $lun->{'iscsi-target'} } = 1 if defined($lun->{'iscsi-target'});
    for my $sid (sort keys $cfg->{ids}->%*) {
        next if defined($exempt) && $sid eq $exempt;
        my $scfg = $cfg->{ids}->{$sid};
        my $type = $scfg->{type};
        if ($vgs{ $scfg->{vgname} // '' }) {
            die "cannot use '$lun->{devpath}' - volume group is used by storage '$sid'\n";
        }
        if (
            defined($scfg->{target})
            && $targets{ $scfg->{target} }
            && PVE::Storage::san_storage_serves_images($scfg)
        ) {
            die "cannot use '$lun->{devpath}' - storage '$sid' serves images from this target\n";
        }
        next if $type ne 'lvm' || !$scfg->{base};
        my $base = PVE::Multipath::parse_base_volume($scfg->{base});
        my $matches = $base
            && ($base->{wwid} eq $lun->{wwid}
                || (defined($base->{full_wwid}) && $base->{full_wwid} eq $lun->{wwid}));
        if (!$matches) {
            my $path = eval { abs_path(PVE::Storage::path($cfg, $scfg->{base})) };
            my $device = abs_path($lun->{devpath});
            $matches = defined($path) && defined($device) && $path eq $device;
        }
        die "cannot use '$lun->{devpath}' - storage '$sid' uses this LUN as its base volume\n"
            if $matches;
    }
    return;
}

# Serializes writes to a LUN cluster-wide. The storage configuration read in the code is current.
sub with_lun_lock($lun, $code) {
    my $key = $lun->{wwid} =~ s/([^A-Za-z0-9_.:-])/sprintf('=%02x', ord($1))/ger;
    PVE::Cluster::cfs_lock_domain("san-$key", undef, $code);
    die $@ if $@;
}

# Device names can pass to another device while a LUN waits for its lock or is worked on. All
# names used for it have to mean the device that is held open.
sub assert_same_device($lun, $fh, @names) {
    my @held = stat($fh);
    for my $name ($lun->{'by-id'}, $lun->{devpath}, @names) {
        my @named = stat($name);
        die "cannot use '$lun->{devpath}' - the device behind '$name' changed\n"
            if !@held || !@named || $held[6] != $named[6];
    }
}

# The SCSI paths of the LUN. Without a map, udev keeps the signatures it has seen for each of them,
# so a wipe needs a udev trigger for every path. The members of a map count whatever their identity,
# which without multipathd comes from udev and can differ from the one of the map.
sub lun_paths($lun) {
    my $paths = PVE::Multipath::Identity::collect(all => 1)->{paths};
    my %names = map { $_ => 1 } grep { $paths->{$_}->{wwid} eq $lun->{wwid} } keys %$paths;
    my $device = basename(abs_path($lun->{devpath}) // $lun->{devpath});
    dir_glob_foreach(
        "/sys/class/block/$device/slaves",
        'sd[a-z]+',
        sub($slave, @) { $names{$slave} = 1 },
    );
    return [map { m/^(sd[a-z]+)$/ ? "/dev/$1" : () } sort keys %names];
}

# The other paths of a LUN reach the same data, but neither the exclusive open of the LUN nor the
# checks of its device cover them.
sub assert_lun_paths_unused($lun) {
    my $device = basename(abs_path($lun->{devpath}) // $lun->{devpath});
    my %devices; # the paths and their partitions
    for my $path (grep { basename($_) ne $device } lun_paths($lun)->@*) {
        if (my $mounted = PVE::Diskmanage::is_mounted($path)) {
            die "cannot use '$lun->{devpath}' - the LUN is mounted through '$mounted'\n";
        }
        my $name = basename($path);
        $devices{$path} = 1;
        dir_glob_foreach(
            "/sys/class/block/$name",
            "\Q$name\E\\d+",
            sub($part, @) {
                $devices{"/dev/$part"} = 1;
            },
        );
    }
    for my $path (sort keys %devices) {
        dir_glob_foreach(
            '/sys/class/block/' . basename($path) . '/holders',
            '[^.].*',
            sub($holder, @) {
                if ($holder ne $device) {
                    die "cannot use '$lun->{devpath}' - another block device uses the LUN"
                        . " through '$path'\n";
                }
            },
        );
    }
    my $openers = PVE::Diskmanage::device_openers([sort keys %devices], writable => 1);
    for my $path (sort keys %$openers) {
        my $opener = $openers->{$path};
        die "cannot use '$lun->{devpath}' - process $opener->{pid} ($opener->{command}) has the"
            . " LUN open through '$path'\n";
    }
}

sub wipe_lun($lun) {
    # Probe the kernel path of the held device, not a mapper alias that can pass to another LUN.
    my $device = PVE::Diskmanage::verify_blockdev_path($lun->{'by-id'});
    sysopen(my $fh, $lun->{'by-id'}, O_RDWR) or die "cannot open '$device' - $!\n";
    assert_same_device($lun, $fh, $device);
    PVE::Storage::LVMPlugin::assert_running_path($device);
    my $held = { %$lun, devpath => $device };
    assert_lun_unreferenced(PVE::Storage::config(), $held);
    assert_lun_paths_unused($held);
    my $parts = lun_partitions($held);
    my %parts = map { basename($_) => 1 } @$parts;
    dir_glob_foreach(
        '/sys/class/block/' . basename($device) . '/holders',
        '[^.].*',
        sub($name, @) {
            die "cannot wipe '$device' - another block device uses it\n" if !$parts{$name};
        },
    );
    for my $path ($device, @$parts) {
        die "cannot wipe '$device' - '$path' is mounted\n" if PVE::Diskmanage::is_mounted($path);
        next if $path eq $device;
        die "cannot wipe '$device' - another block device uses '$path'\n"
            if PVE::Diskmanage::has_holder($path);
    }
    # a guest with the LUN as disk has it open, without being a holder or a storage reference
    my $opener = PVE::Diskmanage::device_opener($device, @$parts);
    die "cannot wipe '$device' - process $opener has it open\n" if defined($opener);
    # the name of a partition can pass to another device as well, after it was listed
    my $check_part = sub($part) {
        assert_same_device($lun, $fh, $device);
        die "cannot wipe '$device' - '$part' is no longer one of its partitions\n"
            if !grep { $_ eq $part } lun_partitions($held)->@*;
    };
    for my $part (@$parts) {
        $check_part->($part);
        run_command(['wipefs', '--all', $part], timeout => 10);
        if ($part =~ m{^/dev/dm-}) {
            $check_part->($part);
            run_command(['dmsetup', 'remove', '--retry', $part], timeout => 10);
        }
    }
    assert_same_device($lun, $fh, $device);
    # claim what was checked: while the first descriptor is open, its device number cannot pass to
    # another device, so an equal number means the same device
    sysopen(my $claim, $lun->{'by-id'}, O_RDWR | O_EXCL) or die "cannot claim '$device' - $!\n";
    die "cannot wipe '$device' - the device behind '$lun->{'by-id'}' changed\n"
        if (stat($claim))[6] != (stat($fh))[6];
    close($fh);
    my $path = '/proc/' . $$ . '/fd/' . fileno($claim);
    PVE::Diskmanage::wipe_blockdev($device, $path);
}

1;
