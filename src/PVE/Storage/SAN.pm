package PVE::Storage::SAN;

use v5.36;

use Cwd qw(abs_path);
use File::Basename qw(basename);

use PVE::INotify;
use PVE::Tools qw(file_read_firstline dir_glob_foreach);

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
    my (%storage, %expected, %vg_owner, %target_owner, %base_owner, %targets, %unmapped);

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
            my $session = $sessions->{$member} // next;
            $targets{$wwid}->{ $session->{target} } = 1;
            $row->{'iscsi-target'} //= $session->{target};
            $row->{'iscsi-lun'} //= $session->{lun};
            $row->{transport} = 'iscsi';
        }
        my $used = $disk->{used};
        if (defined($used)) {
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
    for my $row (values %rows) {
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

1;
