package PVE::Multipath::Status;

use v5.36;

use Compress::Zlib qw(compress uncompress);
use JSON ();
use MIME::Base64 qw(decode_base64 encode_base64);

use PVE::Cluster;
use PVE::INotify;

use PVE::Diskmanage;
use PVE::Multipath;
use PVE::Multipath::Config;
use PVE::Multipath::Generator;

my $json = JSON->new->utf8->canonical;
my $KEY = 'multipath';

# A value in the key-value store of pmxcfs is limited to 32 KiB. Compressed, a report holds several
# times more LUNs: its rows repeat their keys, and the LUNs of one array share a long WWID prefix.
sub encode_report($data) {
    return encode_base64(compress($json->encode($data)), '');
}

sub decode_report($raw) {
    my $data = eval { $json->decode(uncompress(decode_base64($raw // '')) // die "invalid\n") };
    return ref($data) eq 'HASH' ? $data : undef;
}

sub worst_state(@states) {
    my %rank = (unknown => -1, optimal => 0, degraded => 1, missing => 2, failed => 3);
    my $worst = 'unknown';
    for my $state (@states) {
        $worst = $state
            if defined($state) && exists($rank{$state}) && $rank{$state} > $rank{$worst};
    }
    return $worst;
}

my sub namespace_luns($pv_vgs) {
    require PVE::Storage;
    my $cfg = PVE::Storage::config();
    my @storages = grep {
        $cfg->{ids}->{$_}->{type} eq 'nvmeof'
            && PVE::Storage::storage_check_enabled($cfg, $_, undef, 1)
    } sort keys $cfg->{ids}->%*;
    # reading the paths walks all NVMe namespaces of the node, also those of other plugins
    return [] if !@storages;
    my $status = PVE::Diskmanage::get_nvme_path_status();
    my %luns;
    for my $sid (@storages) {
        my $scfg = $cfg->{ids}->{$sid};
        my $expected = eval { scalar(PVE::NVMeOF::storage_paths($scfg)->@*) };
        my $namespaces = PVE::NVMeOF::namespaces($scfg->{subsysnqn});
        for my $name (sort keys %$namespaces) {
            my $wwid = $namespaces->{$name}->{wwid};
            next if $luns{$wwid};
            my $paths = $status->{$name} // { active => 0, paths => [] };
            my $total = scalar($paths->{paths}->@*);
            my $lun = $luns{$wwid} = {
                wwid => $wwid,
                native => 1,
                size => $namespaces->{$name}->{size},
                storages => [$sid],
                'paths-total' => $total,
                'paths-active' => $paths->{active},
                state => PVE::Multipath::derive_health($paths->{active}, $total, $expected),
            };
            $lun->{'paths-expected'} = $expected if defined($expected);
            my $vg = ($pv_vgs // {})->{"/dev/$name"};
            $lun->{vgname} = $vg if defined($vg);
        }
    }
    return [map { $luns{$_} } sort keys %luns];
}

sub publish($report) {
    my $node = PVE::INotify::nodename();
    my $now = time();
    my $data = { %$report, time => $now };
    my $encoded = encode_report($data);
    if (length($encoded) >= 32 * 1024) {
        $data = { %$data, error => 'too many LUNs to report' };
        delete $data->{luns};
        $encoded = encode_report($data);
    }
    my $raw = PVE::Cluster::get_node_kv($KEY, $node);
    my $previous = decode_report($raw->{$node});
    if ($previous) {
        my $age = $now - ($previous->{time} // 0);
        $previous->{time} = $now;
        return if $age >= 0 && $age < 60 && encode_report($previous) eq $encoded;
    }
    PVE::Cluster::broadcast_node_kv($KEY, $encoded);
}

sub update_node_status() {
    my $apply = PVE::Multipath::Generator::sync();
    my $report = { luns => [] };
    $report->{'apply-error'} = $apply->{error} if defined($apply->{error});
    require PVE::Storage::ISCSIPlugin;
    # the initiator name stays if open-iscsi is removed but not purged, so skip it then
    if (PVE::Storage::ISCSIPlugin::iscsi_supported()) {
        my $initiator = eval { PVE::Storage::ISCSIPlugin::read_initiator_info() };
        $report->{'iscsi-initiator-name'} = $initiator->{name}
            if $initiator && defined($initiator->{name});
    }
    require PVE::NVMeOF;
    my $host = eval { PVE::NVMeOF::host_identity() };
    $report->{'nvme-host-nqn'} = $host->{nqn} if $host;
    require PVE::Storage::LVMPlugin;
    my $pv_vgs = PVE::Storage::LVMPlugin::cached_pv_volume_groups();
    eval {
        die "multipath-tools is not installed\n" if !PVE::Multipath::is_supported();
        die "multipathd is not running or does not respond\n" if !PVE::Multipath::is_running();
        # only report maps we manage or use for storage; count unrelated maps under other-maps
        require PVE::Storage;
        my $wanted = $apply->{wwids};
        my $storages = eval { PVE::Storage::config()->{ids} };
        my (%storage_vgs, %base);
        for my $scfg (values %{ $storages // {} }) {
            $storage_vgs{ $scfg->{vgname} } = 1 if defined($scfg->{vgname});
            my $lun = PVE::Multipath::parse_base_volume($scfg->{base});
            $base{ $lun->{wwid} } = 1 if $lun;
        }
        # if wanted WWIDs or storage config is missing, report all maps and flag scope-unknown
        my $all = !$wanted || !$storages;
        $report->{'scope-unknown'} = 1 if $all;
        my %mapped;
        for my $map (PVE::Multipath::get_maps()->@*) {
            my $wwid = $map->{wwid} // next;
            $mapped{$wwid} = 1;
            my $vg = defined($map->{sysfs}) ? ($pv_vgs // {})->{"/dev/$map->{sysfs}"} : undef;
            if (!$all && !$wanted->{$wwid} && !$base{$wwid} && !$storage_vgs{ $vg // '' }) {
                $report->{'other-maps'}++;
                next;
            }
            my $info = ($wanted // {})->{$wwid} // {};
            my $lun = {
                wwid => $wwid,
                'paths-total' => $map->{'paths-total'},
                'paths-active' => $map->{'paths-active'},
                state => PVE::Multipath::derive_health(
                    $map->{'paths-active'},
                    $map->{'paths-total'},
                    $info->{'paths-expected'},
                ),
            };
            $lun->{size} = $map->{size} if defined($map->{size});
            $lun->{name} = $map->{name} if defined($map->{name}) && $map->{name} ne $wwid;
            $lun->{storages} = $info->{storages} if $info->{storages};
            $lun->{'paths-expected'} = $info->{'paths-expected'}
                if defined($info->{'paths-expected'});
            $lun->{vgname} = $vg if defined($vg);
            push $report->{luns}->@*, $lun;
        }
        # The cluster status has no other source for a LUN of a storage that no node assembled. A
        # WWID added in this pass gets its map from the reload that follows.
        my %added = map { $_ => 1 } ($apply->{added} // [])->@*;
        for my $wwid (sort keys %{ $wanted // {} }) {
            my $info = $wanted->{$wwid};
            next if $mapped{$wwid} || $added{$wwid} || !$info->{storages};
            my $lun = { wwid => $wwid, state => 'missing', storages => $info->{storages} };
            $lun->{'paths-expected'} = $info->{'paths-expected'}
                if defined($info->{'paths-expected'});
            push $report->{luns}->@*, $lun;
        }
        # the volume groups come from the hint file of LVM, which it does not always have
        $report->{'vgs-unknown'} = 1 if !$pv_vgs && $report->{'other-maps'};
    };
    if (my $error = $@) {
        chomp $error;
        $report->{error} = $error;
        $report->{luns} = [];
    }
    my $namespaces = eval { namespace_luns($pv_vgs) };
    if (my $error = $@) {
        warn "multipath: cannot query NVMe namespaces - $error";
        # a consumer must not take the namespaces as gone
        $report->{'namespaces-unknown'} = 1;
    }
    push $report->{luns}->@*, @$namespaces if $namespaces;
    eval { publish($report) };
    warn "multipath: cannot publish node status - $@" if $@;
    return $report;
}

sub cluster_status() {
    require PVE::Storage;

    my $cfg = PVE::Multipath::Config::read_config();
    my $storage = PVE::Storage::config();
    my $all_nodes = PVE::Cluster::get_nodelist();
    my $members = PVE::Cluster::get_members();
    my $raw = PVE::Cluster::get_node_kv($KEY);
    my %reports = map { $_ => decode_report($raw->{$_}) // {} } @$all_nodes;
    my %reported = map { $_->{wwid} => 1 }
        grep { defined($_->{wwid}) } map { ($_->{luns} // [])->@* } values %reports;
    my (%nodes, %luns, %expected, %observed, %vg_owner, %lists, %disabled_owner);
    # the nodes that list a map because the configuration names it for them
    my (%managed, %unhinted);
    my $storage_nodes = sub($scfg) {
        return $scfg->{nodes} && $scfg->{nodes}->%* ? [keys $scfg->{nodes}->%*] : $all_nodes;
    };
    my %current = map { $_ => 1 } @$all_nodes;
    my $requests_maps = sub($sid, $node = undef) {
        my $scfg = defined($sid) ? $storage->{ids}->{$sid} : undef;
        my $requests = $scfg && $scfg->{type} eq 'iscsi' && $scfg->{'managed-multipath'};
        return 0 if !$requests || $scfg->{disable};
        my @nodes = grep { $current{$_} } $storage_nodes->($scfg)->@*;
        return defined($node) ? !!grep({ $_ eq $node } @nodes) : !!@nodes;
    };
    for my $wwid (PVE::Multipath::Config::wwid_list($cfg)->@*) {
        my $entry = $cfg->{ids}->{$wwid};
        $luns{$wwid} = { wwid => $wwid, explicit => 1 };
        $luns{$wwid}->{name} = $entry->{alias} if defined($entry->{alias});
        $expected{$wwid}->{$_} = $managed{$wwid}->{$_} = 1 for $storage_nodes->($entry)->@*;
    }
    for my $sid (sort keys $storage->{ids}->%*) {
        my $scfg = $storage->{ids}->{$sid};
        $vg_owner{ $scfg->{vgname} } //= $sid if defined($scfg->{vgname});
        next if $scfg->{type} ne 'lvm';
        my $base = PVE::Multipath::parse_base_volume($scfg->{base}) // next;
        # A base volume by its SCSI name is a map only if its iSCSI storage requests maps. The name
        # can also be another by-id link of the LUN, so only a WWID that a node reports counts.
        my $map = $requests_maps->($base->{storage}) && $reported{ $base->{wwid} };
        next if $base->{kind} eq 'scsi' && !$map;
        my $wwid = $base->{wwid};
        # a disabled storage expects its map nowhere, but uses it if it is listed for another reason
        if ($scfg->{disable}) {
            $disabled_owner{$wwid} //= $sid;
            next;
        }
        $luns{$wwid} //= { wwid => $wwid, explicit => 0 };
        $luns{$wwid}->{native} = 1 if $base->{kind} eq 'nvme';
        $luns{$wwid}->{'used-by-storage'} //= $sid;
        $managed{$wwid}->{$_} = 1 for @$all_nodes;
        $luns{$wwid}->{'multipath-storage'} //= $base->{storage}
            if $requests_maps->($base->{storage});
        $expected{$wwid}->{$_} = 1 for $storage_nodes->($scfg)->@*;
    }
    for my $node (sort @$all_nodes) {
        my $report = $reports{$node};
        my $entry = $nodes{$node} = { state => 'no-report' };
        $entry->{time} = $report->{time} if defined($report->{time});
        my $offline = exists($members->{$node}) && !$members->{$node}->{online};
        next if $offline || !defined($report->{time}) || time() - $report->{time} > 180;
        my @keys = qw(error apply-error iscsi-initiator-name nvme-host-nqn other-maps);
        $entry->{$_} = $report->{$_} for grep { defined($report->{$_}) } @keys;
        $entry->{state} = defined($report->{error}) ? 'unavailable' : 'ok';
        $unhinted{$node} = 1 if $report->{'vgs-unknown'};

        # the node lists its namespaces, also if it is unavailable for its maps
        $lists{$node} = 1 if $report->{luns} && !$report->{'namespaces-unknown'};
        for my $lun (($report->{luns} // [])->@*) {
            my $wwid = $lun->{wwid} // next;
            my $row = $luns{$wwid} //= { wwid => $wwid, explicit => 0 };
            $row->{native} = 1 if $lun->{native};
            $row->{$_} //= $lun->{$_} for grep { defined($lun->{$_}) } qw(name size);
            my $owner = defined($lun->{vgname}) ? $vg_owner{ $lun->{vgname} } : undef;
            $row->{'used-by-storage'} //= $owner if defined($owner);
            # a shared LVM storage on the map needs it on its nodes, also without a base volume
            my $lvm = defined($owner) ? $storage->{ids}->{$owner} : undef;
            if ($lvm && $lvm->{type} eq 'lvm' && $lvm->{shared} && !$lvm->{disable}) {
                $expected{$wwid}->{$_} = 1 for $storage_nodes->($lvm)->@*;
            }
            for my $sid (($lun->{storages} // [])->@*) {
                my $scfg = $storage->{ids}->{$sid} // next;
                $row->{'multipath-storage'} //= $sid if $requests_maps->($sid, $node);
                $row->{'used-by-storage'} //= $sid
                    if PVE::Storage::san_storage_serves_images($scfg);
                $expected{$wwid}->{$_} = 1
                    for !$scfg->{disable} ? $storage_nodes->($scfg)->@* : ();
                $managed{$wwid}->{$_} = 1
                    for $requests_maps->($sid) ? $storage_nodes->($scfg)->@* : ();
            }
            $observed{$wwid}->{$node} = {
                state => worst_state($lun->{state}),
                map { $_ => $lun->{$_} }
                    grep { defined($lun->{$_}) }
                    qw(name size paths-active paths-total paths-expected),
            };
        }
    }
    for my $wwid (sort keys %luns) {
        my $row = $luns{$wwid};
        $row->{nodes} = {};
        for my $node (sort @$all_nodes) {
            if ($observed{$wwid}->{$node}) {
                $row->{nodes}->{$node} = $observed{$wwid}->{$node};
            } elsif ($expected{$wwid}->{$node}) {
                my $listed = $row->{native} ? $lists{$node} : $nodes{$node}->{state} eq 'ok';
                # without the hint file of LVM a node leaves a map out that it only knows through
                # the volume group of a storage
                $listed = 0 if $unhinted{$node} && !$managed{$wwid}->{$node} && !$row->{native};
                $row->{nodes}->{$node} = { state => $listed ? 'missing' : 'unknown' };
            }
        }
        $row->{state} = worst_state(map { $_->{state} } values $row->{nodes}->%*);
        $row->{'used-by-storage'} //= $disabled_owner{$wwid} if defined($disabled_owner{$wwid});
    }
    return { nodes => \%nodes, luns => [map { $luns{$_} } sort keys %luns] };
}

1;
