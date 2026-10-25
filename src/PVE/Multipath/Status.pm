package PVE::Multipath::Status;

use v5.36;

use Compress::Zlib qw(compress uncompress);
use JSON ();
use MIME::Base64 qw(decode_base64 encode_base64);

use PVE::Cluster;
use PVE::INotify;

use PVE::Multipath;
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
    my $initiator = eval { PVE::Storage::ISCSIPlugin::read_initiator_info() };
    $report->{'iscsi-initiator-name'} = $initiator->{name}
        if $initiator && defined($initiator->{name});
    eval {
        die "multipath-tools is not installed\n" if !PVE::Multipath::is_supported();
        die "multipathd is not running or does not respond\n" if !PVE::Multipath::is_running();
        require PVE::Storage::LVMPlugin;
        my $pv_vgs = PVE::Storage::LVMPlugin::cached_pv_volume_groups();
        # A node can have many maps that something else set up, for example a storage plugin with
        # one LUN per guest disk. Only the maps that the node manages, that are the base volume of a
        # storage or hold its volume group get a row, the others are counted.
        require PVE::Storage;
        my $wanted = $apply->{wwids};
        my $storages = eval { PVE::Storage::config()->{ids} };
        my (%storage_vgs, %base);
        for my $scfg (values %{ $storages // {} }) {
            $storage_vgs{ $scfg->{vgname} } = 1 if defined($scfg->{vgname});
            my $lun = PVE::Multipath::parse_base_volume($scfg->{base});
            $base{ $lun->{wwid} } = 1 if $lun;
        }
        # without the wanted WWIDs or the storages nothing tells the maps apart
        my $all = !$wanted || !$storages;
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
        delete $report->{luns};
    }
    eval { publish($report) };
    warn "multipath: cannot publish node status - $@" if $@;
}

1;
