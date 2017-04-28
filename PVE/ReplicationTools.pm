package PVE::ReplicationTools;

use warnings;
use strict;

use PVE::Tools qw(run_command);
use PVE::Cluster;
use PVE::QemuConfig;
use PVE::LXC::Config;
use PVE::LXC;
use PVE::Storage;
use Time::Local;
use JSON;
use Data::Dumper qw(Dumper);

my $STATE_DIR = '/var/lib/pve-replica';
my $STATE_PATH = "$STATE_DIR/pve-replica.state";

PVE::Cluster::cfs_update;
my $local_node = PVE::INotify::nodename();

my $cluster_nodes;

my $get_guestconfig = sub {
    my ($vmid) = @_;

    my $vms = PVE::Cluster::get_vmlist();

    my $type = $vms->{ids}->{$vmid}->{type};

    my $guestconf;
    my $running;

    if ($type =~ m/^qemu$/) {
	$guestconf = PVE::QemuConfig->load_config($vmid);
	$running = PVE::QemuServer::check_running($vmid);
    } elsif ($type =~ m/^lxc$/) {
	$guestconf = PVE::LXC::Config->load_config($vmid);
	$running = PVE::LXC::check_running($vmid);
    }

    return ($guestconf, $type, $running);
};

sub write_state {
    my ($state) = @_;

    mkdir $STATE_DIR;

    PVE::Tools::file_set_contents($STATE_PATH, JSON::encode_json($state));
}

sub read_state {

    return {} if !(-e $STATE_PATH);

    my $raw = PVE::Tools::file_get_contents($STATE_PATH);

    return {} if $raw eq '';
    return JSON::decode_json($raw);
}

sub get_node_ip {
    my ($nodename) = @_;

    my $remoteip = PVE::Cluster::remote_node_ip($nodename, 1);

    my $dc_conf = PVE::Cluster::cfs_read_file('datacenter.cfg');
    if (my $network = $dc_conf->{storage_replication_network}) {

	my $cmd = ['ssh', '-o', 'Batchmode=yes', "root\@$remoteip", '--'
		   ,'pvecm', 'mtunnel', '--get_migration_ip',
		   '--migration_network', $network];

	PVE::Tools::run_command($cmd, outfunc => sub {
	    my $line = shift;

	    if ($line =~ m/^ip: '($PVE::Tools::IPRE)'$/) {
		$remoteip = $1;
	    }
	});
    }
    return $remoteip;
}

sub get_all_jobs {
    my ($nodes) = @_;

    my @nodelist = PVE::Tools::split_list($nodes);

    my $vms = PVE::Cluster::get_vmlist();
    my $state = read_state();
    my $jobs = {};

    my $outfunc = sub {
	my $line = shift;

	my $remote_jobs = JSON::decode_json($line);
	foreach my $vmid (keys %$remote_jobs) {
	    $jobs->{$vmid} = $remote_jobs->{$vmid};
	}
    };

    foreach my $node (@nodelist) {
	if ($local_node ne $node) {

	    my $ip = get_node_ip($node);
	    $ip = [$ip] if Net::IP::ip_is_ipv6($ip);

	    my @cmd = ('ssh', '-o', 'Batchmode=yes', "root\@$ip", '--',
		       'pvesr', 'list', '--json');

	    run_command([@cmd], outfunc=>$outfunc)

	} else {

	    foreach my $vmid (keys %{$vms->{ids}}) {

		next if !($vms->{ids}->{$vmid}->{node} eq $local_node);
		next if !defined($state->{$vmid});
		my $vm_state = $state->{$vmid};
		my $job = {};

		$job->{limit}    = $vm_state->{limit};
		$job->{interval} = $vm_state->{interval};
		$job->{tnode}    = $vm_state->{tnode};
		$job->{lastsync} = $vm_state->{lastsync};
		$job->{state}    = $vm_state->{state};
		$job->{fail}     = $vm_state->{fail};

		$jobs->{$vmid}   = $job;
	    }

	}
    }

    return ($jobs);
}

sub sync_guest {
    my ($vmid, $param) = @_;

    my $jobs = read_state();
    $jobs->{$vmid}->{state} = 'sync';
    write_state($jobs);

    my ($guest_conf, $vm_type, $running) = &$get_guestconfig($vmid);
    my $qga = 0;

    my $job = $jobs->{$vmid};
    my $tnode = $job->{tnode};

    if ($vm_type eq "qemu" && defined($guest_conf->{agent}) ) {
	$qga = PVE::QemuServer::qga_check_running($vmid)
	    if $running;
    }

    # will not die if a disk is not syncable
    my $disks = get_syncable_guestdisks($guest_conf, $vm_type);

    # check if all nodes have the storage availible
    my $storage_config = PVE::Storage::config();
    foreach my $volid (keys  %$disks) {
	my ($storeid) = PVE::Storage::parse_volume_id($volid);

	my $store = $storage_config->{ids}->{$storeid};
	die "Storage $storeid not availible on node: $tnode\n"
	    if $store->{nodes}  && !$store->{nodes}->{$tnode};
	die "Storage $storeid not availible on node: $local_node\n"
	    if $store->{nodes} && !$store->{nodes}->{$local_node};

    }

    my $limit = $param->{limit};
    $limit = $guest_conf->{replica_rate_limit}
	if (!defined($limit));

    my $snap_time = time();

    die "Invalid synctime format: $job->{lastsync}."
	if $job->{lastsync} !~ m/^(\d+)$/;

    my $lastsync = $1;
    my $incremental_snap = $lastsync ? "replica_$lastsync" : undef;

    # freeze filesystem for data consistency
    if ($qga) {
	print "Freeze guest filesystem\n";

	eval {
	    PVE::QemuServer::vm_mon_cmd($vmid, "guest-fsfreeze-freeze");
	};
    }

    my $snapname = "replica_$snap_time";

    my $disks_status = { snapname => $snapname };

    my $sync_job = sub {

	# make snapshot of all volumes
	foreach my $volid (keys %$disks) {

	    eval {
		PVE::Storage::volume_snapshot($storage_config, $volid, $snapname);
	    };

	    if (my $err = $@) {
		if ($qga) {
		    print "Unfreeze guest filesystem\n";
		    eval {
			PVE::QemuServer::vm_mon_cmd($vmid, "guest-fsfreeze-thaw");
		    };
		    warn $@ if $@;
		}
		cleanup_snapshot($disks_status, $snapname, $storage_config, $running);
		$jobs->{$vmid}->{state} = 'error';
		write_state($jobs);

		die $err;
	    }

	    $disks_status->{$volid}->{snapshot} = 1;
	}

	if ($qga) {
	    print "Unfreeze guest filesystem\n";
	    eval { PVE::QemuServer::vm_mon_cmd($vmid, "guest-fsfreeze-thaw"); };
	    warn $@ if $@;
	}

	my $ip = get_node_ip($tnode);

	foreach my $volid (keys %$disks) {

	    eval {
		PVE::Storage::volume_send($storage_config, $volid, $snapname,
					  $ip, $incremental_snap,
					  $param->{verbose}, $limit);
		$job->{fail} = 0;
	    };

	    if (my $err = $@) {
		cleanup_snapshot($disks_status, $snapname, $storage_config, $running, $ip);
		$job->{fail}++;
		$job->{state} = 'error' if $job->{fail} > 3;

		$jobs->{$vmid} = $job;
		write_state($jobs);
		die $err;
	    }

	    $disks_status->{$volid}->{synced} = 1;
	}

	# delet old snapshot if exists
	cleanup_snapshot($disks_status, $snapname, $storage_config, $running, $ip, $lastsync) if
	    $job->{lastsync} ne '0';

	$job->{lastsync} = $snap_time;
	$job->{state} = "ok";
	$jobs->{$vmid} = $job;
	write_state($jobs);
    };

    PVE::Tools::lock_file_full($STATE_PATH, 60, 0 , $sync_job);
    die $@ if $@;

    return $snap_time;
}

sub get_snapshots {
    my ($vol, $prefix, $nodes) = @_;

    my $plugin = $vol->{plugin};
    return $plugin->get_snapshots($vol, $prefix, $nodes);
}

sub send_image {
    my ($vol, $param, $ip, $all_snaps_in_delta, $alter_path) = @_;

    my $plugin = $vol->{plugin};
    $plugin->send_image($vol, $param, $ip, $all_snaps_in_delta, $alter_path);
}

sub job_enable {
    my ($vmid, $no_sync, $target) = @_;

    my $update_state = sub {
	my ($state) = @_;

	my $jobs = read_state();
	my $job = $jobs->{$vmid};
	my ($config) = &$get_guestconfig($vmid);
	my $param = {};

	$job->{interval} = $config->{replica_interval} || 15;

	$job->{tnode} = $target || $config->{replica_target};
	die "Replication target must be set\n" if !defined($job->{tnode});

	die "Target and source node can't be the same\n"
	    if $job->{tnode} eq $local_node;

	$job->{fail} = 0;
	if (!defined($job->{lastsync})) {

	    if ( my $lastsync = get_lastsync($vmid)) {
		$job->{lastsync} = $lastsync;
	    } else {
		$job->{lastsync} = 0;
	    }
	}

	$param->{verbose} = 1;

	$job->{state} = 'ok';
	$jobs->{$vmid} = $job;
	write_state($jobs);

	eval{
	    sync_guest($vmid, $param) if !defined($no_sync);
	};
	if (my $err = $@) {
	    $jobs->{$vmid}->{state} = 'error';
	    write_state($jobs);
	    die $err;
	}
    };

    PVE::Tools::lock_file_full($STATE_PATH, 5, 0 , $update_state);
    die $@ if $@;
}

sub job_disable {
    my ($vmid) = @_;

    my $update_state = sub {

	my $jobs = read_state();

	if (defined($jobs->{$vmid})) {
	    $jobs->{$vmid}->{state} = 'off';
	    write_state($jobs);
	} else {
	    print "No replica service for $vmid\n";
	}
    };

    PVE::Tools::lock_file_full($STATE_PATH, 5, 0 , $update_state);
    die $@ if $@;
}

sub job_remove {
    my ($vmid) = @_;

    my $update_state = sub {

	my $jobs = read_state();

	if (defined($jobs->{$vmid})) {
	    delete($jobs->{$vmid});
	    write_state($jobs);
	} else {
	    print "No replica service for $vmid\n";
	}
    };

    PVE::Tools::lock_file_full($STATE_PATH, 5, 0 , $update_state);
    die $@ if $@;
}

sub get_syncable_guestdisks {
    my ($config, $vm_type, $running, $noerr) = @_;

    my $syncable_disks = {};

    my $cfg = PVE::Storage::config();

    my $warnings = 0;
    my $func = sub {
	my ($id, $volume) = @_;

	my $volname;
	if ($vm_type eq 'qemu') {
	    $volname = $volume->{file};
	} else {
	    $volname = $volume->{volume};
	}

	if( PVE::Storage::volume_has_feature($cfg, 'replicate', $volname , undef, $running)) {
	    $syncable_disks->{$volname} = 1;
	} else {
	    warn "Can't sync Volume: $volname\n"
		if !$noerr &&
		   (!defined($volume->{replica}) || $volume->{replica});
	    $warnings = 1;
	}
    };

    if ($vm_type eq 'qemu') {
	PVE::QemuServer::foreach_drive($config, $func);
    } elsif ($vm_type eq 'lxc') {
	PVE::LXC::Config->foreach_mountpoint($config, $func);
    } else {
	die "Unknown VM type: $vm_type";
    }

    return wantarray ? ($warnings, $syncable_disks) : $syncable_disks;
}

sub destroy_all_snapshots {
    my ($vmid, $regex, $node) = @_;

    my $ip = defined($node) ? get_node_ip($node) : undef;

    my ($guest_conf, $vm_type, $running) = &$get_guestconfig($vmid);

    my $disks = get_syncable_guestdisks($guest_conf, $vm_type);
    my $cfg = PVE::Storage::config();

    my $snapshots = {};
    foreach my $volid (keys %$disks) {
	$snapshots->{$volid} =
	    PVE::Storage::volume_snapshot_list($cfg, $volid, $regex, $node, $ip);
    }

    foreach my $volid (keys %$snapshots) {

	if (defined($regex)) {
	    foreach my $snap (@{$snapshots->{$volid}}) {
		if ($ip) {
		    PVE::Storage::volume_snapshot_delete_remote($cfg, $volid, $snap, $ip);
		} else {
		    PVE::Storage::volume_snapshot_delete($cfg, $volid, $snap, $running);
		}
	    }
	} else {
	    if ($ip) {

		my $cmd = ['ssh', '-o', 'Batchmode=yes', "root\@$ip", '--'
		   ,'pvesm', 'free', $volid];
		PVE::Tools::run_command($cmd);
	    } else {
		PVE::Storage::vdisk_free($cfg, $volid);
	    }
	}
    }

}

sub cleanup_snapshot {
    my ($disks, $snapname, $cfg, $running, $ip, $lastsync_snap) = @_;

    if ($lastsync_snap) {
	$snapname = "replica_$lastsync_snap";
    }

    foreach my $volid (keys %$disks) {
	next if $volid eq "snapname";

	if (defined($lastsync_snap) || $disks->{$volid}->{synced}) {
	    PVE::Storage::volume_snapshot_delete_remote($cfg, $volid, $snapname, $ip);
	}

	if (defined($lastsync_snap) || $disks->{$volid}->{snapshot}) {
	    PVE::Storage::volume_snapshot_delete($cfg, $volid, $snapname, $running);
	}
    }
}

sub destroy_replica {
    my ($vmid) = @_;

    my $code = sub {

	my $jobs = read_state();

	return if !defined($jobs->{$vmid});

	my ($guest_conf, $vm_type) = &$get_guestconfig($vmid);

	destroy_all_snapshots($vmid, 'replica');
	destroy_all_snapshots($vmid, undef, $guest_conf->{replica_target});

	delete($jobs->{$vmid});

	delete($guest_conf->{replica_rate_limit});
	delete($guest_conf->{replica_rate_interval});
	delete($guest_conf->{replica_target});
	delete($guest_conf->{replica});

	if ($vm_type eq 'qemu') {
	    PVE::QemuConfig->write_config($vmid, $guest_conf);
	} else {
	    PVE::LXC::Config->write_config($vmid, $guest_conf);
	}
	write_state($jobs);
    };

    PVE::Tools::lock_file_full($STATE_PATH, 30, 0 , $code);
    die $@ if $@;
}

sub get_lastsync {
    my ($vmid) = @_;

    my ($conf, $vm_type) = &$get_guestconfig($vmid);

    my $sync_vol = get_syncable_guestdisks($conf, $vm_type);
    my $cfg = PVE::Storage::config();

    my $time;
    foreach my $volid (keys %$sync_vol) {
	my $list =
	    PVE::Storage::volume_snapshot_list($cfg, $volid, 'replica', $local_node);

	if (my $tmp_snap = shift @$list) {
	    $tmp_snap =~ m/^replica_(\d+)$/;
	    die "snapshots are not coherent\n"
		if defined($time) && !($time eq $1);
	    $time = $1;
	}
    }

    return $time;
}

sub get_last_replica_snap {
    my ($volid) = @_;

    my $cfg = PVE::Storage::config();
    my $list = PVE::Storage::volume_snapshot_list($cfg, $volid, 'replica_', $local_node);

    return shift @$list;
}

sub check_guest_volumes_syncable {
    my ($conf, $vm_type) = @_;

    my ($warnings, $disks) = get_syncable_guestdisks($conf, $vm_type, 1);

    return undef if $warnings || !%$disks;

    return 1;
}

sub update_conf {
    my ($vmid, $key, $value) = @_;

    if ($key eq 'replica_target') {
	    destroy_replica($vmid);
	    job_enable($vmid, undef, $value);
	    return;
    }

    my $update = sub {
	my $jobs = read_state();

	return if !defined($jobs->{$vmid});

	if ($key eq 'replica_interval') {
	    $jobs->{$vmid}->{interval} = $value || 15;
	} elsif ($key eq 'replica_rate_limit'){
		$jobs->{$vmid}->{limit} = $value ||
		    delet $jobs->{$vmid}->{limit};
	}  else {
	    die "Config parameter $key not known";
	}

	write_state($jobs);
    };

    PVE::Tools::lock_file_full($STATE_PATH, 60, 0 , $update);
}

1;
