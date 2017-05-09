package PVE::ReplicationTools;

use warnings;
use strict;
use Data::Dumper;
use JSON;

use PVE::INotify;
use PVE::Tools;
use PVE::Cluster;
use PVE::QemuConfig;
use PVE::QemuServer;
use PVE::LXC::Config;
use PVE::LXC;
use PVE::Storage;

my $STATE_DIR = '/var/lib/pve-replica';
my $STATE_PATH = "$STATE_DIR/pve-replica.state";

my $get_ssh_cmd = sub {
    my ($ip) = @_;

    return ['ssh', '-o', 'Batchmode=yes', "root\@$ip" ];
};

sub get_guest_config {
    my ($vmid) = @_;

    my $vms = PVE::Cluster::get_vmlist();

    die "no such guest '$vmid'\n" if !defined($vms->{ids}->{$vmid});

    my $vm_type = $vms->{ids}->{$vmid}->{type};

    my $conf;
    my $running;

    if ($vm_type eq 'qemu') {
	$conf = PVE::QemuConfig->load_config($vmid);
	$running = PVE::QemuServer::check_running($vmid);
    } elsif ($vm_type eq 'lxc') {
	$conf = PVE::LXC::Config->load_config($vmid);
	$running = PVE::LXC::check_running($vmid);
    } else {
	die "internal error";
    }

    return ($conf, $vm_type, $running);
}

sub write_state {
    my ($state) = @_;

    mkdir $STATE_DIR;

    PVE::Tools::file_set_contents($STATE_PATH, encode_json($state));
}

sub read_state {

    return {} if ! -e $STATE_PATH;

    my $raw = PVE::Tools::file_get_contents($STATE_PATH);

    return {} if $raw eq '';

    return decode_json($raw);
}

sub get_node_ip {
    my ($nodename) = @_;

    my $remoteip = PVE::Cluster::remote_node_ip($nodename);

    my $dc_conf = PVE::Cluster::cfs_read_file('datacenter.cfg');
    if (my $network = $dc_conf->{storage_replication_network}) {

	my $cmd = $get_ssh_cmd->($remoteip);

	push @$cmd, '--', 'pvecm', 'mtunnel', '--get_migration_ip', '--migration_network', $network;

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

    my $vms = PVE::Cluster::get_vmlist();

    my $state = read_state();

    my $jobs = {};

    my $local_node = PVE::INotify::nodename();

    foreach my $vmid (keys %{$vms->{ids}}) {
	next if $vms->{ids}->{$vmid}->{node} ne $local_node;
	my $vm_state = $state->{$vmid};
	next if !defined($vm_state);

	my $job = {};

	$job->{limit}    = $vm_state->{limit};
	$job->{interval} = $vm_state->{interval};
	$job->{tnode}    = $vm_state->{tnode};
	$job->{lastsync} = $vm_state->{lastsync};
	$job->{state}    = $vm_state->{state};
	$job->{fail}     = $vm_state->{fail};

	$jobs->{$vmid}   = $job;
    }

    return $jobs;
}

sub sync_guest {
    my ($vmid, $param) = @_;

    my $local_node = PVE::INotify::nodename();

    my $jobs = read_state();
    $jobs->{$vmid}->{state} = 'sync';
    write_state($jobs);

    my ($guest_conf, $vm_type, $running) = get_guest_config($vmid);
    my $qga = 0;

    my $job = $jobs->{$vmid};
    my $tnode = $job->{tnode};

    if ($vm_type eq 'qemu' && defined($guest_conf->{agent}) ) {
	$qga = PVE::QemuServer::qga_check_running($vmid)
	    if $running;
    }

    my $storecfg = PVE::Storage::config();
    # will not die if a disk is not syncable
    my $disks = get_replicatable_volumes($storecfg, $guest_conf, $vm_type);

    # check if all nodes have the storage availible
    foreach my $volid (keys %$disks) {
	my ($storeid) = PVE::Storage::parse_volume_id($volid);

	my $store = $storecfg->{ids}->{$storeid};
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

    my $disks_status = {};

    my $sync_job = sub {

	# make snapshot of all volumes
	foreach my $volid (keys %$disks) {

	    eval {
		PVE::Storage::volume_snapshot($storecfg, $volid, $snapname);
	    };

	    if (my $err = $@) {
		if ($qga) {
		    print "Unfreeze guest filesystem\n";
		    eval {
			PVE::QemuServer::vm_mon_cmd($vmid, "guest-fsfreeze-thaw");
		    };
		    warn $@ if $@;
		}
		cleanup_snapshot($disks_status, $snapname, $storecfg, $running);
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
		PVE::Storage::volume_send($storecfg, $volid, $snapname,
					  $ip, $incremental_snap,
					  $param->{verbose}, $limit);
		$job->{fail} = 0;
	    };

	    if (my $err = $@) {
		cleanup_snapshot($disks_status, $snapname, $storecfg, $running, $ip);
		$job->{fail}++;
		$job->{state} = 'error' if $job->{fail} > 3;

		$jobs->{$vmid} = $job;
		write_state($jobs);
		die $err;
	    }

	    $disks_status->{$volid}->{synced} = 1;
	}

	# delete old snapshot if exists
	cleanup_snapshot($disks_status, $snapname, $storecfg, $running, $ip, $lastsync) if
	    $lastsync != 0;

	$job->{lastsync} = $snap_time;
	$job->{state} = "ok";
	$jobs->{$vmid} = $job;
	write_state($jobs);
    };

    PVE::Tools::lock_file_full($STATE_PATH, 60, 0 , $sync_job);
    die $@ if $@;

    return $snap_time;
}

sub send_image {
    my ($vol, $param, $ip, $all_snaps_in_delta, $alter_path) = @_;

    my $plugin = $vol->{plugin};
    $plugin->send_image($vol, $param, $ip, $all_snaps_in_delta, $alter_path);
}

sub job_enable {
    my ($vmid, $no_sync, $target) = @_;

    my $local_node = PVE::INotify::nodename();

    my $update_state = sub {
	my ($state) = @_;

	my $jobs = read_state();
	my $job = $jobs->{$vmid};
	my ($config) = get_guest_config($vmid);
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

sub get_replicatable_volumes {
    my ($storecfg, $conf, $vm_type, $noerr) = @_;

    if ($vm_type eq 'qemu') {
	PVE::QemuConfig->get_replicatable_volumes($storecfg, $conf, $noerr);
    } elsif ($vm_type eq 'lxc') {
	PVE::LXC::Config->get_replicatable_volumes($storecfg, $conf, $noerr);
    } else {
	die "internal error";
    }
}

sub destroy_all_snapshots {
    my ($vmid, $regex, $node) = @_;

    my $ip = defined($node) ? get_node_ip($node) : undef;

    my ($guest_conf, $vm_type, $running) = get_guest_config($vmid);

    my $storecfg = PVE::Storage::config();
    my $disks = get_replicatable_volumes($storecfg, $guest_conf, $vm_type);

    my $snapshots = {};
    foreach my $volid (keys %$disks) {
	$snapshots->{$volid} =
	    PVE::Storage::volume_snapshot_list($storecfg, $volid, $regex, $node, $ip);
    }

    foreach my $volid (keys %$snapshots) {

	if (defined($regex)) {
	    foreach my $snap (@{$snapshots->{$volid}}) {
		if ($ip) {
		    PVE::Storage::volume_snapshot_delete_remote($storecfg, $volid, $snap, $ip);
		} else {
		    PVE::Storage::volume_snapshot_delete($storecfg, $volid, $snap, $running);
		}
	    }
	} else {
	    if ($ip) {

		my $cmd = $get_ssh_cmd->($ip);

		push @$cmd, '--', 'pvesm', 'free', $volid;

		PVE::Tools::run_command($cmd);
	    } else {
		die "internal error";
	    }
	}
    }

}

sub cleanup_snapshot {
    my ($disks, $snapname, $storecfg, $running, $ip, $lastsync_snap) = @_;

    if ($lastsync_snap) {
	$snapname = "replica_$lastsync_snap";
    }

    foreach my $volid (keys %$disks) {

	if (defined($ip) && (defined($lastsync_snap) || $disks->{$volid}->{synced})) {
	    PVE::Storage::volume_snapshot_delete_remote($storecfg, $volid, $snapname, $ip);
	}

	if (defined($lastsync_snap) || $disks->{$volid}->{snapshot}) {
	    PVE::Storage::volume_snapshot_delete($storecfg, $volid, $snapname, $running);
	}
    }
}

sub destroy_replica {
    my ($vmid) = @_;

    my $code = sub {

	my $jobs = read_state();

	return if !defined($jobs->{$vmid});

	my ($guest_conf, $vm_type) = get_guest_config($vmid);

	destroy_all_snapshots($vmid, 'replica_');
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

    my ($conf, $vm_type) = get_guest_config($vmid);

    my $storecfg = PVE::Storage::config();
    my $sync_vol = get_replicatable_volumes($storecfg, $conf, $vm_type);

    my $time;
    foreach my $volid (keys %$sync_vol) {
	my $list =
	    PVE::Storage::volume_snapshot_list($storecfg, $volid, 'replica_');

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

    my $storecfg = PVE::Storage::config();
    my $list = PVE::Storage::volume_snapshot_list($storecfg, $volid, 'replica_');

    return shift @$list;
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
		    delete $jobs->{$vmid}->{limit};
	}  else {
	    die "Config parameter $key not known";
	}

	write_state($jobs);
    };

    PVE::Tools::lock_file_full($STATE_PATH, 60, 0 , $update);
}

1;
