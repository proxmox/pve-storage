package PVE::Storage::DRBDPlugin;

use strict;
use warnings;
use IO::File;
use Net::DBus;
use Data::Dumper;

use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration 

sub type {
    return 'drbd';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}

sub properties {
    return {
	redundancy => {
	    description => "The redundancy count specifies the number of nodes to which the resource should be deployed. It must be at least 1 and at most the number of nodes in the cluster.",
	    type => 'integer',
	    minimum => 1,
	    maximum => 16,
	    default => 2,
	},
    };
}

sub options {
    return {
        redundancy => { optional => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
    };
}

# helper

sub connect_drbdmanage_service {

    my $bus = Net::DBus->system;

    my $service = $bus->get_service("org.drbd.drbdmanaged");

    my $hdl = $service->get_object("/interface", "org.drbd.drbdmanaged");

    return $hdl;
}

sub check_drbd_rc {
    my ($rc) = @_;
    
    die "got undefined drbd rc\n" if !$rc;

    my ($code, $msg, $details) = @$rc;

    return undef if $code == 0;

    $msg = "drbd error: got error code $code" if !$msg;
    chomp $msg;
    
    # fixme: add error details?
    #print Dumper($details);
    
    die "drbd error: $msg\n";
}

sub drbd_list_volumes {
    my ($hdl) = @_;
    
    $hdl = connect_drbdmanage_service() if !$hdl;
    
    my ($rc, $res) = $hdl->list_volumes([], 0, {}, []);
    check_drbd_rc($rc->[0]);

    my $volumes = {};
    
    foreach my $entry (@$res) {
	my ($volname, $properties, $vol_list) = @$entry;

	next if $volname !~ m/^vm-(\d+)-/;
	my $vmid = $1;

	# fixme: we always use volid 0 ?
	my $size = 0;
	foreach my $volentry (@$vol_list) {
	    my ($vol_id, $vol_properties) = @$volentry;
	    next if $vol_id != 0;
	    my $vol_size = $vol_properties->{vol_size} * 1024;
	    $size = $vol_size if $vol_size > $size;
	}

	$volumes->{$volname} = { format => 'raw', size => $size, 
				 vmid => $vmid };
    }
    
    return $volumes; 
}
    
# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^(vm-(\d+)-[a-z][a-z0-9\-\_\.]*[a-z0-9]+)$/) {
	return ('images', $1, $2);
    }

    die "unable to parse lvm volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    # fixme: always use volid 0?
    my $path = "/dev/drbd/by-res/$volname/0";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in drbd storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in drbd storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n" 
	if  $name && $name !~ m/^vm-$vmid-/;

    my $hdl = connect_drbdmanage_service();
    my $volumes = drbd_list_volumes($hdl);

    die "volume '$name' already exists\n" if $volumes->{$name};
    
    if (!$name) {	
	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($volumes->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;
    
    my ($rc, $res) = $hdl->create_resource($name, {});
    check_drbd_rc($rc->[0]);

    ($rc, $res) = $hdl->create_volume($name, $size, {});
    check_drbd_rc($rc->[0]);

    ($rc, $res) = $hdl->auto_deploy($name, $scfg->{redundancy}, 0, 0);
    check_drbd_rc($rc->[0]);

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;
 
    my $hdl = connect_drbdmanage_service();
    my ($rc, $res) = $hdl->remove_resource($volname, 0);
    check_drbd_rc($rc->[0]);

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{drbd_volumes} = drbd_list_volumes() if !$cache->{drbd_volumes};
	
    my $res = [];

    my $dat =  $cache->{drbd_volumes};
    
    foreach my $volname (keys %$dat) {

	my $owner = $dat->{$volname}->{vmid};

	my $volid = "$storeid:$volname";

	if ($vollist) {
	    my $found = grep { $_ eq $volid } @$vollist;
	    next if !$found;
	} else {
	    next if defined ($vmid) && ($owner ne $vmid);
	}

	my $info = $dat->{$volname};
	$info->{volid} = $volid;

	push @$res, $info;
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    eval {
	my $hdl = connect_drbdmanage_service();
	my ($rc, $res) = $hdl->cluster_free_query($scfg->{redundancy});
	check_drbd_rc($rc->[0]);

	my $avail = $res;
	my $used = 0; # fixme
	my $total = $used + $avail;

	return ($total, $avail, $used, 1);
    };

    # ignore error,
    # assume storage if offline
    
    return undef;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return undef;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return undef;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;

    return undef;    
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $cache) = @_;

    return undef;    
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    $size = ($size/1024/1024) . "M";

    my $path = $class->path($scfg, $volname);

    # fixme: howto implement this
    die "drbd volume_resize is not implemented";
    
    #my $cmd = ['/sbin/lvextend', '-L', $size, $path];
    #run_command($cmd, errmsg => "error resizing volume '$path'");

    return 1;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "drbd snapshot is not implemented";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "drbd snapshot rollback is not implemented";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "drbd snapshot delete is not implemented";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	copy => { base => 1, current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
	$key = 'snap';
    }else{
	$key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
