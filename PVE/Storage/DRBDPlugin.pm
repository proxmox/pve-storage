package PVE::Storage::DRBDPlugin;

use strict;
use warnings;
use IO::File;
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

    die "fixme";
    
    my $vg = $scfg->{vgname};
    
    my $path = "/dev/$vg/$name";

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


    if (!$name) {
	die "fixme";
	
	my $lvs = {};

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($lvs->{fixme})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my $cmd = ['drbdmanage', 'new-volume', $name];

    # fixme: deploy
    
    run_command($cmd, errmsg => "drbdmanage new-volume error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;
 
    my $cmd = ['drbdmanage', 'remove-volume', $volname];

    # fixme: undeploy
    
    run_command($cmd, errmsg => "drbdmanage remove-volume error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    #$cache->{lvs} = lvm_lvs() if !$cache->{lvs};

    my $res = [];

    die "fixme";
	
    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    die "fixme";
    
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

    die "fixme";
    
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
