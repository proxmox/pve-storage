package PVE::Storage::SheepdogPlugin;

use strict;
use warnings;
use IO::File;
use PVE::Tools qw(run_command trim);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

my $collie_cmd = sub {
    my ($scfg, $class, $op, @options) = @_;

    my $portal = $scfg->{portal};
    my ($server, $port) = split(':', $portal);
    my $cmd = ['/usr/sbin/collie', $class, $op, '-a', $server];
    push @$cmd, '-p', $port if $port;

    push @$cmd, @options if scalar(@options);

    return $cmd;
};

sub sheepdog_ls {
    my ($scfg, $storeid) = @_;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'graph');

    my $relationship = {};
    my $child = undef;

    run_command($cmd, outfunc => sub {
        my $line = shift;

	my $parent = undef;
	my $name = undef;

        $line = trim($line);

	if ($line =~ /\"(\S+)\"\s->\s\"(\S+)\"/) {
	    $parent = $1;
	    $child = $2;
	    $relationship->{$child}->{parent} = $parent;
	}
	elsif ($line =~ /group\s\=\s\"(\S+)\",/) {
	    $name = $1;
	    $relationship->{$child}->{name} = $name if $child;

	}

    });


    $cmd = &$collie_cmd($scfg, 'vdi', 'list', '-r');

    my $list = {};

    run_command($cmd, outfunc => sub {
        my $line = shift;
        $line = trim($line);
	if ($line =~ /(=|c) ((vm|base)-(\d+)-\S+)\s+(\d+)\s+(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\S+)\s(\d+)/) {
	    my $image = $2;
	    my $owner = $4;
	    my $size = $5;
	    my $idvdi = $10;
	    my $parentid = $relationship->{$idvdi}->{parent} if $relationship->{$idvdi}->{parent};
	    my $parent = $relationship->{$parentid}->{name} if $parentid;
	    $list->{$storeid}->{$image} = {
		name => $image,
		size => $size,
		parent => $parent,
		vmid => $owner
	    };
	}
    });

    return $list;
}

sub sheepdog_snapshot_ls {
    my ($scfg, $volname) = @_;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'list', '-r');

    my $list = {};
    run_command($cmd, outfunc => sub {
        my $line = shift;
        $line = trim($line);
	if ($line =~ /s\s(\S+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\S+)\s(\d+)\s(\S+)/) {
	    $list->{$9} = 1;
	}
    });

    return $list;
}

# Configuration


sub type {
    return 'sheepdog';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}


sub options {
    return {
        nodes => { optional => 1 },
        disable => { optional => 1 },
	portal => { fixed => 1 },
	content => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^((base-(\d+)-\S+)\/)?((base)?(vm)?-(\d+)-\S+)$/) {
	return ('images', $4, $7, $2, $3, $5);
    }

    die "unable to parse rbd volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $portal = $scfg->{portal};
    my ($server, $port) = split(':', $portal);
    $port = 7000 if !$port;

    my $path = "sheepdog:$server:$port:$name";

    return ($path, $vmid, $vtype);
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "not implemented";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid) = @_;

    die "not implemented";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;


    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
	if  $name && $name !~ m/^vm-$vmid-/;

    if (!$name) {
	my $sheepdog = sheepdog_ls($scfg, $storeid);

	for (my $i = 1; $i < 100; $i++) {
	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($sheepdog->{$storeid}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'create', $name , "${size}KB");

    run_command($cmd, errmsg => "sheepdog create $name' error");

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my $snapshots = sheepdog_snapshot_ls($scfg, $volname);
    while (my ($snapname) = each %$snapshots) {
	my $cmd = &$collie_cmd($scfg, 'vdi', 'delete' , '-s', $snapname, $volname);
	run_command($cmd, errmsg => "sheepdog delete snapshot $snapname $volname' error");
    }

    my $cmd = &$collie_cmd($scfg, 'vdi', 'delete' , $volname);

    run_command($cmd, errmsg => "sheepdog delete $volname' error");

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{sheepdog} = sheepdog_ls($scfg, $storeid) if !$cache->{sheepdog};
    my $res = [];

    if (my $dat = $cache->{sheepdog}->{$storeid}) {
        foreach my $image (keys %$dat) {

            my $volname = $dat->{$image}->{name};

            my $volid = "$storeid:$volname";

            my $owner = $dat->{$volname}->{vmid};
            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                next if defined ($vmid) && ($owner ne $vmid);
            }

            my $info = $dat->{$volname};
            $info->{volid} = $volid;
	    $info->{format} = 'raw';
            push @$res, $info;
        }
    }

   return $res;
}


sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 1;

    my $cmd = &$collie_cmd($scfg, 'node', 'info' , '-r');

    my $parser = sub {
        my $line = shift;
	if ($line =~ m/^Total\s(\d+)\s(\d+)\s/) {
	    $total = $1;
	    $used = $2;
	    $free = $total - $used;
	}
    };

    run_command($cmd, outfunc => $parser, errmsg => "sheepdog node info error");

    return ($total,$free,$used,$active);

    return undef;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my $size = undef;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'list', '-r');

    run_command($cmd, outfunc => sub {
        my $line = shift;
        $line = trim($line);
        if ($line =~ /(=|c) $volname\s+(\d+)\s+(\d+)\s(\d+)\s(\d+)\s/) {
            $size = $3;

        }
    });

    return $size;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'resize' , $volname, $size);
    run_command($cmd, errmsg => "sheepdog resize $volname' error");

    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'snapshot', '-s', $snap, $volname);
    run_command($cmd, errmsg => "sheepdog snapshot $volname' error");

    return undef;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'rollback', '-s', $snap, $volname);
    run_command($cmd, errmsg => "sheepdog snapshot $volname' error");

}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    return 1 if $running;

    my $cmd = &$collie_cmd($scfg, 'vdi', 'delete', '-s', $snap, $volname);
    run_command($cmd, errmsg => "sheepdog snapshot $volname' error");

    return undef;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => 1, snap => 1},
	clone => { snap => 1},
    };

    my $snap = $snapname ? 'snap' : 'current';
    return 1 if $features->{$feature}->{$snap};

    return undef;
}

1;
