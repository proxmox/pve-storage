package PVE::Storage::ZFSPlugin;

use strict;
use warnings;
use IO::File;
use POSIX;
use PVE::Tools qw(run_command);
use PVE::Storage::ZFSDirPlugin;

use base qw(PVE::Storage::ZFSDirPlugin);
use PVE::Storage::LunCmd::Comstar;
use PVE::Storage::LunCmd::Istgt;
use PVE::Storage::LunCmd::Iet;

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';

my $lun_cmds = {
    create_lu   => 1,
    delete_lu   => 1,
    import_lu   => 1,
    modify_lu   => 1,
    add_view    => 1,
    list_view   => 1,
    list_lu     => 1,
};

my $zfs_unknown_scsi_provider = sub {
    my ($provider) = @_;

    die "$provider: unknown iscsi provider. Available [comstar, istgt, iet]";
};

my $zfs_get_base = sub {
    my ($scfg) = @_;

    if ($scfg->{iscsiprovider} eq 'comstar') {
        return PVE::Storage::LunCmd::Comstar::get_base;
    } elsif ($scfg->{iscsiprovider} eq 'istgt') {
        return PVE::Storage::LunCmd::Istgt::get_base;
    } elsif ($scfg->{iscsiprovider} eq 'iet') {
        return PVE::Storage::LunCmd::Iet::get_base;
    } else {
        $zfs_unknown_scsi_provider->($scfg->{iscsiprovider});
    }
};

sub zfs_request {
    my ($class, $scfg, $timeout, $method, @params) = @_;

    $timeout = 5 if !$timeout;

    my $msg = '';

    if ($lun_cmds->{$method}) {
        if ($scfg->{iscsiprovider} eq 'comstar') {
            $msg = PVE::Storage::LunCmd::Comstar::run_lun_command($scfg, $timeout, $method, @params);
        } elsif ($scfg->{iscsiprovider} eq 'istgt') {
            $msg = PVE::Storage::LunCmd::Istgt::run_lun_command($scfg, $timeout, $method, @params);
        } elsif ($scfg->{iscsiprovider} eq 'iet') {
            $msg = PVE::Storage::LunCmd::Iet::run_lun_command($scfg, $timeout, $method, @params);
        } else {
            $zfs_unknown_scsi_provider->($scfg->{iscsiprovider});
        }
    } else {

	my $target = 'root@' . $scfg->{portal};

	my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target];

        if ($method eq 'zpool_list') {
	    push @$cmd, 'zpool', 'list';
	} else {
	    push @$cmd, 'zfs', $method;
        }

	push @$cmd, @params;

	my $output = sub {
	    my $line = shift;
	    $msg .= "$line\n";
        };

        run_command($cmd, outfunc => $output, timeout => $timeout);
    }

    return $msg;
}

sub zfs_get_lu_name {
    my ($class, $scfg, $zvol) = @_;

    my $base = $zfs_get_base->($scfg);

    my $object = ($zvol =~ /^.+\/.+/) ? "$base/$zvol" : "$base/$scfg->{pool}/$zvol";

    my $lu_name = $class->zfs_request($scfg, undef, 'list_lu', $object);

    return $lu_name if $lu_name;

    die "Could not find lu_name for zvol $zvol";
}

sub zfs_add_lun_mapping_entry {
    my ($class, $scfg, $zvol, $guid) = @_;

    if (!defined($guid)) {
	$guid = $class->zfs_get_lu_name($scfg, $zvol);
    }

    $class->zfs_request($scfg, undef, 'add_view', $guid);
}

sub zfs_delete_lu {
    my ($class, $scfg, $zvol) = @_;

    my $guid = $class->zfs_get_lu_name($scfg, $zvol);

    $class->zfs_request($scfg, undef, 'delete_lu', $guid);
}

sub zfs_create_lu {
    my ($class, $scfg, $zvol) = @_;

    my $base = $zfs_get_base->($scfg);
    my $guid = $class->zfs_request($scfg, undef, 'create_lu', "$base/$scfg->{pool}/$zvol");

    return $guid;
}

sub zfs_import_lu {
    my ($class, $scfg, $zvol) = @_;

    my $base = $zfs_get_base->($scfg);
    $class->zfs_request($scfg, undef, 'import_lu', "$base/$scfg->{pool}/$zvol");
}

sub zfs_resize_lu {
    my ($class, $scfg, $zvol, $size) = @_;

    my $guid = $class->zfs_get_lu_name($scfg, $zvol);

    $class->zfs_request($scfg, undef, 'modify_lu', "${size}K", $guid);
}

sub zfs_get_lun_number {
    my ($class, $scfg, $guid) = @_;

    die "could not find lun_number for guid $guid" if !$guid;

    return $class->zfs_request($scfg, undef, 'list_view', $guid);
}

# Configuration

sub type {
    return 'zfs';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}

sub properties {
    return {
	iscsiprovider => {
	    description => "iscsi provider",
	    type => 'string',
	},
	# this will disable write caching on comstar and istgt.
	# it is not implemented for iet. iet blockio always operates with
	# writethrough caching when not in readonly mode
	nowritecache => {
	    description => "disable write caching on the target",
	    type => 'boolean',
	},
	comstar_tg => {
	    description => "target group for comstar views",
	    type => 'string',
	},
	comstar_hg => {
	    description => "host group for comstar views",
	    type => 'string',
	},
    };
}

sub options {
    return {
	nodes => { optional => 1 },
	disable => { optional => 1 },
	portal => { fixed => 1 },
	target => { fixed => 1 },
	pool => { fixed => 1 },
	blocksize => { fixed => 1 },
	iscsiprovider => { fixed => 1 },
	nowritecache => { optional => 1 },
	sparse => { optional => 1 },
	comstar_hg => { optional => 1 },
	comstar_tg => { optional => 1 },
	content => { optional => 1 },
    };
}

# Storage implementation

sub path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $guid = $class->zfs_get_lu_name($scfg, $name);
    my $lun = $class->zfs_get_lun_number($scfg, $guid);

    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $snap = '__base__';

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    die "create_base not possible with base image\n" if $isBase;

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basename/$newname" : "$newname";

    $class->zfs_delete_lu($scfg, $name);
    $class->zfs_request($scfg, undef, 'rename', "$scfg->{pool}/$name", "$scfg->{pool}/$newname");

    my $guid = $class->zfs_create_lu($scfg, $newname);
    $class->zfs_add_lun_mapping_entry($scfg, $newname, $guid);

    my $running  = undef; #fixme : is create_base always offline ?

    $class->volume_snapshot($scfg, $storeid, $newname, $snap, $running);

    return $newvolname;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    $snap ||= '__base__';

    my ($vtype, $basename, $basevmid, undef, undef, $isBase) =
        $class->parse_volname($volname);

    die "clone_image only works on base images\n" if !$isBase;

    my $name = $class->zfs_find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename to $name\n";

    $class->zfs_request($scfg, undef, 'clone', "$scfg->{pool}/$basename\@$snap", "$scfg->{pool}/$name");

    my $guid = $class->zfs_create_lu($scfg, $name);
    $class->zfs_add_lun_mapping_entry($scfg, $name, $guid);

    return $name;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;
    
    my $volname = $class->SUPER::alloc_image($storeid, $scfg, $vmid, $fmt, $name, $size);
 
    my $guid = $class->zfs_create_lu($scfg, $volname);
    $class->zfs_add_lun_mapping_entry($scfg, $volname, $guid);

    return $volname;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    $class->zfs_delete_lu($scfg, $name);

    eval { $class->zfs_delete_zvol($scfg, $name); };
    if (my $err = $@) {
        my $guid = $class->zfs_create_lu($scfg, $name);
        $class->zfs_add_lun_mapping_entry($scfg, $name, $guid);
        die $err;
    }

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{zfs} = $class->zfs_list_zvol($scfg) if !$cache->{zfs};
    my $zfspool = $scfg->{pool};
    my $res = [];

    if (my $dat = $cache->{zfs}->{$zfspool}) {

	foreach my $image (keys %$dat) {

	    my $volname = $dat->{$image}->{name};
	    my $parent = $dat->{$image}->{parent};

	    my $volid = undef;
            if ($parent && $parent =~ m/^(\S+)@(\S+)$/) {
		my ($basename) = ($1);
		$volid = "$storeid:$basename/$volname";
	    } else {
		$volid = "$storeid:$volname";
	    }

	    my $owner = $dat->{$volname}->{vmid};
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
    }

    return $res;
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

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my $new_size = ($size/1024);

    $class->zfs_request($scfg, undef, 'set', 'volsize=' . $new_size . 'k', "$scfg->{pool}/$volname");
    $class->zfs_resize_lu($scfg, $volname, $new_size);
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    # abort rollback if snapshot is not the latest
    my @params = ('-t', 'snapshot', '-o', 'name', '-s', 'creation');
    my $text = $class->zfs_request($scfg, undef, 'list', @params);
    my @snapshots = split(/\n/, $text);
    my $recentsnap = undef;
    foreach (@snapshots) {
        if (/$scfg->{pool}\/$volname/) {
            s/^.*@//;
            $recentsnap = $_;
        }
    }
    if ($snap ne $recentsnap) {
        die "cannot rollback, more recent snapshots exist\n";
    }

    $class->zfs_delete_lu($scfg, $volname);

    $class->zfs_request($scfg, undef, 'rollback', "$scfg->{pool}/$volname\@$snap");

    $class->zfs_import_lu($scfg, $volname);

    $class->zfs_add_lun_mapping_entry($scfg, $volname);
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => 1, snap => 1},
	clone => { base => 1},
	template => { current => 1},
	copy => { base => 1, current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	$class->parse_volname($volname);

    my $key = undef;

    if ($snapname) {
	$key = 'snap';
    } else {
	$key = $isBase ? 'base' : 'current';
    }

    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
