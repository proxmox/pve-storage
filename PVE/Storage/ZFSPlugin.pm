package PVE::Storage::ZFSPlugin;

use strict;
use warnings;
use IO::File;
use POSIX;
use PVE::Tools qw(run_command);
use PVE::Storage::Plugin;

use base qw(PVE::Storage::Plugin);
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
    my ($scfg, $timeout, $method, @params) = @_;

    my $cmdmap;
    my $zfscmd;
    my $target;
    my $msg;

    $timeout = 5 if !$timeout;

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
        if ($method eq 'zpool_list') {
            $zfscmd = 'zpool';
            $method = 'list',
        } else {
            $zfscmd = 'zfs';
        }

        $target = 'root@' . $scfg->{portal};

        my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $zfscmd, $method, @params];

        $msg = '';

        my $output = sub {
        my $line = shift;
        $msg .= "$line\n";
        };

        run_command($cmd, outfunc => $output, timeout => $timeout);
    }

    return $msg;
}

sub zfs_parse_size {
    my ($text) = @_;

    return 0 if !$text;

    if ($text =~ m/^(\d+(\.\d+)?)([TGMK])?$/) {
    my ($size, $reminder, $unit) = ($1, $2, $3);
    return $size if !$unit;
    if ($unit eq 'K') {
        $size *= 1024;
    } elsif ($unit eq 'M') {
        $size *= 1024*1024;
    } elsif ($unit eq 'G') {
        $size *= 1024*1024*1024;
    } elsif ($unit eq 'T') {
        $size *= 1024*1024*1024*1024;
    }

    if ($reminder) {
        $size = ceil($size);
    }
    return $size;
    } else {
    return 0;
    }
}

sub zfs_get_pool_stats {
    my ($scfg) = @_;

    my $available = 0;
    my $used = 0;

    my $text = zfs_request($scfg, undef, 'get', '-o', 'value', '-Hp',
               'available,used', $scfg->{pool});

    my @lines = split /\n/, $text;

    if($lines[0] =~ /^(\d+)$/) {
    $available = $1;
    }

    if($lines[1] =~ /^(\d+)$/) {
    $used = $1;
    }

    return ($available, $used);
}

sub zfs_parse_zvol_list {
    my ($text) = @_;

    my $list = ();

    return $list if !$text;

    my @lines = split /\n/, $text;
    foreach my $line (@lines) {
    if ($line =~ /^(.+)\s+([a-zA-Z0-9\.]+|\-)\s+(.+)$/) {
        my $zvol = {};
        my @parts = split /\//, $1;
        my $name = pop @parts;
        my $pool = join('/', @parts);

        if ($pool !~ /^rpool$/) {
            next unless $name =~ m!^(\w+)-(\d+)-(\w+)-(\d+)$!;
            $name = $pool . '/' . $name;
        } else {
            next;
        }

        $zvol->{pool} = $pool;
        $zvol->{name} = $name;
        $zvol->{size} = zfs_parse_size($2);
        if ($3 !~ /^-$/) {
            $zvol->{origin} = $3;
        }
        push @$list, $zvol;
    }
    }

    return $list;
}

sub zfs_get_lu_name {
    my ($scfg, $zvol) = @_;
    my $object;

    my $base = $zfs_get_base->($scfg);
    if ($zvol =~ /^.+\/.+/) {
        $object = "$base/$zvol";
    } else {
        $object = "$base/$scfg->{pool}/$zvol";
    }

    my $lu_name = zfs_request($scfg, undef, 'list_lu', $object);

    return $lu_name if $lu_name;

    die "Could not find lu_name for zvol $zvol";
}

sub zfs_get_zvol_size {
    my ($scfg, $zvol) = @_;

    my $text = zfs_request($scfg, undef, 'get', '-Hp', 'volsize', "$scfg->{pool}/$zvol");

    if($text =~ /volsize\s(\d+)/){
    return $1;
    }

    die "Could not get zvol size";
}

sub zfs_add_lun_mapping_entry {
    my ($scfg, $zvol, $guid) = @_;

    if (! defined($guid)) {
    $guid = zfs_get_lu_name($scfg, $zvol);
    }

    zfs_request($scfg, undef, 'add_view', $guid);
}

sub zfs_delete_lu {
    my ($scfg, $zvol) = @_;

    my $guid = zfs_get_lu_name($scfg, $zvol);

    zfs_request($scfg, undef, 'delete_lu', $guid);
}

sub zfs_create_lu {
    my ($scfg, $zvol) = @_;

    my $base = $zfs_get_base->($scfg);
    my $guid = zfs_request($scfg, undef, 'create_lu', "$base/$scfg->{pool}/$zvol");

    return $guid;
}

sub zfs_import_lu {
    my ($scfg, $zvol) = @_;

    my $base = $zfs_get_base->($scfg);
    zfs_request($scfg, undef, 'import_lu', "$base/$scfg->{pool}/$zvol");
}

sub zfs_resize_lu {
    my ($scfg, $zvol, $size) = @_;

    my $guid = zfs_get_lu_name($scfg, $zvol);

    zfs_request($scfg, undef, 'modify_lu', "${size}K", $guid);
}

sub zfs_create_zvol {
    my ($scfg, $zvol, $size) = @_;
    
    my $sparse = '';
    if ($scfg->{sparse}) {
        $sparse = '-s';
    }

    zfs_request($scfg, undef, 'create', $sparse, '-b', $scfg->{blocksize}, '-V', "${size}k", "$scfg->{pool}/$zvol");
}

sub zfs_delete_zvol {
    my ($scfg, $zvol) = @_;

    zfs_request($scfg, undef, 'destroy', '-r', "$scfg->{pool}/$zvol");
}

sub zfs_get_lun_number {
    my ($scfg, $guid) = @_;

    die "could not find lun_number for guid $guid" if !$guid;

    return zfs_request($scfg, undef, 'list_view', $guid);
}

sub zfs_list_zvol {
    my ($scfg) = @_;

    my $text = zfs_request($scfg, 10, 'list', '-o', 'name,volsize,origin', '-t', 'volume', '-Hr');
    my $zvols = zfs_parse_zvol_list($text);
    return undef if !$zvols;

    my $list = ();
    foreach my $zvol (@$zvols) {
    my @values = split('/', $zvol->{name});

    my $image = pop @values;
    my $pool = join('/', @values);

    next if $image !~ m/^((vm|base)-(\d+)-\S+)$/;
    my $owner = $3;

    my $parent = $zvol->{origin};
    if($zvol->{origin} && $zvol->{origin} =~ m/^$scfg->{pool}\/(\S+)$/){
        $parent = $1;
    }

    $list->{$pool}->{$image} = {
        name => $image,
        size => $zvol->{size},
        parent => $parent,
        format => 'raw',
        vmid => $owner
    };
    }

    return $list;
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
    blocksize => {
        description => "block size",
        type => 'string',
    },
    sparse => {
        description => "use sparse volumes",
        type => 'boolean',
	optional => 1,
    }
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
    sparse => { optional => 1 },
    content => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^(((base|vm)-(\d+)-\S+)\/)?((base)?(vm)?-(\d+)-\S+)$/) {
    return ('images', $5, $8, $2, $4, $6);
    }

    die "unable to parse zfs volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $guid = zfs_get_lu_name($scfg, $name);
    my $lun = zfs_get_lun_number($scfg, $guid);

    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}

my $find_free_diskname = sub {
    my ($storeid, $scfg, $vmid) = @_;

    my $name = undef;
    my $volumes = zfs_list_zvol($scfg);

    my $disk_ids = {};
    my $dat = $volumes->{$scfg->{pool}};

    foreach my $image (keys %$dat) {
        my $volname = $dat->{$image}->{name};
        if ($volname =~ m/(vm|base)-$vmid-disk-(\d+)/){
            $disk_ids->{$2} = 1;
        }
    }

    for (my $i = 1; $i < 100; $i++) {
        if (!$disk_ids->{$i}) {
            return "vm-$vmid-disk-$i";
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n";
};

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $snap = '__base__';

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    die "create_base not possible with base image\n" if $isBase;

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basename/$newname" : "$newname";

    zfs_delete_lu($scfg, $name);
    zfs_request($scfg, undef, 'rename', "$scfg->{pool}/$name", "$scfg->{pool}/$newname");

    my $guid = zfs_create_lu($scfg, $newname);
    zfs_add_lun_mapping_entry($scfg, $newname, $guid);

    my $running  = undef; #fixme : is create_base always offline ?

    $class->volume_snapshot($scfg, $storeid, $newname, $snap, $running);

    return $newvolname;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid) = @_;

    my $snap = '__base__';

    my ($vtype, $basename, $basevmid, undef, undef, $isBase) =
        $class->parse_volname($volname);

    die "clone_image only works on base images\n" if !$isBase;

    my $name = &$find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename to $name\n";

    zfs_request($scfg, undef, 'clone', "$scfg->{pool}/$basename\@$snap", "$scfg->{pool}/$name");

    my $guid = zfs_create_lu($scfg, $name);
    zfs_add_lun_mapping_entry($scfg, $name, $guid);

    return $name;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
    if $name && $name !~ m/^vm-$vmid-/;

    $name = &$find_free_diskname($storeid, $scfg, $vmid);

    zfs_create_zvol($scfg, $name, $size);
    my $guid = zfs_create_lu($scfg, $name);
    zfs_add_lun_mapping_entry($scfg, $name, $guid);

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    zfs_delete_lu($scfg, $name);
    eval {
        zfs_delete_zvol($scfg, $name);
    };
    do {
        my $err = $@;
        my $guid = zfs_create_lu($scfg, $name);
        zfs_add_lun_mapping_entry($scfg, $name, $guid);
        die $err;
    } if $@;

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{zfs} = zfs_list_zvol($scfg) if !$cache->{zfs};
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

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 0;

    eval {
    ($free, $used) = zfs_get_pool_stats($scfg);
    $active = 1;
    $total = $free + $used;
    };
    warn $@ if $@;

    return ($total, $free, $used, $active);
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

    return zfs_get_zvol_size($scfg, $volname);
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my $new_size = ($size/1024);

    zfs_request($scfg, undef, 'set', 'volsize=' . $new_size . 'k', "$scfg->{pool}/$volname");
    zfs_resize_lu($scfg, $volname, $new_size);
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    zfs_request($scfg, undef, 'snapshot', "$scfg->{pool}/$volname\@$snap");
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    zfs_delete_lu($scfg, $volname);

    zfs_request($scfg, undef, 'rollback', "$scfg->{pool}/$volname\@$snap");

    zfs_import_lu($scfg, $volname);

    zfs_add_lun_mapping_entry($scfg, $volname);
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    zfs_request($scfg, undef, 'destroy', "$scfg->{pool}/$volname\@$snap");
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
