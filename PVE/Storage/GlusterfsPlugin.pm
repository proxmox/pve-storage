package PVE::Storage::GlusterfsPlugin;

use strict;
use warnings;
use IO::File;
use File::Path;
use PVE::Tools qw(run_command);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);
use Net::Ping;

use base qw(PVE::Storage::Plugin);

# Glusterfs helper functions

sub read_proc_mounts {

    local $/; # enable slurp mode

    my $data = "";
    if (my $fd = IO::File->new("/proc/mounts", "r")) {
	$data = <$fd>;
	close ($fd);
    }

    return $data;
}

sub glusterfs_is_mounted {
    my ($server, $volume, $mountpoint, $mountdata) = @_;

    my $source = "$server:$volume";

    $mountdata = read_proc_mounts() if !$mountdata;

    if ($mountdata =~ m|^$source/?\s$mountpoint\sfuse.glusterfs|m) {
	return $mountpoint;
    }

    return undef;
}

sub glusterfs_mount {
    my ($server, $volume, $mountpoint) = @_;

    my $source = "$server:$volume";

    my $cmd = ['/bin/mount', '-t', 'glusterfs', $source, $mountpoint];

    run_command($cmd, errmsg => "mount error");
}

# Configuration

sub type {
    return 'glusterfs';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1},
		     { images => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    };
}

sub properties {
    return {
	volume => {
	    description => "Glusterfs Volume.",
	    type => 'string',
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	server => { optional => 1 },
	volume => { fixed => 1 },
        nodes => { optional => 1 },
	disable => { optional => 1 },
        maxfiles => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
    };
}


sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} = "/mnt/pve/$sectionId" if $create && !$config->{path};

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

# Storage implementation

sub parse_name_dir {
    my $name = shift;

    if ($name =~ m!^((base-)?[^/\s]+\.(raw|qcow2|vmdk))$!) {
        return ($1, $3, $2);
    }

    die "unable to parse volume filename '$name'\n";
}

my $find_free_diskname = sub {
    my ($imgdir, $vmid, $fmt) = @_;

    my $disk_ids = {};
    PVE::Tools::dir_glob_foreach($imgdir,
                                 qr!(vm|base)-$vmid-disk-(\d+)\..*!,
                                 sub {
                                     my ($fn, $type, $disk) = @_;
                                     $disk_ids->{$disk} = 1;
                                 });

    for (my $i = 1; $i < 100; $i++) {
        if (!$disk_ids->{$i}) {
            return "vm-$vmid-disk-$i.$fmt";
        }
    }

    die "unable to allocate a new image name for VM $vmid in '$imgdir'\n";
};

sub path {
    my ($class, $scfg, $volname, $storeid) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';
    my $glustervolume = $scfg->{volume};

    my $path = undef;
    if($vtype eq 'images'){
	$path = "gluster://$server/$glustervolume/images/$vmid/$name";
    }else{
	my $dir = $class->get_subdir($scfg, $vtype);
	$path = "$dir/$name";
    }


    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";

    mkpath $imagedir;

    $name = &$find_free_diskname($imagedir, $vmid, $fmt) if !$name;

    my (undef, $tmpfmt) = parse_name_dir($name);

    die "illegal name '$name' - wrong extension for format ('$tmpfmt != '$fmt')\n"
        if $tmpfmt ne $fmt;

    my $path = "$imagedir/$name";

    die "disk image '$path' already exists\n" if -e $path;

    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';
    my $glustervolume = $scfg->{volume};
    my $volumepath = "gluster://$server/$glustervolume/images/$vmid/$name";

    my $cmd = ['/usr/bin/qemu-img', 'create'];

    push @$cmd, '-o', 'preallocation=metadata' if $fmt eq 'qcow2';

    push @$cmd, '-f', $fmt, $volumepath, "${size}K";

    run_command($cmd, errmsg => "unable to create image");

    return "$vmid/$name";
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';

    my $volume = $scfg->{volume};

    return undef if !glusterfs_is_mounted($server, $volume, $path, $cache->{mountdata});

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';
    my $volume = $scfg->{volume};

    if (!glusterfs_is_mounted($server, $volume, $path, $cache->{mountdata})) {

	mkpath $path;

	die "unable to activate storage '$storeid' - " .
	    "directory '$path' does not exist\n" if ! -d $path;

	glusterfs_mount($server, $volume, $path, $scfg->{options});
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = read_proc_mounts() if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';
    my $volume = $scfg->{volume};

    if (glusterfs_is_mounted($server, $volume, $path, $cache->{mountdata})) {
	my $cmd = ['/bin/umount', $path];
	run_command($cmd, errmsg => 'umount error');
    }
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;

    # do nothing by default
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $cache) = @_;

    # do nothing by default
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    my $server = $scfg->{server} ? $scfg->{server} : 'localhost';
    my $volume = $scfg->{volume};

    my $status = 0;

    if($server && $server ne 'localhost' && $server ne '127.0.0.1'){
	my $p = Net::Ping->new("tcp", 2);
	$status = $p->ping($server);

    }else{

	my $parser = sub {
	    my $line = shift;

	    if ($line =~ m/Status: Started$/) {
		$status = 1;
	    }
	};

	my $cmd = ['/usr/sbin/gluster', 'volume', 'info', $volume];

	run_command($cmd, errmsg => "glusterfs error", errfunc => sub {}, outfunc => $parser);
    }

    return $status;
}

1;
