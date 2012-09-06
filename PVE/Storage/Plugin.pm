package PVE::Storage::Plugin;

use strict;
use warnings;
use File::Path;
use PVE::Tools qw(run_command);
use PVE::JSONSchema qw(get_standard_option);
use PVE::Cluster qw(cfs_register_file);

use Data::Dumper;

use base qw(PVE::SectionConfig);

cfs_register_file ('storage.cfg', 
		   sub { __PACKAGE__->parse_config(@_); },
		   sub { __PACKAGE__->write_config(@_); });

my $defaultData = {
    propertyList => {
	type => { description => "Storage type." },
	storage => get_standard_option('pve-storage-id'),
	nodes => get_standard_option('pve-node-list', { optional => 1 }),
	content => {
	    description => "Allowed content types.",
	    type => 'string', format => 'pve-storage-content-list',
	    optional => 1,
	},
	disable => {
	    description => "Flag to disable the storage.",
	    type => 'boolean',
	    optional => 1,
	},
	maxfiles => {
	    description => "Maximal number of backup files per VM. Use '0' for unlimted.",
	    type => 'integer',
	    minimum => 0,
	    optional => 1,
	},
	shared => {
	    description => "Mark storage as shared.",
	    type => 'boolean',
	    optional => 1,
	},
	'format' => { 
	    description => "Default Image format.",
	    type => 'string', format => 'pve-storage-format',
	    optional => 1,
	},
    },
};

sub content_hash_to_string {
    my $hash = shift;

    my @cta;
    foreach my $ct (keys %$hash) {
	push @cta, $ct if $hash->{$ct};
    } 

    return join(',', @cta);
}

sub valid_content_types {
    my ($type) = @_;

    my $def = $defaultData->{plugindata}->{$type};

    return {} if !$def;

    return $def->{content}->[0];
}

sub default_format {
    my ($scfg) = @_;

    my $type = $scfg->{type};
    my $def = $defaultData->{plugindata}->{$type};
 
    my $def_format = 'raw';
    my $valid_formats = [ $def_format ];

    if (defined($def->{format})) {
	$def_format = $scfg->{format} || $def->{format}->[1];
	$valid_formats = [ sort keys %{$def->{format}->[0]} ];
    }
   
    return wantarray ? ($def_format, $valid_formats) : $def_format;
}

PVE::JSONSchema::register_format('pve-storage-path', \&verify_path);
sub verify_path {
    my ($path, $noerr) = @_;

    # fixme: exclude more shell meta characters?
    # we need absolute paths
    if ($path !~ m|^/[^;\(\)]+|) {
	return undef if $noerr;
	die "value does not look like a valid absolute path\n";
    }
    return $path;
}

PVE::JSONSchema::register_format('pve-storage-server', \&verify_server);
sub verify_server {
    my ($server, $noerr) = @_;

    # fixme: use better regex ?
    # IP or DNS name
    if ($server !~ m/^[[:alnum:]\-\.]+$/) {
	return undef if $noerr;
	die "value does not look like a valid server name or IP address\n";
    }
    return $server;
}

# fixme: do we need this
#PVE::JSONSchema::register_format('pve-storage-portal', \&verify_portal);
#sub verify_portal {
#    my ($portal, $noerr) = @_;
#
#    # IP with optional port
#    if ($portal !~ m/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?$/) {
#	return undef if $noerr;
#	die "value does not look like a valid portal address\n";
#    }
#    return $portal;
#}

PVE::JSONSchema::register_format('pve-storage-portal-dns', \&verify_portal_dns);
sub verify_portal_dns {
    my ($portal, $noerr) = @_;

    # IP or DNS name with optional port
    if ($portal !~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[[:alnum:]\-\.]+)(:\d+)?$/) {
	return undef if $noerr;
	die "value does not look like a valid portal address\n";
    }
    return $portal;
}

PVE::JSONSchema::register_format('pve-storage-content', \&verify_content);
sub verify_content {
    my ($ct, $noerr) = @_;

    my $valid_content = valid_content_types('dir'); # dir includes all types
 
    if (!$valid_content->{$ct}) {
	return undef if $noerr;
	die "invalid content type '$ct'\n";
    }

    return $ct;
}

PVE::JSONSchema::register_format('pve-storage-format', \&verify_format);
sub verify_format {
    my ($fmt, $noerr) = @_;

    if ($fmt !~ m/(raw|qcow2|vmdk)/) {
	return undef if $noerr;
	die "invalid format '$fmt'\n";
    }

    return $fmt;
}

PVE::JSONSchema::register_format('pve-storage-options', \&verify_options);
sub verify_options {
    my ($value, $noerr) = @_;

    # mount options (see man fstab)
    if ($value !~ m/^\S+$/) {
	return undef if $noerr;
	die "invalid options '$value'\n";
    }

    return $value;
}


sub private {
    return $defaultData;
}

sub parse_section_header {
    my ($class, $line) = @_;

    if ($line =~ m/^(\S+):\s*(\S+)\s*$/) {
	my ($type, $storeid) = (lc($1), $2);
	my $errmsg = undef; # set if you want to skip whole section
	eval { PVE::JSONSchema::parse_storage_id($storeid); };
	$errmsg = $@ if $@;
	my $config = {}; # to return additional attributes
	return ($type, $storeid, $errmsg, $config);
    }
    return undef;
}

sub decode_value {
    my ($class, $type, $key, $value) = @_;

    my $def = $defaultData->{plugindata}->{$type};

    if ($key eq 'content') {
	my $valid_content = $def->{content}->[0];
	
	my $res = {};

	foreach my $c (PVE::Tools::split_list($value)) {
	    if (!$valid_content->{$c}) {
		die "storage does not support content type '$c'\n";
	    }
	    $res->{$c} = 1;
	} 

	if ($res->{none} && scalar (keys %$res) > 1) {
	    die "unable to combine 'none' with other content types\n";
	}

	return $res;
    } elsif ($key eq 'format') {
	my $valid_formats = $def->{format}->[0];

	if (!$valid_formats->{$value}) {
	    die "storage does not support format '$value'\n";
	}

	return $value;
    } elsif ($key eq 'nodes') {
	my $res = {};

	foreach my $node (PVE::Tools::split_list($value)) {
	    if (PVE::JSONSchema::pve_verify_node_name($node)) {
		$res->{$node} = 1;
	    }
	}

	# fixme:
	# no node restrictions for local storage
	#if ($storeid && $storeid eq 'local' && scalar(keys(%$res))) {
	#    die "storage '$storeid' does not allow node restrictions\n";
	#}

	return $res;
    }

    return $value;
}

sub encode_value {
    my ($class, $type, $key, $value) = @_;

    if ($key eq 'nodes') {
        return join(',', keys(%$value));
    } elsif ($key eq 'content') {
	my $res = content_hash_to_string($value) || 'none';
	return $res;
    }

    return $value;
}

sub parse_config {
    my ($class, $filename, $raw) = @_;

    my $cfg = $class->SUPER::parse_config($filename, $raw);
    my $ids = $cfg->{ids};

    # make sure we have a reasonable 'local:' storage
    # openvz expects things to be there
    if (!$ids->{local} || $ids->{local}->{type} ne 'dir' ||
	($ids->{local}->{path} && $ids->{local}->{path} ne '/var/lib/vz')) {
	$ids->{local} = {
	    type => 'dir',
	    priority => 0, # force first entry
	    path => '/var/lib/vz',
	    maxfiles => 0,
	    content => { images => 1, rootdir => 1, vztmpl => 1, iso => 1},
	};
    }
    
    # we always need this for OpenVZ
    $ids->{local}->{content}->{rootdir} = 1;
    $ids->{local}->{content}->{vztmpl} = 1;
    delete ($ids->{local}->{disable});

    # make sure we have a path
    $ids->{local}->{path} = '/var/lib/vz' if !$ids->{local}->{path};

    # remove node restrictions for local storage
    delete($ids->{local}->{nodes});

    foreach my $storeid (keys %$ids) {
	my $d = $ids->{$storeid};
	my $type = $d->{type};

	my $def = $defaultData->{plugindata}->{$type};

	if ($def->{content}) {
	    $d->{content} = $def->{content}->[1] if !$d->{content};
	}

	if ($type eq 'iscsi' || $type eq 'nfs' || $type eq 'rbd' || $type eq 'sheepdog' || $type eq 'iscsidirect' || $type eq 'nexenta' ) {
	    $d->{shared} = 1;
	}
    }

    return $cfg;
}

# Storage implementation

sub cluster_lock_storage {
    my ($class, $storeid, $shared, $timeout, $func, @param) = @_;

    my $res;
    if (!$shared) {
	my $lockid = "pve-storage-$storeid";
	my $lockdir = "/var/lock/pve-manager";
	mkdir $lockdir;
	$res = PVE::Tools::lock_file("$lockdir/$lockid", $timeout, $func, @param); 
	die $@ if $@;
    } else {
	$res = PVE::Cluster::cfs_lock_storage($storeid, $timeout, $func, @param);
	die $@ if $@;
    }	
    return $res;
}

sub parse_name_dir {
    my $name = shift;

    if ($name =~ m!^([^/\s]+\.(raw|qcow2|vmdk))$!) {
	return ($1, $2);
    }

    die "unable to parse volume filename '$name'\n";
}

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^(\d+)/(\S+)$!) {
	my ($vmid, $name) = ($1, $2);
	parse_name_dir($name);
	return ('images', $name, $vmid);
    } elsif ($volname =~ m!^iso/([^/]+\.[Ii][Ss][Oo])$!) {
	return ('iso', $1);
    } elsif ($volname =~ m!^vztmpl/([^/]+\.tar\.gz)$!) {
	return ('vztmpl', $1);
    } elsif ($volname =~ m!^rootdir/(\d+)$!) {
	return ('rootdir', $1, $1);
    } elsif ($volname =~ m!^backup/([^/]+(\.(tar|tar\.gz|tar\.lzo|tgz)))$!) {
	my $fn = $1;
	if ($fn =~ m/^vzdump-(openvz|qemu)-(\d+)-.+/) {
	    return ('backup', $fn, $2);
	}
	return ('backup', $fn);
    }

    die "unable to parse directory volume name '$volname'\n";
}

my $vtype_subdirs = { 
    images => 'images',
    rootdir => 'private',
    iso => 'template/iso',
    vztmpl => 'template/cache',
    backup => 'dump',
};

sub get_subdir {
    my ($class, $scfg, $vtype) = @_;

    my $path = $scfg->{path};

    die "storage definintion has no path\n" if !$path;

    my $subdir = $vtype_subdirs->{$vtype};

    die "unknown vtype '$vtype'\n" if !defined($subdir);

    return "$path/$subdir"; 
}

sub path {
    my ($class, $scfg, $volname, $storeid) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $dir = $class->get_subdir($scfg, $vtype);

    $dir .= "/$vmid" if $vtype eq 'images';

    my $path = "$dir/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";

    mkpath $imagedir;

    if (!$name) {
	for (my $i = 1; $i < 100; $i++) {
	    my @gr = <$imagedir/vm-$vmid-disk-$i.*>;
	    if (!scalar(@gr)) {
		$name = "vm-$vmid-disk-$i.$fmt";
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    my (undef, $tmpfmt) = parse_name_dir($name);

    die "illegal name '$name' - wrong extension for format ('$tmpfmt != '$fmt')\n" 
	if $tmpfmt ne $fmt;

    my $path = "$imagedir/$name";

    die "disk image '$path' already exists\n" if -e $path;

    run_command("/usr/bin/qemu-img create -f $fmt '$path' ${size}K", 
		errmsg => "unable to create image");

    return "$vmid/$name";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $path = $class->path($scfg, $volname);

    if (! -f $path) {
	warn "disk image '$path' does not exists\n";
    } else {
	unlink $path;
    }

    return undef;
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'info', $filename];

    my $format;
    my $size = 0;
    my $used = 0;

    eval {
	run_command($cmd, timeout => $timeout, outfunc => sub {
	    my $line = shift;

	    if ($line =~ m/^file format:\s+(\S+)\s*$/) {
		$format = $1;
	    } elsif ($line =~ m/^virtual size:\s\S+\s+\((\d+)\s+bytes\)$/) {
		$size = int($1);
	    } elsif ($line =~ m/^disk size:\s+(\d+(.\d+)?)([KMGT])\s*$/) {
		$used = $1;
		my $u = $3;

		$used *= 1024 if $u eq 'K';
		$used *= (1024*1024) if $u eq 'M';
		$used *= (1024*1024*1024) if $u eq 'G';
		$used *= (1024*1024*1024*1024) if $u eq 'T';

		$used = int($used);
	    }
	});
    };

    return wantarray ? ($size, $format, $used) : $size;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my $path = $class->path($scfg, $volname);
    return file_size_info($path, $timeout);

}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    die "can't resize this image format" if $volname !~ m/\.(raw|qcow2)$/;

    return 1 if $running;

    my $path = $class->path($scfg, $volname);

    my $cmd = ['/usr/bin/qemu-img', 'resize', $path , $size];

    run_command($cmd, timeout => 1);

    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "can't snapshot this image format" if $volname !~ m/\.(qcow2|qed)$/;

    return 1 if $running;

    my $path = $class->path($scfg, $volname);

    my $cmd = ['/usr/bin/qemu-img', 'snapshot','-c', $snap, $path];

    run_command($cmd, timeout => 1);

    return undef;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "can't rollback snapshot this image format" if $volname !~ m/\.(qcow2|qed)$/;

    my $path = $class->path($scfg, $volname);

    my $cmd = ['/usr/bin/qemu-img', 'snapshot','-a', $snap, $path];

    run_command($cmd, timeout => 1);

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $imagedir = $class->get_subdir($scfg, 'images');

    my ($defFmt, $vaidFmts) = default_format($scfg);
    my $fmts = join ('|', @$vaidFmts); 

    my $res = [];

    foreach my $fn (<$imagedir/[0-9][0-9]*/*>) {

	next if $fn !~ m!^(/.+/(\d+)/([^/]+\.($fmts)))$!;
	$fn = $1; # untaint

	my $owner = $2;
	my $name = $3;
	my $volid = "$storeid:$owner/$name";

	if ($vollist) {
	    my $found = grep { $_ eq $volid } @$vollist;
	    next if !$found;
	} else {
	    next if defined($vmid) && ($owner ne $vmid);
	}

	my ($size, $format, $used) = file_size_info($fn);

	if ($format && $size) {
	    push @$res, { 
		volid => $volid, format => $format,
		size => $size, vmid => $owner, used => $used };
	}

    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};

    die "storage definintion has no path\n" if !$path;
	    
    my $timeout = 2;
    my $res = PVE::Tools::df($path, $timeout);

    return undef if !$res || !$res->{total};

    return ($res->{total}, $res->{avail}, $res->{used}, 1);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};

    die "storage definintion has no path\n" if !$path;

    die "unable to activate storage '$storeid' - " .
	"directory '$path' does not exist\n" if ! -d $path;

    if (defined($scfg->{content})) {
	foreach my $vtype (keys %$vtype_subdirs) {
	    next if !defined($scfg->{content}->{$vtype});
	    my $subdir = $class->get_subdir($scfg, $vtype);
	    mkpath $subdir if $subdir ne $path;
	}
    }
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    # do nothing by default
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;

    my $path = $class->path($scfg, $volname);

    # check is volume exists
    if ($scfg->{path}) {
	die "volume '$storeid:$volname' does not exist\n" if ! -e $path;
    } else {
	die "volume '$storeid:$volname' does not exist\n" if ! -b $path;
    }
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $cache) = @_;

    # do nothing by default
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;
    # do nothing by default
    return 1;
}


1;
