package PVE::Storage::Plugin;

use strict;
use warnings;

use File::chdir;
use File::Path;

use PVE::Tools qw(run_command);
use PVE::JSONSchema qw(get_standard_option);
use PVE::Cluster qw(cfs_register_file);

use base qw(PVE::SectionConfig);

our @COMMON_TAR_FLAGS = qw(
    --one-file-system
    -p --sparse --numeric-owner --acls
    --xattrs --xattrs-include=user.* --xattrs-include=security.capability
    --warning=no-file-ignored --warning=no-xattr-write
);

our @SHARED_STORAGE = (
    'iscsi',
    'nfs',
    'cifs',
    'rbd',
    'cephfs',
    'sheepdog',
    'iscsidirect',
    'glusterfs',
    'zfs',
    'drbd');

cfs_register_file ('storage.cfg',
		   sub { __PACKAGE__->parse_config(@_); },
		   sub { __PACKAGE__->write_config(@_); });


my $defaultData = {
    propertyList => {
	type => { description => "Storage type." },
	storage => get_standard_option('pve-storage-id',
	    { completion => \&PVE::Storage::complete_storage }),
	nodes => get_standard_option('pve-node-list', { optional => 1 }),
	content => {
	    description => "Allowed content types.\n\nNOTE: the value " .
		"'rootdir' is used for Containers, and value 'images' for VMs.\n",
	    type => 'string', format => 'pve-storage-content-list',
	    optional => 1,
	    completion => \&PVE::Storage::complete_content_type,
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
	    description => "Default image format.",
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

    if (!(PVE::JSONSchema::pve_verify_ip($server, 1) ||
          PVE::JSONSchema::pve_verify_dns_name($server, 1)))
    {
	return undef if $noerr;
	die "value does not look like a valid server name or IP address\n";
    }
    return $server;
}

PVE::JSONSchema::register_format('pve-storage-vgname', \&parse_lvm_name);
sub parse_lvm_name {
    my ($name, $noerr) = @_;

    if ($name !~ m/^[a-z][a-z0-9\-\_\.]*[a-z0-9]$/i) {
	return undef if $noerr;
	die "lvm name '$name' contains illegal characters\n";
    }

    return $name;
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
    if (!PVE::Tools::parse_host_and_port($portal)) {
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

    if ($fmt !~ m/(raw|qcow2|vmdk|subvol)/) {
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

PVE::JSONSchema::register_format('pve-volume-id', \&parse_volume_id);
sub parse_volume_id {
    my ($volid, $noerr) = @_;

    if ($volid =~ m/^([a-z][a-z0-9\-\_\.]*[a-z0-9]):(.+)$/i) {
	return wantarray ? ($1, $2) : $1;
    }
    return undef if $noerr;
    die "unable to parse volume ID '$volid'\n";
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
		warn "storage does not support content type '$c'\n";
		next;
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
	    warn "storage does not support format '$value'\n";
	    next;
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
    # we want 'local' to be always the same 'type' (on all cluster nodes)
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
	if (grep { $_ eq $type }  @SHARED_STORAGE) {
	    $d->{shared} = 1;
	}
    }

    return $cfg;
}

# Storage implementation

# called during addition of storage (before the new storage config got written)
# die to abort additon if there are (grave) problems
# NOTE: runs in a storage config *locked* context
sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    # do nothing by default
}

# called during deletion of storage (before the new storage config got written)
# and if the activate check on addition fails, to cleanup all storage traces
# which on_add_hook may have created.
# die to abort deletion if there are (very grave) problems
# NOTE: runs in a storage config *locked* context
sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    # do nothing by default
}

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

    if ($name =~ m!^((base-)?[^/\s]+\.(raw|qcow2|vmdk|subvol))$!) {
	return ($1, $3, $2); # (name, format, isBase)
    }

    die "unable to parse volume filename '$name'\n";
}

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^(\d+)/(\S+)/(\d+)/(\S+)$!) {
	my ($basedvmid, $basename) = ($1, $2);
	parse_name_dir($basename);
	my ($vmid, $name) = ($3, $4);
	my (undef, $format, $isBase) = parse_name_dir($name);
	return ('images', $name, $vmid, $basename, $basedvmid, $isBase, $format);
    } elsif ($volname =~ m!^(\d+)/(\S+)$!) {
	my ($vmid, $name) = ($1, $2);
	my (undef, $format, $isBase) = parse_name_dir($name);
	return ('images', $name, $vmid, undef, undef, $isBase, $format);
    } elsif ($volname =~ m!^iso/([^/]+\.[Ii][Ss][Oo])$!) {
	return ('iso', $1);
    } elsif ($volname =~ m!^vztmpl/([^/]+\.tar\.[gx]z)$!) {
	return ('vztmpl', $1);
    } elsif ($volname =~ m!^rootdir/(\d+)$!) {
	return ('rootdir', $1, $1);
    } elsif ($volname =~ m!^backup/([^/]+(\.(tar|tar\.gz|tar\.lzo|tgz|vma|vma\.gz|vma\.lzo)))$!) {
	my $fn = $1;
	if ($fn =~ m/^vzdump-(openvz|lxc|qemu)-(\d+)-.+/) {
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

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    # Note: qcow2/qed has internal snapshot, so path is always
    # the same (with or without snapshot => same file).
    die "can't snapshot this image format\n"
	if defined($snapname) && $format !~ m/^(qcow2|qed)$/;

    my $dir = $class->get_subdir($scfg, $vtype);

    $dir .= "/$vmid" if $vtype eq 'images';

    my $path = "$dir/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    return $class->filesystem_path($scfg, $volname, $snapname);
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    # this only works for file based storage types
    die "storage definition has no path\n" if !$scfg->{path};

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
	$class->parse_volname($volname);

    die "create_base on wrong vtype '$vtype'\n" if $vtype ne 'images';

    die "create_base not possible with base image\n" if $isBase;

    my $path = $class->filesystem_path($scfg, $volname);

    my ($size, undef, $used, $parent) = file_size_info($path);
    die "file_size_info on '$volname' failed\n" if !($format && defined($size));

    die "volname '$volname' contains wrong information about parent\n"
	if $basename && (!$parent || $parent ne "../$basevmid/$basename");

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basevmid/$basename/$vmid/$newname" :
	"$vmid/$newname";

    my $newpath = $class->filesystem_path($scfg, $newvolname);

    die "file '$newpath' already exists\n" if -f $newpath;

    rename($path, $newpath) ||
	die "rename '$path' to '$newpath' failed - $!\n";

    # We try to protect base volume

    chmod(0444, $newpath); # nobody should write anything

    # also try to set immutable flag
    eval { run_command(['/usr/bin/chattr', '+i', $newpath]); };
    warn $@ if $@;

    return $newvolname;
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

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    # this only works for file based storage types
    die "storage definintion has no path\n" if !$scfg->{path};

    my ($vtype, $basename, $basevmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    die "clone_image on wrong vtype '$vtype'\n" if $vtype ne 'images';

    die "this storage type does not support clone_image on snapshot\n" if $snap;

    die "this storage type does not support clone_image on subvolumes\n" if $format eq 'subvol';

    die "clone_image only works on base images\n" if !$isBase;

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";

    mkpath $imagedir;

    my $name = &$find_free_diskname($imagedir, $vmid, "qcow2");

    warn "clone $volname: $vtype, $name, $vmid to $name (base=../$basevmid/$basename)\n";

    my $newvol = "$basevmid/$basename/$vmid/$name";

    my $path = $class->filesystem_path($scfg, $newvol);

    # Note: we use relative paths, so we need to call chdir before qemu-img
    eval {
	local $CWD = $imagedir;

	my $cmd = ['/usr/bin/qemu-img', 'create', '-b', "../$basevmid/$basename",
		   '-f', 'qcow2', $path];

	run_command($cmd);
    };
    my $err = $@;

    die $err if $err;

    return $newvol;
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

    if ($fmt eq 'subvol') {
	# only allow this if size = 0, so that user knows what he is doing
	die "storage does not support subvol quotas\n" if $size != 0;
	
	my $old_umask = umask(0022);
	my $err;
	mkdir($path) or $err = "unable to create subvol '$path' - $!\n";
	umask $old_umask;
	die $err if $err;
    } else {
	my $cmd = ['/usr/bin/qemu-img', 'create'];

	push @$cmd, '-o', 'preallocation=metadata' if $fmt eq 'qcow2';
	
	push @$cmd, '-f', $fmt, $path, "${size}K";

	run_command($cmd, errmsg => "unable to create image");
    }
    
    return "$vmid/$name";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    my $path = $class->filesystem_path($scfg, $volname);

    if ($isBase) {
	# try to remove immutable flag
	eval { run_command(['/usr/bin/chattr', '-i', $path]); };
	warn $@ if $@;
    }

    if (defined($format) && ($format eq 'subvol')) {
	File::Path::remove_tree($path);
    } else {
    
	if (! -f $path) {
	    warn "disk image '$path' does not exists\n";
	    return undef;
	}

	unlink($path) || die "unlink '$path' failed - $!\n";
    }
    
    return undef;
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    if (-d $filename) {
	return wantarray ? (0, 'subvol', 0, undef) : 1;
    }
    
    my $cmd = ['/usr/bin/qemu-img', 'info', $filename];

    my $format;
    my $parent;
    my $size = 0;
    my $used = 0;

    eval {
	run_command($cmd, timeout => $timeout, outfunc => sub {
	    my $line = shift;
	    if ($line =~ m/^file format:\s+(\S+)\s*$/) {
		$format = $1;
	    } elsif ($line =~ m/^backing file:\s(\S+)\s/) {
		$parent = $1;
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

    return wantarray ? ($size, $format, $used, $parent) : $size;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my $path = $class->filesystem_path($scfg, $volname);
    return file_size_info($path, $timeout);

}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    die "can't resize this image format\n" if $volname !~ m/\.(raw|qcow2)$/;

    return 1 if $running;

    my $path = $class->filesystem_path($scfg, $volname);

    my $format = ($class->parse_volname($volname))[6];

    my $cmd = ['/usr/bin/qemu-img', 'resize', '-f', $format, $path , $size];

    run_command($cmd, timeout => 10);

    return undef;
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "can't snapshot this image format\n" if $volname !~ m/\.(qcow2|qed)$/;

    my $path = $class->filesystem_path($scfg, $volname);

    my $cmd = ['/usr/bin/qemu-img', 'snapshot','-c', $snap, $path];

    run_command($cmd);

    return undef;
}

sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap) = @_; 

    return 1; 
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "can't rollback snapshot this image format\n" if $volname !~ m/\.(qcow2|qed)$/;

    my $path = $class->filesystem_path($scfg, $volname);

    my $cmd = ['/usr/bin/qemu-img', 'snapshot','-a', $snap, $path];

    run_command($cmd);

    return undef;
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "can't delete snapshot for this image format\n" if $volname !~ m/\.(qcow2|qed)$/;

    return 1 if $running;

    my $path = $class->filesystem_path($scfg, $volname);

    $class->deactivate_volume($storeid, $scfg, $volname, $snap, {});

    my $cmd = ['/usr/bin/qemu-img', 'snapshot','-d', $snap, $path];

    run_command($cmd);

    return undef;
}

sub storage_can_replicate {
    my ($class, $scfg, $storeid, $format) = @_;

    return 0;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => { qcow2 => 1}, snap => { qcow2 => 1} },
	clone => { base => {qcow2 => 1, raw => 1, vmdk => 1} },
	template => { current => {qcow2 => 1, raw => 1, vmdk => 1, subvol => 1} },
	copy => { base => {qcow2 => 1, raw => 1, vmdk => 1},
		  current => {qcow2 => 1, raw => 1, vmdk => 1},
		  snap => {qcow2 => 1} },
	sparseinit => { base => {qcow2 => 1, raw => 1, vmdk => 1},
			current => {qcow2 => 1, raw => 1, vmdk => 1} },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
        $key = 'snap';
    }else{
        $key =  $isBase ? 'base' : 'current';
    }

    return 1 if defined($features->{$feature}->{$key}->{$format});

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

	next if !$vollist && defined($vmid) && ($owner ne $vmid);

	my ($size, $format, $used, $parent) = file_size_info($fn);
	next if !($format && defined($size));

	my $volid;
	if ($parent && $parent =~ m!^../(\d+)/([^/]+\.($fmts))$!) {
	    my ($basevmid, $basename) = ($1, $2);
	    $volid = "$storeid:$basevmid/$basename/$owner/$name";
	} else {
	    $volid = "$storeid:$owner/$name";
	}

	if ($vollist) {
	    my $found = grep { $_ eq $volid } @$vollist;
	    next if !$found;
	}

	push @$res, {
	    volid => $volid, format => $format,
	    size => $size, vmid => $owner, used => $used, parent => $parent
	};
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

sub volume_snapshot_list {
    my ($class, $scfg, $storeid, $volname) = @_;

    # implement in subclass
    die "Volume_snapshot_list is not implemented for $class";

    # return an empty array if dataset does not exist.
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};

    die "storage definintion has no path\n" if !$path;

    # this path test may hang indefinitely on unresponsive mounts
    my $timeout = 2;
    if (! PVE::Tools::run_fork_with_timeout($timeout, sub {-d $path})) {
	die "unable to activate storage '$storeid' - " .
	"directory '$path' does not exist or is unreachable\n";
    }


    return if defined($scfg->{mkdir}) && !$scfg->{mkdir};

    if (defined($scfg->{content})) {
	foreach my $vtype (keys %$vtype_subdirs) {
	    # OpenVZMigrate uses backup (dump) dir
	    if (defined($scfg->{content}->{$vtype}) ||
		($vtype eq 'backup' && defined($scfg->{content}->{'rootdir'}))) {
		my $subdir = $class->get_subdir($scfg, $vtype);
		mkpath $subdir if $subdir ne $path;
	    }
	}
    }
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    # do nothing by default
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $path = $class->filesystem_path($scfg, $volname, $snapname);

    # check is volume exists
    if ($scfg->{path}) {
	die "volume '$storeid:$volname' does not exist\n" if ! -e $path;
    } else {
	die "volume '$storeid:$volname' does not exist\n" if ! -b $path;
    }
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    # do nothing by default
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;
    # do nothing by default
    return 1;
}

# Import/Export interface:
#   Any path based storage is assumed to support 'raw' and 'tar' streams, so
#   the default implementations will return this if $scfg->{path} is set,
#   mimicking the old PVE::Storage::storage_migrate() function.
#
# Plugins may fall back to PVE::Storage::Plugin::volume_{export,import}...
#   functions in case the format doesn't match their specialized
#   implementations to reuse the raw/tar code.
#
# Format specification:
#   The following formats are all prefixed with image information in the form
#   of a 64 bit little endian unsigned integer (pack('Q<')) in order to be able
#   to preallocate the image on storages which require it.
#
#   raw+size: (image files only)
#     A raw binary data stream such as produced via `dd if=TheImageFile`.
#   qcow2+size, vmdk: (image files only)
#     A raw qcow2/vmdk/... file such as produced via `dd if=some.qcow2` for
#     files which are already in qcow2 format, or via `qemu-img convert`.
#     Note that these formats are only valid with $with_snapshots being true.
#   tar+size: (subvolumes only)
#     A GNU tar stream containing just the inner contents of the subvolume.
#     This does not distinguish between the contents of a privileged or
#     unprivileged container. In other words, this is from the root user
#     namespace's point of view with no uid-mapping in effect.
#     As produced via `tar -C vm-100-disk-1.subvol -cpf TheOutputFile.dat .`

# Plugins may reuse these helpers. Changes to the header format should be
# reflected by changes to the function prototypes.
sub write_common_header($$) {
    my ($fh, $image_size_in_bytes) = @_;
    syswrite($fh, pack("Q<", $image_size_in_bytes), 8);
}

sub read_common_header($) {
    my ($fh) = @_;
    sysread($fh, my $size, 8);
    $size = unpack('Q<', $size);
    die "got a bad size (not a multiple of 1K)\n" if ($size&1023);
    # Size is in bytes!
    return $size;
}

# Export a volume into a file handle as a stream of desired format.
sub volume_export {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots) = @_;
    if ($scfg->{path} && !defined($snapshot) && !defined($base_snapshot)) {
	my $file = $class->path($scfg, $volname, $storeid)
	    or goto unsupported;
	my ($size, $file_format) = file_size_info($file);

	if ($format eq 'raw+size') {
	    goto unsupported if $with_snapshots || $file_format eq 'subvol';
	    write_common_header($fh, $size);
	    if ($file_format eq 'raw') {
		run_command(['dd', "if=$file", "bs=4k"], output => '>&'.fileno($fh));
	    } else {
		run_command(['qemu-img', 'convert', '-f', $file_format, '-O', 'raw', $file, '/dev/stdout'],
		            output => '>&'.fileno($fh));
	    }
	    return;
	} elsif ($format =~ /^(qcow2|vmdk)\+size$/) {
	    my $data_format = $1;
	    goto unsupported if !$with_snapshots || $file_format ne $data_format;
	    write_common_header($fh, $size);
	    run_command(['dd', "if=$file", "bs=4k"], output => '>&'.fileno($fh));
	    return;
	} elsif ($format eq 'tar+size') {
	    goto unsupported if $file_format ne 'subvol';
	    write_common_header($fh, $size);
	    run_command(['tar', @COMMON_TAR_FLAGS, '-cf', '-', '-C', $file, '.'],
	                output => '>&'.fileno($fh));
	    return;
	}
    }
 unsupported:
    die "volume export format $format not available for $class";
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    if ($scfg->{path} && !defined($snapshot) && !defined($base_snapshot)) {
	my $file = $class->path($scfg, $volname, $storeid)
	    or return;
	my ($size, $format) = file_size_info($file);

	if ($with_snapshots) {
	    return ($format.'+size') if ($format eq 'qcow2' || $format eq 'vmdk');
	    return ();
	}
	return ('tar+size') if $format eq 'subvol';
	return ('raw+size');
    }
    return ();
}

# Import data from a stream, creating a new or replacing or adding to an existing volume.
sub volume_import {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $base_snapshot, $with_snapshots) = @_;

    die "volume import format '$format' not available for $class\n"
	if $format !~ /^(raw|tar|qcow2|vmdk)\+size$/;
    my $data_format = $1;

    die "format $format cannot be imported without snapshots\n"
	if !$with_snapshots && ($data_format eq 'qcow2' || $data_format eq 'vmdk');
    die "format $format cannot be imported with snapshots\n"
	if $with_snapshots && ($data_format eq 'raw' || $data_format eq 'tar');

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $file_format) =
	$class->parse_volname($volname);

    # XXX: Should we bother with conversion routines at this level? This won't
    # happen without manual CLI usage, so for now we just error out...
    die "cannot import format $format into a file of format $file_format\n"
	if $data_format ne $file_format && !($data_format eq 'tar' && $file_format eq 'subvol');

    # Check for an existing file first since interrupting alloc_image doesn't
    # free it.
    my $file = $class->path($scfg, $volname, $storeid);
    die "file '$file' already exists\n" if -e $file;

    my ($size) = read_common_header($fh);
    $size = int($size/1024);

    eval {
	my $allocname = $class->alloc_image($storeid, $scfg, $vmid, $file_format, $name, $size);
	if ($allocname ne $volname) {
	    my $oldname = $volname;
	    $volname = $allocname; # Let the cleanup code know what to free
	    die "internal error: unexpected allocated name: '$allocname' != '$oldname'\n";
	}
	my $file = $class->path($scfg, $volname, $storeid)
	    or die "internal error: failed to get path to newly allocated volume $volname\n";
	if ($data_format eq 'raw' || $data_format eq 'qcow2' || $data_format eq 'vmdk') {
	    run_command(['dd', "of=$file", 'conv=sparse', 'bs=64k'],
	                input => '<&'.fileno($fh));
	} elsif ($data_format eq 'tar') {
	    run_command(['tar', @COMMON_TAR_FLAGS, '-C', $file, '-xf', '-'],
	                input => '<&'.fileno($fh));
	} else {
	    die "volume import format '$format' not available for $class";
	}
    };
    if (my $err = $@) {
	eval { $class->free_image($storeid, $scfg, $volname, 0, $file_format) };
	warn $@ if $@;
	die $err;
    }
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $base_snapshot, $with_snapshots) = @_;
    if ($scfg->{path} && !defined($base_snapshot)) {
	my $format = ($class->parse_volname($volname))[6];
	if ($with_snapshots) {
	    return ($format.'+size') if ($format eq 'qcow2' || $format eq 'vmdk');
	    return ();
	}
	return ('tar+size') if $format eq 'subvol';
	return ('raw+size');
    }
    return ();
}

1;
