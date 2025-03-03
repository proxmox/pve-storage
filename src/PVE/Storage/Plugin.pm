package PVE::Storage::Plugin;

use strict;
use warnings;

use Cwd qw(abs_path);
use Encode qw(decode);
use Fcntl ':mode';
use File::chdir;
use File::Path;
use File::Basename;
use File::stat qw();

use PVE::Tools qw(run_command);
use PVE::JSONSchema qw(get_standard_option register_standard_option);
use PVE::Cluster qw(cfs_register_file);

use PVE::Storage::Common;

use JSON;

use base qw(PVE::SectionConfig);

use constant KNOWN_COMPRESSION_FORMATS =>  ('gz', 'lzo', 'zst', 'bz2');
use constant COMPRESSOR_RE => join('|', KNOWN_COMPRESSION_FORMATS);

use constant LOG_EXT => ".log";
use constant NOTES_EXT => ".notes";

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
    'iscsidirect',
    'glusterfs',
    'zfs',
    'drbd',
    'pbs',
);

our $QCOW2_PREALLOCATION = {
    off => 1,
    metadata => 1,
    falloc => 1,
    full => 1,
};

our $RAW_PREALLOCATION = {
    off => 1,
    falloc => 1,
    full => 1,
};

our $MAX_VOLUMES_PER_GUEST = 1024;

cfs_register_file ('storage.cfg',
		   sub { __PACKAGE__->parse_config(@_); },
		   sub { __PACKAGE__->write_config(@_); });

my %prune_option = (
    optional => 1,
    type => 'integer', minimum => '0',
    format_description => 'N',
);

our $prune_backups_format = {
    'keep-all' => {
	type => 'boolean',
	description => 'Keep all backups. Conflicts with the other options when true.',
	optional => 1,
    },
    'keep-last' => {
	%prune_option,
	description => 'Keep the last <N> backups.',
    },
    'keep-hourly' => {
	%prune_option,
	description => 'Keep backups for the last <N> different hours. If there is more' .
		       'than one backup for a single hour, only the latest one is kept.'
    },
    'keep-daily' => {
	%prune_option,
	description => 'Keep backups for the last <N> different days. If there is more' .
		       'than one backup for a single day, only the latest one is kept.'
    },
    'keep-weekly' => {
	%prune_option,
	description => 'Keep backups for the last <N> different weeks. If there is more' .
		       'than one backup for a single week, only the latest one is kept.'
    },
    'keep-monthly' => {
	%prune_option,
	description => 'Keep backups for the last <N> different months. If there is more' .
		       'than one backup for a single month, only the latest one is kept.'
    },
    'keep-yearly' => {
	%prune_option,
	description => 'Keep backups for the last <N> different years. If there is more' .
		       'than one backup for a single year, only the latest one is kept.'
    },
};
PVE::JSONSchema::register_format('prune-backups', $prune_backups_format, \&validate_prune_backups);
sub validate_prune_backups {
    my ($prune_backups) = @_;

    my $res = { $prune_backups->%* };

    my $keep_all = delete $res->{'keep-all'};

    if (scalar(grep { $_ > 0 } values %{$res}) == 0) {
	$res = { 'keep-all' => 1 };
    } elsif ($keep_all) {
	die "keep-all cannot be set together with other options.\n";
    }

    return $res;
}
register_standard_option('prune-backups', {
    description => "The retention options with shorter intervals are processed first " .
		   "with --keep-last being the very first one. Each option covers a " .
		   "specific period of time. We say that backups within this period " .
		   "are covered by this option. The next option does not take care " .
		   "of already covered backups and only considers older backups.",
    optional => 1,
    type => 'string',
    format => 'prune-backups',
});

my $defaultData = {
    propertyList => {
	type => { description => "Storage type." },
	storage => get_standard_option('pve-storage-id',
	    { completion => \&PVE::Storage::complete_storage }),
	nodes => get_standard_option('pve-node-list', {
	    description => "List of nodes for which the storage configuration applies.",
	    optional => 1,
	}),
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
	    description => "Deprecated: use 'prune-backups' instead. " .
		"Maximal number of backup files per VM. Use '0' for unlimited.",
	    type => 'integer',
	    minimum => 0,
	    optional => 1,
	},
	'prune-backups' => get_standard_option('prune-backups'),
	'max-protected-backups' => {
	    description => "Maximal number of protected backups per guest. Use '-1' for unlimited.",
	    type => 'integer',
	    minimum => -1,
	    optional => 1,
	    default => "Unlimited for users with Datastore.Allocate privilege, 5 for other users",
	},
	shared => {
	    description => "Indicate that this is a single storage with the same contents on all "
		."nodes (or all listed in the 'nodes' option). It will not make the contents of a "
		."local storage automatically accessible to other nodes, it just marks an already "
		."shared storage as such!",
	    type => 'boolean',
	    optional => 1,
	},
	subdir => {
	    description => "Subdir to mount.",
	    type => 'string', format => 'pve-storage-path',
	    optional => 1,
	},
	format => get_standard_option('pve-storage-image-format', {
	    description => "Default image format.",
	    optional => 1,
	}),
	preallocation => {
	    description => "Preallocation mode for raw and qcow2 images. " .
		"Using 'metadata' on raw images results in preallocation=off.",
	    type => 'string', enum => ['off', 'metadata', 'falloc', 'full'],
	    default => 'metadata',
	    optional => 1,
	},
	'content-dirs' => {
	    description => "Overrides for default content type directories.",
	    type => "string", format => "pve-dir-override-list",
	    optional => 1,
	},
	options => {
	    description => "NFS/CIFS mount options (see 'man nfs' or 'man mount.cifs')",
	    type => 'string',
	    format => 'pve-storage-options',
	    optional => 1,
	},
	port => {
	    description => "Use this port to connect to the storage instead of the default one (for"
		." example, with PBS or ESXi). For NFS and CIFS, use the 'options' option to"
		." configure the port via the mount options.",
	    type => 'integer',
	    minimum => 1,
	    maximum => 65535,
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

sub dirs_hash_to_string {
    my $hash = shift;

    return join(',', map { "$_=$hash->{$_}" } sort keys %$hash);
}

sub sensitive_properties {
    my ($type) = @_;

    my $data = $defaultData->{plugindata}->{$type};
    if (my $sensitive_properties = $data->{'sensitive-properties'}) {
	return [sort keys $sensitive_properties->%*];
    }

    # For backwards compatibility. This list was hardcoded in the API module previously.
    return [qw(encryption-key keyring master-pubkey password)];
}

sub storage_has_feature {
    my ($type, $feature) = @_;

    my $data = $defaultData->{plugindata}->{$type};
    if (my $features = $data->{features}) {
	return $features->{$feature};
    }
    return;
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

    if ($name !~ m/^[a-z0-9][a-z0-9\-\_\.]*[a-z0-9]$/i) {
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

    return $ct if $ct eq 'import';

    my $valid_content = valid_content_types('dir'); # dir includes all other types

    if (!$valid_content->{$ct}) {
	return undef if $noerr;
	die "invalid content type '$ct'\n";
    }

    return $ct;
}

# NOTE the 'pve-storage-format' is deprecated, use the 'pve-storage-image-format' standard option
# from Storage/Common.pm instead
# TODO PVE 9 - remove after doing a versioned breaks for pve-guest-common, which was using this
# format.
PVE::JSONSchema::register_format('pve-storage-format', \&verify_format);
sub verify_format {
    my ($fmt, $noerr) = @_;

    if ($fmt !~ m/^(raw|qcow2|vmdk|subvol)$/) {
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

PVE::JSONSchema::register_format('pve-dir-override', \&verify_dir_override);
sub verify_dir_override {
    my ($value, $noerr) = @_;

    if ($value =~ m/^([a-z]+)=([^.]*(?:\.?[^.]+)+)$/) {
	my ($content_type, $relative_path) = ($1, $2);
	if (verify_content($content_type, $noerr)) {
	    # linux has 4k max-path, but limit total length to lower as its concat'd for full path
	    if (length($relative_path) < 1023 && !(grep { length($_) >= 255 } split('/', $relative_path))) {
		return $value;
	    }
	}
    }

    return undef if $noerr;
    die "invalid override '$value'\n";
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

	if (scalar(keys $res->%*) == 0 && !$valid_content->{none}) {
	    die "storage does not support content type 'none'\n";
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
    } elsif ($key eq 'content-dirs') {
	my $valid_content = $def->{content}->[0];
	my $res = {};

	foreach my $dir (PVE::Tools::split_list($value)) {
	    my ($content, $path) = split(/=/, $dir, 2);

	    if (!$valid_content->{$content}) {
		warn "storage does not support content type '$content'\n";
		next;
	    }

	    $res->{$content} = $path;
	}

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
    } elsif ($key eq 'content-dirs') {
	my $res = dirs_hash_to_string($value);
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
	    'prune-backups' => 'keep-all=1',
	    content => {
		backup => 1,
		images => 1,
		iso => 1,
		rootdir => 1,
		snippets => 1,
		vztmpl => 1,
	    },
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

sub preallocation_cmd_option {
    my ($scfg, $fmt) = @_;

    my $prealloc = $scfg->{preallocation};

    if ($fmt eq 'qcow2') {
	$prealloc = $prealloc // 'metadata';

	die "preallocation mode '$prealloc' not supported by format '$fmt'\n"
	    if !$QCOW2_PREALLOCATION->{$prealloc};

	return "preallocation=$prealloc";
    } elsif ($fmt eq 'raw') {
	$prealloc = $prealloc // 'off';
	$prealloc = 'off' if $prealloc eq 'metadata';

	die "preallocation mode '$prealloc' not supported by format '$fmt'\n"
	    if !$RAW_PREALLOCATION->{$prealloc};

	return "preallocation=$prealloc";
    }

    return;
}

# Storage implementation

# called during addition of storage (before the new storage config got written)
# die to abort addition if there are (grave) problems
# NOTE: runs in a storage config *locked* context
sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    # do nothing by default
    return undef;
}

# called during storage configuration update (before the updated storage config got written)
# die to abort the update if there are (grave) problems
# NOTE: runs in a storage config *locked* context
sub on_update_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    # do nothing by default
    return undef;
}

# called during deletion of storage (before the new storage config got written)
# and if the activate check on addition fails, to cleanup all storage traces
# which on_add_hook may have created.
# die to abort deletion if there are (very grave) problems
# NOTE: runs in a storage config *locked* context
sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    # do nothing by default
    return undef;
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
    } elsif ($volname =~ m!^iso/([^/]+$PVE::Storage::ISO_EXT_RE_0)$!) {
	return ('iso', $1, undef, undef, undef, undef, 'raw');
    } elsif ($volname =~ m!^vztmpl/([^/]+$PVE::Storage::VZTMPL_EXT_RE_1)$!) {
	return ('vztmpl', $1, undef, undef, undef, undef, 'raw');
    } elsif ($volname =~ m!^rootdir/(\d+)$!) {
	return ('rootdir', $1, $1);
    } elsif ($volname =~ m!^backup/([^/]+$PVE::Storage::BACKUP_EXT_RE_2)$!) {
	my $fn = $1;
	if ($fn =~ m/^vzdump-(openvz|lxc|qemu)-(\d+)-.+/) {
	    return ('backup', $fn, $2, undef, undef, undef, 'raw');
	}
	return ('backup', $fn, undef, undef, undef, undef, 'raw');
    } elsif ($volname =~ m!^snippets/([^/]+)$!) {
	return ('snippets', $1, undef, undef, undef, undef, 'raw');
    } elsif ($volname =~ m!^import/(${PVE::Storage::SAFE_CHAR_WITH_WHITESPACE_CLASS_RE}+\.ova\/${PVE::Storage::OVA_CONTENT_RE_1})$!) {
	my $packed_image = $1;
	my $format = $2;
	return ('import', $packed_image, undef, undef, undef, undef, "ova+$format");
    } elsif ($volname =~ m!^import/(${PVE::Storage::SAFE_CHAR_WITH_WHITESPACE_CLASS_RE}+$PVE::Storage::IMPORT_EXT_RE_1)$!) {
	return ('import', $1, undef, undef, undef, undef, $2);
    }

    die "unable to parse directory volume name '$volname'\n";
}

my $vtype_subdirs = {
    images => 'images',
    rootdir => 'private',
    iso => 'template/iso',
    vztmpl => 'template/cache',
    backup => 'dump',
    snippets => 'snippets',
    import => 'import',
};

sub get_vtype_subdirs {
    return $vtype_subdirs;
}

sub get_subdir {
    my ($class, $scfg, $vtype) = @_;

    my $path = $scfg->{path};

    die "storage definition has no path\n" if !$path;
    die "unknown vtype '$vtype'\n" if !exists($vtype_subdirs->{$vtype});

    my $subdir = $scfg->{"content-dirs"}->{$vtype} // $vtype_subdirs->{$vtype};

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

    my ($size, undef, undef, $parent) = file_size_info($path, undef, $format);
    die "file_size_info on '$volname' failed\n" if !defined($size);

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

my $get_vm_disk_number = sub {
    my ($disk_name, $scfg, $vmid, $suffix) = @_;

    my $disk_regex = qr/(vm|base)-$vmid-disk-(\d+)$suffix/;

    my $type = $scfg->{type};
    my $def = { %{$defaultData->{plugindata}->{$type}} };

    my $valid = $def->{format}[0];
    if ($valid->{subvol}) {
	$disk_regex = qr/(vm|base|subvol|basevol)-$vmid-disk-(\d+)/;
    }

    if ($disk_name =~ m/$disk_regex/) {
	return $2;
    }

    return undef;
};

sub get_next_vm_diskname {
    my ($disk_list, $storeid, $vmid, $fmt, $scfg, $add_fmt_suffix) = @_;

    $fmt //= '';
    my $prefix = ($fmt eq 'subvol') ? 'subvol' : 'vm';
    my $suffix = $add_fmt_suffix ? ".$fmt" : '';

    my $disk_ids = {};
    foreach my $disk (@$disk_list) {
	my $disknum = $get_vm_disk_number->($disk, $scfg, $vmid, $suffix);
	$disk_ids->{$disknum} = 1 if defined($disknum);
    }

    for (my $i = 0; $i < $MAX_VOLUMES_PER_GUEST; $i++) {
	if (!$disk_ids->{$i}) {
	    return "$prefix-$vmid-disk-$i$suffix";
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
}

sub find_free_diskname {
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;

    my $disks = $class->list_images($storeid, $scfg, $vmid);

    my $disk_list = [ map { $_->{volid} } @$disks ];

    return get_next_vm_diskname($disk_list, $storeid, $vmid, $fmt, $scfg, $add_fmt_suffix);
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    # this only works for file based storage types
    die "storage definition has no path\n" if !$scfg->{path};

    my ($vtype, $basename, $basevmid, undef, undef, $isBase, $format) =
	$class->parse_volname($volname);

    die "clone_image on wrong vtype '$vtype'\n" if $vtype ne 'images';

    die "this storage type does not support clone_image on snapshot\n" if $snap;

    die "this storage type does not support clone_image on subvolumes\n" if $format eq 'subvol';

    die "clone_image only works on base images\n" if !$isBase;

    my $imagedir = $class->get_subdir($scfg, 'images');
    $imagedir .= "/$vmid";

    mkpath $imagedir;

    my $name = $class->find_free_diskname($storeid, $scfg, $vmid, "qcow2", 1);

    warn "clone $volname: $vtype, $name, $vmid to $name (base=../$basevmid/$basename)\n";

    my $newvol = "$basevmid/$basename/$vmid/$name";

    my $path = $class->filesystem_path($scfg, $newvol);

    # Note: we use relative paths, so we need to call chdir before qemu-img
    eval {
	local $CWD = $imagedir;

	my $cmd = ['/usr/bin/qemu-img', 'create', '-b', "../$basevmid/$basename",
		   '-F', $format, '-f', 'qcow2', $path];

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

    $name = $class->find_free_diskname($storeid, $scfg, $vmid, $fmt, 1) if !$name;

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

	my $prealloc_opt = preallocation_cmd_option($scfg, $fmt);
	push @$cmd, '-o', $prealloc_opt if defined($prealloc_opt);

	push @$cmd, '-f', $fmt, $path, "${size}K";

	eval { run_command($cmd, errmsg => "unable to create image"); };
	if ($@) {
	    unlink $path;
	    rmdir $imagedir;
	    die "$@";
	}
    }

    return "$vmid/$name";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    die "cannot remove protected volume '$volname' on '$storeid'\n"
	if $class->get_volume_attribute($scfg, $storeid, $volname, 'protected');

    my $path = $class->filesystem_path($scfg, $volname);

    if ($isBase) {
	# try to remove immutable flag
	eval { run_command(['/usr/bin/chattr', '-i', $path]); };
	warn $@ if $@;
    }

    if (defined($format) && ($format eq 'subvol')) {
	File::Path::remove_tree($path);
    } else {
	if (!(-f $path || -l $path)) {
	    warn "disk image '$path' does not exist\n";
	    return undef;
	}

	unlink($path) || die "unlink '$path' failed - $!\n";
    }

    # try to cleanup directory to not clutter storage with empty $vmid dirs if
    # all images from a guest got deleted
    my $dir = dirname($path);
    rmdir($dir);

    return undef;
}

# TODO taken from PVE/QemuServer/Drive.pm, avoiding duplication would be nice
my @checked_qemu_img_formats = qw(raw qcow qcow2 qed vmdk cloop);

# set $untrusted if the file in question might be malicious since it isn't
# created by our stack
# this makes certain checks fatal, and adds extra checks for known problems like
# - backing files (qcow2/vmdk)
# - external data files (qcow2)
#
# Set $file_format to force qemu-img to treat the image as being a specific format. Use the value
# 'auto-detect' for auto-detection. The parameter is planned to become mandatory with Proxmox VE 9.
sub file_size_info {
    my ($filename, $timeout, $file_format, $untrusted) = @_;

    # TODO PVE 9 make $file_format mandatory
    warn "file_size_info: detected call without \$file_format parameter\n"
	if !defined($file_format);

    # compat for old parameter order
    # TODO PVE 9 remove
    if (defined($file_format) && ($file_format eq '1' || $file_format eq '0')) {
	warn "file_size_info: detected call with legacy parameter order: \$untrusted before"
	    ." \$file_format\n";
	$untrusted = $file_format;
	$file_format = undef;
    }

    $file_format = undef if $file_format && $file_format eq 'auto-detect';

    my $st = File::stat::stat($filename);

    if (!defined($st)) {
	my $extramsg = -l $filename ? ' - dangling symlink?' : '';
	my $msg = "failed to stat '$filename'$extramsg\n";
	if ($untrusted) {
	    die $msg;
	} else {
	    warn $msg;
	    return undef;
	}
    }

    my $handle_error = sub {
	my ($msg) = @_;
	if ($untrusted) {
	    die $msg;
	} else {
	    warn $msg;
	    return wantarray ? (undef, undef, undef, undef, $st->ctime) : undef;
	}
    };

    if (S_ISDIR($st->mode)) {
	$handle_error->("expected format '$file_format', but '$filename' is a directory\n")
	    if $file_format && $file_format ne 'subvol';
	return wantarray ? (0, 'subvol', 0, undef, $st->ctime) : 0;
    } elsif ($file_format && $file_format eq 'subvol') {
	$handle_error->("expected format '$file_format', but '$filename' is not a directory\n");
    }

    # TODO PVE 9 - consider upgrading to "die" if an unsupported format is passed in after
    # evaluating breakage potential.
    if ($file_format && !grep { $_ eq $file_format } @checked_qemu_img_formats) {
	warn "file_size_info: '$filename': falling back to 'raw' from unknown format '$file_format'\n";
	$file_format = 'raw';
    }
    my $cmd = ['/usr/bin/qemu-img', 'info', '--output=json', $filename];
    push $cmd->@*, '-f', $file_format if $file_format;

    my $json = '';
    my $err_output = '';
    eval {
	run_command($cmd,
	    timeout => $timeout,
	    outfunc => sub { $json .= shift },
	    errfunc => sub { $err_output .= shift . "\n"},
	);
    };
    warn $@ if $@;
    if ($err_output) {
	# if qemu did not output anything to stdout we die with stderr as an error
	die $err_output if !$json;
	# otherwise we warn about it and try to parse the json
	warn $err_output;
    }
    if (!$json) {
	die "failed to query file information with qemu-img\n" if $untrusted;
	# skip decoding if there was no output, e.g. if there was a timeout.
	return wantarray ? (undef, undef, undef, undef, $st->ctime) : undef;
    }

    my $info = eval { decode_json($json) };
    $handle_error->("could not parse qemu-img info command output for '$filename' - $@\n") if $@;

    if ($untrusted) {
	if (my $format_specific = $info->{'format-specific'}) {
	    if ($format_specific->{type} eq 'qcow2' && $format_specific->{data}->{"data-file"}) {
		die "$filename: 'data-file' references are not allowed!\n";
	    } elsif ($format_specific->{type} eq 'vmdk') {
		my $extents = $format_specific->{data}->{extents};
		my $children = $info->{children};
		die "$filename: multiple children or extents are not allowed!\n"
		    if scalar($children->@*) > 1 || scalar($extents->@*) > 1;
	    }
	}
    }

    my ($size, $format, $used, $parent) = $info->@{qw(virtual-size format actual-size backing-filename)};

    die "backing file not allowed for untrusted image '$filename'!\n" if $untrusted && $parent;

    ($size) = ($size =~ /^(\d+)$/); # untaint
    die "size '$size' not an integer\n" if !defined($size);
    # coerce back from string
    $size = int($size);
    ($used) = ($used =~ /^(\d+)$/); # untaint
    die "used '$used' not an integer\n" if !defined($used);
    # coerce back from string
    $used = int($used);
    ($format) = ($format =~ /^(\S+)$/); # untaint
    die "format '$format' includes whitespace\n" if !defined($format);
    if (defined($parent)) {
	warn "strange parent name path '$parent' found\n" if $parent =~ m/[^\S]/;
	($parent) = ($parent =~ /^(\S+)$/); # untaint
    }

    die "qemu-img bug: queried format does not match format in result '$file_format ne $format'"
	if $file_format && $file_format ne $format;

    return wantarray ? ($size, $format, $used, $parent, $st->ctime) : $size;
}

# FIXME remove on the next APIAGE reset.
# Deprecated, use get_volume_attribute instead.
sub get_volume_notes {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    die "volume notes are not supported for $class";
}

# FIXME remove on the next APIAGE reset.
# Deprecated, use update_volume_attribute instead.
sub update_volume_notes {
    my ($class, $scfg, $storeid, $volname, $notes, $timeout) = @_;

    die "volume notes are not supported for $class";
}

# Returns undef if the attribute is not supported for the volume.
# Should die if there is an error fetching the attribute.
# Possible attributes:
# notes     - user-provided comments/notes.
# protected - not to be removed by free_image, and for backups, ignored when pruning.
sub get_volume_attribute {
    my ($class, $scfg, $storeid, $volname, $attribute) = @_;

    if ($attribute eq 'notes') {
	 my $notes = eval { $class->get_volume_notes($scfg, $storeid, $volname); };
	 if (my $err = $@) {
	     return if $err =~ m/^volume notes are not supported/;
	     die $err;
	 }
	 return $notes;
    }

    return;
}

# Dies if the attribute is not supported for the volume.
sub update_volume_attribute {
    my ($class, $scfg, $storeid, $volname, $attribute, $value) = @_;

    if ($attribute eq 'notes') {
	$class->update_volume_notes($scfg, $storeid, $volname, $value);
    }

    die "attribute '$attribute' is not supported for storage type '$scfg->{type}'\n";
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my $format = ($class->parse_volname($volname))[6];
    my $path = $class->filesystem_path($scfg, $volname);
    return file_size_info($path, $timeout, $format);

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

# Asserts that a rollback to $snap on $volname is possible.
# If certain snapshots are preventing the rollback and $blockers is an array
# reference, the snapshot names can be pushed onto $blockers prior to dying.
sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap, $blockers) = @_;

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

sub volume_snapshot_needs_fsfreeze {

    return 0;
}
sub storage_can_replicate {
    my ($class, $scfg, $storeid, $format) = @_;

    return 0;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running, $opts) = @_;

    my $features = {
	snapshot => {
	    current => { qcow2 => 1 },
	    snap => { qcow2 => 1 },
	},
	clone => {
	    base => { qcow2 => 1, raw => 1, vmdk => 1 },
	},
	template => {
	    current => { qcow2 => 1, raw => 1, vmdk => 1, subvol => 1 },
	},
	copy => {
	    base => { qcow2 => 1, raw => 1, vmdk => 1 },
	    current => { qcow2 => 1, raw => 1, vmdk => 1 },
	    snap => { qcow2 => 1 },
	},
	sparseinit => {
	    base => { qcow2 => 1, raw => 1, vmdk => 1 },
	    current => { qcow2 => 1, raw => 1, vmdk => 1 },
	},
	rename => {
	    current => {qcow2 => 1, raw => 1, vmdk => 1},
	},
    };

    if ($feature eq 'clone') {
	if (
	    defined($opts->{valid_target_formats})
	    && !(grep { $_ eq 'qcow2' } @{$opts->{valid_target_formats}})
	) {
	    return 0; # clone_image creates a qcow2 volume
	}
    } elsif ($feature eq 'rename') {
	return 0 if $class->can('api') && $class->api() < 10;
    }


    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) = $class->parse_volname($volname);

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
	my $format = $4;

	next if !$vollist && defined($vmid) && ($owner ne $vmid);

	my ($size, undef, $used, $parent, $ctime) = eval {
	    file_size_info($fn, undef, $format);
	};
	if (my $err = $@) {
	    die $err if $err !~ m/Image is not in \S+ format$/;
	    warn "image '$fn' is not in expected format '$format', querying as raw\n";
	    ($size, undef, $used, $parent, $ctime) = file_size_info($fn, undef, 'raw');
	    $format = 'invalid';
	}
	next if !defined($size);

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

	my $info = {
	    volid => $volid, format => $format,
	    size => $size, vmid => $owner, used => $used, parent => $parent
	};

	$info->{ctime} = $ctime if $ctime;

	push @$res, $info;
    }

    return $res;
}

# list templates ($tt = <iso|vztmpl|backup|snippets|import>)
my $get_subdir_files = sub {
    my ($sid, $path, $tt, $vmid) = @_;

    my $res = [];

    foreach my $fn (<$path/*>) {
	my $st = File::stat::stat($fn);

	next if (!$st || S_ISDIR($st->mode));

	my $info;

	if ($tt eq 'iso') {
	    next if $fn !~ m!/([^/]+$PVE::Storage::ISO_EXT_RE_0)$!i;

	    $info = { volid => "$sid:iso/$1", format => 'iso' };

	} elsif ($tt eq 'vztmpl') {
	    next if $fn !~ m!/([^/]+$PVE::Storage::VZTMPL_EXT_RE_1)$!;

	    $info = { volid => "$sid:vztmpl/$1", format => "t$2" };

	} elsif ($tt eq 'backup') {
	    next if $fn !~ m!/([^/]+$PVE::Storage::BACKUP_EXT_RE_2)$!;
	    my $original = $fn;
	    my $format = $2;
	    $fn = $1;

	    # only match for VMID now, to avoid false positives (VMID in parent directory name)
	    next if defined($vmid) && $fn !~ m/\S+-$vmid-\S+/;

	    $info = { volid => "$sid:backup/$fn", format => $format };

	    my $archive_info = eval { PVE::Storage::archive_info($fn) } // {};

	    $info->{ctime} = $archive_info->{ctime} if defined($archive_info->{ctime});
	    $info->{subtype} = $archive_info->{type} // 'unknown';

	    if (defined($vmid) || $fn =~ m!\-([1-9][0-9]{2,8})\-[^/]+\.${format}$!) {
		$info->{vmid} = $vmid // $1;
	    }

	    my $notes_fn = $original.NOTES_EXT;
	    if (-f $notes_fn) {
		my $notes = PVE::Tools::file_read_firstline($notes_fn);
		$info->{notes} = eval { decode('UTF-8', $notes, 1) } // $notes if defined($notes);
	    }

	    $info->{protected} = 1 if -e PVE::Storage::protection_file_path($original);
	} elsif ($tt eq 'snippets') {

	    $info = {
		volid => "$sid:snippets/". basename($fn),
		format => 'snippet',
	    };
	} elsif ($tt eq 'import') {
	    next if $fn !~ m!/(${PVE::Storage::SAFE_CHAR_CLASS_RE}+$PVE::Storage::IMPORT_EXT_RE_1)$!i;

	    $info = { volid => "$sid:import/$1", format => "$2" };
	}

	$info->{size} = $st->size;
	$info->{ctime} //= $st->ctime;

	push @$res, $info;
    }

    return $res;
};

# If attributes are set on a volume, they should be included in the result.
# See get_volume_attribute for a list of possible attributes.
sub list_volumes {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    my $res = [];
    my $vmlist = PVE::Cluster::get_vmlist();
    foreach my $type (@$content_types) {
	my $data;

	if ($type eq 'images' || $type eq 'rootdir') {
	    $data = $class->list_images($storeid, $scfg, $vmid);
	} elsif ($scfg->{path}) {
	    my $path = $class->get_subdir($scfg, $type);

	    if ($type eq 'iso' && !defined($vmid)) {
		$data = $get_subdir_files->($storeid, $path, 'iso');
	    } elsif ($type eq 'vztmpl'&& !defined($vmid)) {
		$data = $get_subdir_files->($storeid, $path, 'vztmpl');
	    } elsif ($type eq 'backup') {
		$data = $get_subdir_files->($storeid, $path, 'backup', $vmid);
	    } elsif ($type eq 'snippets') {
		$data = $get_subdir_files->($storeid, $path, 'snippets');
	    } elsif ($type eq 'import') {
		$data = $get_subdir_files->($storeid, $path, 'import');
	    }
	}

	next if !$data;

	foreach my $item (@$data) {
	    if ($type eq 'images' || $type eq 'rootdir') {
		my $vminfo = $vmlist->{ids}->{$item->{vmid}};
		my $vmtype;
		if (defined($vminfo)) {
		    $vmtype = $vminfo->{type};
		}
		if (defined($vmtype) && $vmtype eq 'lxc') {
		    $item->{content} = 'rootdir';
		} else {
		    $item->{content} = 'images';
		}
		next if $type ne $item->{content};
	    } else {
		$item->{content} = $type;
	    }

	    push @$res, $item;
	}
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};

    die "storage definition has no path\n" if !$path;

    my $timeout = 2;
    my $res = PVE::Tools::df($path, $timeout);

    return undef if !$res || !$res->{total};

    return ($res->{total}, $res->{avail}, $res->{used}, 1);
}

# Returns a hash with the snapshot names as keys and the following data:
# id        - Unique id to distinguish different snapshots even if the have the same name.
# timestamp - Creation time of the snapshot (seconds since epoch).
# Returns an empty hash if the volume does not exist.
sub volume_snapshot_info {
    my ($class, $scfg, $storeid, $volname) = @_;

    die "volume_snapshot_info is not implemented for $class";
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $path = $scfg->{path};

    die "storage definition has no path\n" if !$path;

    # this path test may hang indefinitely on unresponsive mounts
    my $timeout = 2;
    if (! PVE::Tools::run_fork_with_timeout($timeout, sub {-d $path})) {
	die "unable to activate storage '$storeid' - " .
	"directory '$path' does not exist or is unreachable\n";
    }

    # TODO: mkdir is basically deprecated since 8.0, but we don't warn here until 8.4 or 9.0, as we
    # only got the replacement in 8.0, so no real replacement window, and its really noisy.

    if (defined($scfg->{content})) {
	# (opt-out) create content dirs and check validity
	if (
	    (!defined($scfg->{'create-subdirs'}) || $scfg->{'create-subdirs'})
	    # FIXME The mkdir option is deprecated. Remove with PVE 9?
	    && (!defined($scfg->{mkdir}) || $scfg->{mkdir})
	) {
	    for my $vtype (sort keys %$vtype_subdirs) {
		# OpenVZMigrate uses backup (dump) dir
		if (
		    defined($scfg->{content}->{$vtype})
		    || ($vtype eq 'backup' && defined($scfg->{content}->{'rootdir'}))
		) {
		    my $subdir = $class->get_subdir($scfg, $vtype);
		    mkpath $subdir if $subdir ne $path;
		}
	    }
	}

	# check that content dirs are pairwise inequal
	my $resolved_subdirs = {};
	for my $vtype (sort keys $scfg->{content}->%*) {
	    my $subdir = $class->get_subdir($scfg, $vtype);
	    my $abs_subdir = abs_path($subdir);
	    next if !defined($abs_subdir);

	    die "storage '$storeid' uses directory $abs_subdir for multiple content types\n"
		if defined($abs_subdir) && defined($resolved_subdirs->{$abs_subdir});

	    $resolved_subdirs->{$abs_subdir} = 1;
	}
    }
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    # do nothing by default
}

sub map_volume {
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;

    my ($path) = $class->path($scfg, $volname, $storeid, $snapname);
    return $path;
}

sub unmap_volume {
    my ($class, $storeid, $scfg, $volname, $snapname) = @_;

    return 1;
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

sub prune_backups {
    my ($class, $scfg, $storeid, $keep, $vmid, $type, $dryrun, $logfunc) = @_;

    $logfunc //= sub { print "$_[1]\n" };

    my $backups = $class->list_volumes($storeid, $scfg, $vmid, ['backup']);

    my $backup_groups = {};
    my $prune_list = [];

    foreach my $backup (@{$backups}) {
	my $volid = $backup->{volid};
	my $archive_info = eval { PVE::Storage::archive_info($volid) } // {};
	my $backup_type = $archive_info->{type} // 'unknown';
	my $backup_vmid = $archive_info->{vmid} // $backup->{vmid};

	next if defined($type) && $type ne $backup_type;

	my $prune_entry = {
	    ctime => $backup->{ctime},
	    type => $backup_type,
	    volid => $volid,
	};

	$prune_entry->{vmid} = $backup_vmid if defined($backup_vmid);

	if ($archive_info->{is_std_name}) {
	    die "internal error - got no VMID\n" if !defined($backup_vmid);
	    die "internal error - got wrong VMID '$backup_vmid' != '$vmid'\n"
		if defined($vmid) && $backup_vmid ne $vmid;

	    $prune_entry->{ctime} = $archive_info->{ctime};
	    my $group = "$backup_type/$backup_vmid";
	    push @{$backup_groups->{$group}}, $prune_entry;
	} else {
	    # ignore backups that don't use the standard naming scheme
	    $prune_entry->{mark} = 'renamed';
	}

	$prune_entry->{mark} = 'protected' if $backup->{protected};

	push @{$prune_list}, $prune_entry;
    }

    foreach my $backup_group (values %{$backup_groups}) {
	PVE::Storage::prune_mark_backup_group($backup_group, $keep);
    }

    my $failed;
    if (!$dryrun) {
	foreach my $prune_entry (@{$prune_list}) {
	    next if $prune_entry->{mark} ne 'remove';

	    my $volid = $prune_entry->{volid};
	    $logfunc->('info', "removing backup '$volid'");
	    eval {
		my (undef, $volname) = parse_volume_id($volid);
		my $archive_path = $class->filesystem_path($scfg, $volname);
		PVE::Storage::archive_remove($archive_path);
	    };
	    if (my $err = $@) {
		$logfunc->('err', "error when removing backup '$volid' - $err\n");
		$failed = 1;
	    }
	}
    }
    die "error pruning backups - check log\n" if $failed;

    return $prune_list;
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
    die "import: no size found in export header, aborting.\n" if !defined($size);
    # Size is in bytes!
    return $size;
}

# Export a volume into a file handle as a stream of desired format.
sub volume_export {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots) = @_;

    my $err_msg = "volume export format $format not available for $class\n";
    if ($scfg->{path} && !defined($snapshot) && !defined($base_snapshot)) {
	my ($file) = $class->path($scfg, $volname, $storeid) or die $err_msg;
	my $file_format = ($class->parse_volname($volname))[6];
	my $size = file_size_info($file, undef, $file_format);

	if ($format eq 'raw+size') {
	    die $err_msg if $with_snapshots || $file_format eq 'subvol';
	    write_common_header($fh, $size);
	    if ($file_format eq 'raw') {
		run_command(['dd', "if=$file", "bs=4k", "status=progress"], output => '>&'.fileno($fh));
	    } else {
		run_command(['qemu-img', 'convert', '-f', $file_format, '-O', 'raw', $file, '/dev/stdout'],
		            output => '>&'.fileno($fh));
	    }
	    return;
	} elsif ($format =~ /^(qcow2|vmdk)\+size$/) {
	    my $data_format = $1;
	    die $err_msg if !$with_snapshots || $file_format ne $data_format;
	    write_common_header($fh, $size);
	    run_command(['dd', "if=$file", "bs=4k", "status=progress"], output => '>&'.fileno($fh));
	    return;
	} elsif ($format eq 'tar+size') {
	    die $err_msg if $file_format ne 'subvol';
	    write_common_header($fh, $size);
	    run_command(['tar', @COMMON_TAR_FLAGS, '-cf', '-', '-C', $file, '.'],
	                output => '>&'.fileno($fh));
	    return;
	}
    }
    die $err_msg;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    if ($scfg->{path} && !defined($snapshot) && !defined($base_snapshot)) {
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

# Import data from a stream, creating a new or replacing or adding to an existing volume.
sub volume_import {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots, $allow_rename) = @_;

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
    my ($file) = $class->path($scfg, $volname, $storeid);
    if (-e $file) {
	die "file '$file' already exists\n" if !$allow_rename;
	warn "file '$file' already exists - importing with a different name\n";
	$name = undef;
    }

    my ($size) = read_common_header($fh);
    $size = PVE::Storage::Common::align_size_up($size, 1024) / 1024;

    eval {
	my $allocname = $class->alloc_image($storeid, $scfg, $vmid, $file_format, $name, $size);
	my $oldname = $volname;
	$volname = $allocname;
	if (defined($name) && $allocname ne $oldname) {
	    die "internal error: unexpected allocated name: '$allocname' != '$oldname'\n";
	}
	my ($file) = $class->path($scfg, $volname, $storeid)
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

    return "$storeid:$volname";
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
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

sub rename_volume {
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;
    die "not implemented in storage plugin '$class'\n" if $class->can('api') && $class->api() < 10;
    die "no path found\n" if !$scfg->{path};

    my (
	undef,
	$source_image,
	$source_vmid,
	$base_name,
	$base_vmid,
	undef,
	$format
    ) = $class->parse_volname($source_volname);

    $target_volname = $class->find_free_diskname($storeid, $scfg, $target_vmid, $format, 1)
	if !$target_volname;

    my $basedir = $class->get_subdir($scfg, 'images');

    mkpath "${basedir}/${target_vmid}";

    my $old_path = "${basedir}/${source_vmid}/${source_image}";
    my $new_path = "${basedir}/${target_vmid}/${target_volname}";

    die "target volume '${target_volname}' already exists\n" if -e $new_path;

    my $base = $base_name ? "${base_vmid}/${base_name}/" : '';

    rename($old_path, $new_path) ||
	die "rename '$old_path' to '$new_path' failed - $!\n";

    return "${storeid}:${base}${target_vmid}/${target_volname}";
}

# Used by storage plugins for external backup providers. See PVE::BackupProvider::Plugin for the API
# the provider needs to implement.
#
# $scfg - the storage configuration
# $storeid - the storage ID
# $log_function($log_level, $message) - this log function can be used to write to the backup task
#   log in Proxmox VE. $log_level is 'info', 'warn' or 'err', $message is the message to be printed.
#
# Returns a blessed reference to the backup provider class.
sub new_backup_provider {
    my ($class, $scfg, $storeid, $log_function) = @_;

    die "implement me if enabling the feature 'backup-provider' in plugindata()->{features}\n";
}

sub config_aware_base_mkdir {
    my ($class, $scfg, $path) = @_;

    # FIXME the mkdir parameter is deprecated and create-base-path should be used
    if ($scfg->{'create-base-path'} // $scfg->{mkdir} // 1) {
	mkpath($path);
    }
}

1;
