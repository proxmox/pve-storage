package PVE::CLI::pvesm;

use strict;
use warnings;

use POSIX qw(O_RDONLY O_WRONLY O_CREAT O_TRUNC);
use Fcntl ':flock';
use File::Path;

use PVE::SafeSyslog;
use PVE::Cluster;
use PVE::INotify;
use PVE::RPCEnvironment;
use PVE::Storage;
use PVE::Tools qw(extract_param);
use PVE::API2::Storage::Config;
use PVE::API2::Storage::Content;
use PVE::API2::Storage::PruneBackups;
use PVE::API2::Storage::Status;
use PVE::JSONSchema qw(get_standard_option);
use PVE::PTY;

use PVE::CLIHandler;

use base qw(PVE::CLIHandler);

my $KNOWN_EXPORT_FORMATS = ['raw+size', 'tar+size', 'qcow2+size', 'vmdk+size', 'zfs'];

my $nodename = PVE::INotify::nodename();

sub param_mapping {
    my ($name) = @_;

    my $password_map = PVE::CLIHandler::get_standard_mapping('pve-password', {
	func => sub {
	    my ($value) = @_;
	    return $value if $value;
	    return PVE::PTY::read_password("Enter Password: ");
	},
    });

    my $enc_key_map = {
	name => 'encryption-key',
	desc => 'a file containing an encryption key, or the special value "autogen"',
	func => sub {
	    my ($value) = @_;
	    return $value if $value eq 'autogen';
	    return PVE::Tools::file_get_contents($value);
	}
    };


    my $mapping = {
	'cifsscan' => [ $password_map ],
	'create' => [ $password_map, $enc_key_map ],
	'update' => [ $password_map, $enc_key_map ],
    };
    return $mapping->{$name};
}

sub setup_environment {
    PVE::RPCEnvironment->setup_default_cli_env();
}

__PACKAGE__->register_method ({
    name => 'apiinfo',
    path => 'apiinfo',
    method => 'GET',
    description => "Returns APIVER and APIAGE.",
    parameters => {
	additionalProperties => 0,
	properties => {},
    },
    returns => {
	type => 'object',
	properties => {
	    apiver => { type => 'integer' },
	    apiage => { type => 'integer' },
	},
    },
    code => sub {
	return {
	    apiver => PVE::Storage::APIVER,
	    apiage => PVE::Storage::APIAGE,
	};
    }
});

__PACKAGE__->register_method ({
    name => 'path',
    path => 'path',
    method => 'GET',
    description => "Get filesystem path for specified volume",
    parameters => {
    	additionalProperties => 0,
	properties => {
	    volume => {
		description => "Volume identifier",
		type => 'string', format => 'pve-volume-id',
		completion => \&PVE::Storage::complete_volume,
	    },
	},
    },
    returns => { type => 'null' },

    code => sub {
	my ($param) = @_;

	my $cfg = PVE::Storage::config();

	my $path = PVE::Storage::path ($cfg, $param->{volume});

	print "$path\n";

	return undef;

    }});

__PACKAGE__->register_method ({
    name => 'extractconfig',
    path => 'extractconfig',
    method => 'GET',
    description => "Extract configuration from vzdump backup archive.",
    permissions => {
	description => "The user needs 'VM.Backup' permissions on the backed up guest ID, and 'Datastore.AllocateSpace' on the backup storage.",
	user => 'all',
    },
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    volume => {
		description => "Volume identifier",
		type => 'string',
		completion => \&PVE::Storage::complete_volume,
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;
	my $volume = $param->{volume};

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my $storage_cfg = PVE::Storage::config();
	PVE::Storage::check_volume_access($rpcenv, $authuser, $storage_cfg, undef, $volume);

	my $config_raw = PVE::Storage::extract_vzdump_config($storage_cfg, $volume);

	print "$config_raw\n";
	return;
    }});

my $print_content = sub {
    my ($list) = @_;

    my ($maxlenname, $maxsize) = (0, 0);
    foreach my $info (@$list) {
	my $volid = $info->{volid};
	my $sidlen =  length ($volid);
	$maxlenname = $sidlen if $sidlen > $maxlenname;
	$maxsize = $info->{size} if ($info->{size} // 0) > $maxsize;
    }
    my $sizemaxdigits = length($maxsize);

    my $basefmt = "%-${maxlenname}s %-7s %-9s %${sizemaxdigits}s";
    printf "$basefmt %s\n", "Volid", "Format", "Type", "Size", "VMID";

    foreach my $info (@$list) {
	next if !$info->{vmid};
	my $volid = $info->{volid};

	printf "$basefmt %d\n", $volid, $info->{format}, $info->{content}, $info->{size}, $info->{vmid};
    }

    foreach my $info (sort { $a->{format} cmp $b->{format} } @$list) {
	next if $info->{vmid};
	my $volid = $info->{volid};

	printf "$basefmt\n", $volid, $info->{format}, $info->{content}, $info->{size};
    }
};

my $print_status = sub {
    my $res = shift;

    my $maxlen = 0;
    foreach my $res (@$res) {
	my $storeid = $res->{storage};
	$maxlen = length ($storeid) if length ($storeid) > $maxlen;
    }
    $maxlen+=1;

    printf "%-${maxlen}s %10s %10s %15s %15s %15s %8s\n", 'Name', 'Type',
	'Status', 'Total', 'Used', 'Available', '%';

    foreach my $res (sort { $a->{storage} cmp $b->{storage} } @$res) {
	my $storeid = $res->{storage};

	my $active = $res->{active} ? 'active' : 'inactive';
	my ($per, $per_fmt) = (0, '% 7.2f%%');
	$per = ($res->{used}*100)/$res->{total} if $res->{total} > 0;

	if (!$res->{enabled}) {
	    $per = 'N/A';
	    $per_fmt = '% 8s';
	    $active = 'disabled';
	}

	printf "%-${maxlen}s %10s %10s %15d %15d %15d $per_fmt\n", $storeid,
	    $res->{type}, $active, $res->{total}/1024, $res->{used}/1024,
	    $res->{avail}/1024, $per;
    }
};

__PACKAGE__->register_method ({
    name => 'export',
    path => 'export',
    method => 'GET',
    description => "Used internally to export a volume.",
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    volume => {
		description => "Volume identifier",
		type => 'string',
		completion => \&PVE::Storage::complete_volume,
	    },
	    format => {
		description => "Export stream format",
		type => 'string',
		enum => $KNOWN_EXPORT_FORMATS,
	    },
	    filename => {
		description => "Destination file name",
		type => 'string',
	    },
	    base => {
		description => "Snapshot to start an incremental stream from",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/,
		maxLength => 40,
		optional => 1,
	    },
	    snapshot => {
		description => "Snapshot to export",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/,
		maxLength => 40,
		optional => 1,
	    },
	    'with-snapshots' => {
		description =>
		    "Whether to include intermediate snapshots in the stream",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $filename = $param->{filename};

	my $outfh;
	if ($filename eq '-') {
	    $outfh = \*STDOUT;
	} else {
	    sysopen($outfh, $filename, O_CREAT|O_WRONLY|O_TRUNC)
		or die "open($filename): $!\n";
	}

	eval {
	    my $cfg = PVE::Storage::config();
	    PVE::Storage::volume_export($cfg, $outfh, $param->{volume}, $param->{format},
		$param->{snapshot}, $param->{base}, $param->{'with-snapshots'});
	};
	my $err = $@;
	if ($filename ne '-') {
	    close($outfh);
	    unlink($filename) if $err;
	}
	die $err if $err;
	return;
    }
});

__PACKAGE__->register_method ({
    name => 'import',
    path => 'import',
    method => 'PUT',
    description => "Used internally to import a volume.",
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    volume => {
		description => "Volume identifier",
		type => 'string',
		completion => \&PVE::Storage::complete_volume,
	    },
	    format => {
		description => "Import stream format",
		type => 'string',
		enum => $KNOWN_EXPORT_FORMATS,
	    },
	    filename => {
		description => "Source file name. For '-' stdin is used, the " .
		  "tcp://<IP-or-CIDR> format allows to use a TCP connection as input. " .
		  "Else, the file is treated as common file.",
		type => 'string',
	    },
	    base => {
		description => "Base snapshot of an incremental stream",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/,
		maxLength => 40,
		optional => 1,
	    },
	    'with-snapshots' => {
		description =>
		    "Whether the stream includes intermediate snapshots",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	    'delete-snapshot' => {
		description => "A snapshot to delete on success",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,80}/,
		maxLength => 80,
		optional => 1,
	    },
	    'allow-rename' => {
		description => "Choose a new volume ID if the requested " .
		  "volume ID already exists, instead of throwing an error.",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $filename = $param->{filename};

	my $infh;
	if ($filename eq '-') {
	    $infh = \*STDIN;
	} elsif ($filename =~ m!^tcp://(([^/]+)(/\d+)?)$!) {
	    my ($cidr, $ip, $subnet) = ($1, $2, $3);
	    if ($subnet) { # got real CIDR notation, not just IP
		my $ips = PVE::Network::get_local_ip_from_cidr($cidr);
		die "Unable to get any local IP address in network '$cidr'\n"
		    if scalar(@$ips) < 1;
		die "Got multiple local IP address in network '$cidr'\n"
		    if scalar(@$ips) > 1;

		$ip = $ips->[0];
	    }
	    my $family = PVE::Tools::get_host_address_family($ip);
	    my $port = PVE::Tools::next_migrate_port($family, $ip);

	    my $sock_params = {
		Listen => 1,
		ReuseAddr => 1,
		Proto => &Socket::IPPROTO_TCP,
		GetAddrInfoFlags => 0,
		LocalAddr => $ip,
		LocalPort => $port,
	    };
	    my $socket = IO::Socket::IP->new(%$sock_params)
	        or die "failed to open socket: $!\n";

	    print "$ip\n$port\n"; # tell remote where to connect
	    *STDOUT->flush();

	    my $prev_alarm = alarm 0;
	    local $SIG{ALRM} = sub { die "timed out waiting for client\n" };
	    alarm 30;
	    my $client = $socket->accept; # Wait for a client
	    alarm $prev_alarm;
	    close($socket);

	    $infh = \*$client;
	} else {
	    sysopen($infh, $filename, O_RDONLY)
		or die "open($filename): $!\n";
	}

	my $cfg = PVE::Storage::config();
	my $volume = $param->{volume};
	my $delete = $param->{'delete-snapshot'};
	my $imported_volid = PVE::Storage::volume_import($cfg, $infh, $volume, $param->{format},
	    $param->{base}, $param->{'with-snapshots'}, $param->{'allow-rename'});
	PVE::Storage::volume_snapshot_delete($cfg, $imported_volid, $delete)
	    if defined($delete);
	return $imported_volid;
    }
});

__PACKAGE__->register_method ({
    name => 'nfsscan',
    path => 'nfs',
    method => 'GET',
    description => "Scan remote NFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		path => {
		    description => "The exported path.",
		    type => 'string',
		},
		options => {
		    description => "NFS export options.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};
	my $res = PVE::Storage::scan_nfs($server);

	my $data = [];
	foreach my $k (sort keys %$res) {
	    push @$data, { path => $k, options => $res->{$k} };
	}
	return $data;
    }});

__PACKAGE__->register_method ({
    name => 'cifsscan',
    path => 'cifs',
    method => 'GET',
    description => "Scan remote CIFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	    username => {
		description => "User name.",
		type => 'string',
		optional => 1,
	    },
	    password => {
		description => "User password.",
		type => 'string',
		optional => 1,
	    },
	    domain => {
		description => "SMB domain (Workgroup).",
		type => 'string',
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		share => {
		    description => "The cifs share name.",
		    type => 'string',
		},
		description => {
		    description => "Descriptive text from server.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};

	my $username = $param->{username};
	my $password = $param->{password};
	my $domain = $param->{domain};

	my $res = PVE::Storage::scan_cifs($server, $username, $password, $domain);

	my $data = [];
	foreach my $k (sort keys %$res) {
	    push @$data, { share => $k, description => $res->{$k} };
	}

	return $data;
    }});

# Note: GlusterFS currently does not have an equivalent of showmount.
# As workaround, we simply use nfs showmount.
# see http://www.gluster.org/category/volumes/

__PACKAGE__->register_method ({
    name => 'glusterfsscan',
    path => 'glusterfs',
    method => 'GET',
    description => "Scan remote GlusterFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    server => {
		description => "The server address (name or IP).",
		type => 'string', format => 'pve-storage-server',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		volname => {
		    description => "The volume name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $server = $param->{server};
	my $res = PVE::Storage::scan_nfs($server);

	my $data = [];
	foreach my $path (sort keys %$res) {
	    if ($path =~ m!^/([^\s/]+)$!) {
		push @$data, { volname => $1 };
	    }
	}
	return $data;
    }});

__PACKAGE__->register_method ({
    name => 'iscsiscan',
    path => 'iscsi',
    method => 'GET',
    description => "Scan remote iSCSI server.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    portal => {
		description => "The iSCSI portal (IP or DNS name with optional port).",
		type => 'string', format => 'pve-storage-portal-dns',
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		target => {
		    description => "The iSCSI target name.",
		    type => 'string',
		},
		portal => {
		    description => "The iSCSI portal name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $res = PVE::Storage::scan_iscsi($param->{portal});

	my $data = [];
	foreach my $k (sort keys %$res) {
	    push @$data, { target => $k, portal => join(',', @{$res->{$k}}) };
	}

	return $data;
    }});

__PACKAGE__->register_method ({
    name => 'lvmscan',
    path => 'lvm',
    method => 'GET',
    description => "List local LVM volume groups.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		vg => {
		    description => "The LVM logical volume group name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $res = PVE::Storage::LVMPlugin::lvm_vgs();
	return PVE::RESTHandler::hash_to_array($res, 'vg');
    }});

__PACKAGE__->register_method ({
    name => 'lvmthinscan',
    path => 'lvmthin',
    method => 'GET',
    description => "List local LVM Thin Pools.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    vg => {
		type => 'string',
		pattern => '[a-zA-Z0-9\.\+\_][a-zA-Z0-9\.\+\_\-]+', # see lvm(8) manpage
		maxLength => 100,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		lv => {
		    description => "The LVM Thin Pool name (LVM logical volume).",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Storage::LvmThinPlugin::list_thinpools($param->{vg});
    }});

__PACKAGE__->register_method ({
    name => 'zfsscan',
    path => 'zfs',
    method => 'GET',
    description => "Scan zfs pool list on local node.",
    protected => 1,
    proxyto => "node",
    permissions => {
	check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		pool => {
		    description => "ZFS pool name.",
		    type => 'string',
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Storage::scan_zfs();
    }});

__PACKAGE__->register_method ({
    name => 'prunebackups',
    path => 'prunebackups',
    method => 'GET',
    description => "Prune backups. This is only a wrapper for the proper API endpoints.",
    protected => 1,
    proxyto => 'node',
    parameters => {
	additionalProperties => 0,
	properties => {
	    'dry-run' => {
		description => "Only show what would be pruned, don't delete anything.",
		type => 'boolean',
		optional => 1,
	    },
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
		completion => \&PVE::Storage::complete_storage_enabled,
            }),
	    'prune-backups' => get_standard_option('prune-backups', {
		description => "Use these retention options instead of those from the storage configuration.",
		optional => 1,
	    }),
	    type => {
		description => "Either 'qemu' or 'lxc'. Only consider backups for guests of this type.",
		type => 'string',
		optional => 1,
		enum => ['qemu', 'lxc'],
	    },
	    vmid => get_standard_option('pve-vmid', {
		description => "Only consider backups for this guest.",
		optional => 1,
		completion => \&PVE::Cluster::complete_vmid,
	    }),
	},
    },
    returns => {
	type => 'object',
	properties => {
	    dryrun => {
		description => 'If it was a dry run or not. The list will only be defined in that case.',
		type => 'boolean',
	    },
	    list => {
		type => 'array',
		items => {
		    type => 'object',
		    properties => {
			volid => {
			    description => "Backup volume ID.",
			    type => 'string',
			},
			'ctime' => {
			    description => "Creation time of the backup (seconds since the UNIX epoch).",
			    type => 'integer',
			},
			'mark' => {
			    description => "Whether the backup would be kept or removed. For backups that don't " .
					   "use the standard naming scheme, it's 'protected'.",
			    type => 'string',
			},
			type => {
			    description => "One of 'qemu', 'lxc', 'openvz' or 'unknown'.",
			    type => 'string',
			},
			'vmid' => {
			    description => "The VM the backup belongs to.",
			    type => 'integer',
			    optional => 1,
			},
		    },
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	my $dryrun = extract_param($param, 'dry-run') ? 1 : 0;

	my $list = [];
	if ($dryrun) {
	    $list = PVE::API2::Storage::PruneBackups->dryrun($param);
	} else {
	    PVE::API2::Storage::PruneBackups->delete($param);
	}

	return {
	    dryrun => $dryrun,
	    list => $list,
	};
    }});

our $cmddef = {
    add => [ "PVE::API2::Storage::Config", 'create', ['type', 'storage'] ],
    set => [ "PVE::API2::Storage::Config", 'update', ['storage'] ],
    remove => [ "PVE::API2::Storage::Config", 'delete', ['storage'] ],
    status => [ "PVE::API2::Storage::Status", 'index', [],
		{ node => $nodename }, $print_status ],
    list => [ "PVE::API2::Storage::Content", 'index', ['storage'],
	      { node => $nodename }, $print_content ],
    alloc => [ "PVE::API2::Storage::Content", 'create', ['storage', 'vmid', 'filename', 'size'],
	       { node => $nodename }, sub {
		   my $volid = shift;
		   print "successfully created '$volid'\n";
	       }],
    free => [ "PVE::API2::Storage::Content", 'delete', ['volume'],
	      { node => $nodename } ],
    scan => {
	nfs => [ __PACKAGE__, 'nfsscan', ['server'], { node => $nodename }, sub  {
	    my $res = shift;

	    my $maxlen = 0;
	    foreach my $rec (@$res) {
		my $len = length ($rec->{path});
		$maxlen = $len if $len > $maxlen;
	    }
	    foreach my $rec (@$res) {
		printf "%-${maxlen}s %s\n", $rec->{path}, $rec->{options};
	    }
	}],
	cifs => [ __PACKAGE__, 'cifsscan', ['server'], { node => $nodename }, sub  {
	    my $res = shift;

	    my $maxlen = 0;
	    foreach my $rec (@$res) {
		my $len = length ($rec->{share});
		$maxlen = $len if $len > $maxlen;
	    }
	    foreach my $rec (@$res) {
		printf "%-${maxlen}s %s\n", $rec->{share}, $rec->{description};
	    }
	}],
	glusterfs => [ __PACKAGE__, 'glusterfsscan', ['server'], { node => $nodename }, sub  {
	    my $res = shift;

	    foreach my $rec (@$res) {
		printf "%s\n", $rec->{volname};
	    }
	}],
	iscsi => [ __PACKAGE__, 'iscsiscan', ['portal'], { node => $nodename }, sub  {
	    my $res = shift;

	    my $maxlen = 0;
	    foreach my $rec (@$res) {
		my $len = length ($rec->{target});
		$maxlen = $len if $len > $maxlen;
	    }
	    foreach my $rec (@$res) {
		printf "%-${maxlen}s %s\n", $rec->{target}, $rec->{portal};
	    }
	}],
	lvm => [ __PACKAGE__, 'lvmscan', [], { node => $nodename }, sub  {
	    my $res = shift;
	    foreach my $rec (@$res) {
		printf "$rec->{vg}\n";
	    }
	}],
	lvmthin => [ __PACKAGE__, 'lvmthinscan', ['vg'], { node => $nodename }, sub  {
	    my $res = shift;
	    foreach my $rec (@$res) {
		printf "$rec->{lv}\n";
	    }
	}],
	zfs => [ __PACKAGE__, 'zfsscan', [], { node => $nodename }, sub  {
	    my $res = shift;

	    foreach my $rec (@$res) {
		 printf "$rec->{pool}\n";
	    }
	}],
    },
    nfsscan => { alias => 'scan nfs' },
    cifsscan => { alias => 'scan cifs' },
    glusterfsscan => { alias => 'scan glusterfs' },
    iscsiscan => { alias => 'scan iscsi' },
    lvmscan => { alias => 'scan lvm' },
    lvmthinscan => { alias => 'scan lvmthin' },
    zfsscan => { alias => 'scan zfs' },
    path => [ __PACKAGE__, 'path', ['volume']],
    extractconfig => [__PACKAGE__, 'extractconfig', ['volume']],
    export => [ __PACKAGE__, 'export', ['volume', 'format', 'filename']],
    import => [ __PACKAGE__, 'import', ['volume', 'format', 'filename'], {}, sub  {
	my $volid = shift;
	print PVE::Storage::volume_imported_message($volid);
    }],
    apiinfo => [ __PACKAGE__, 'apiinfo', [], {}, sub {
	my $res = shift;

	print "APIVER $res->{apiver}\n";
	print "APIAGE $res->{apiage}\n";
    }],
    'prune-backups' => [ __PACKAGE__, 'prunebackups', ['storage'], { node => $nodename }, sub {
	my $res = shift;

	my ($dryrun, $list) = ($res->{dryrun}, $res->{list});

	return if !$dryrun;

	print "NOTE: this is only a preview and might not be what a subsequent\n" .
	      "prune call does if backups are removed/added in the meantime.\n\n";

	my @sorted = sort {
	    my $vmcmp = PVE::Tools::safe_compare($a->{vmid}, $b->{vmid}, sub { $_[0] <=> $_[1] });
	    return $vmcmp if $vmcmp ne 0;
	    return $a->{ctime} <=> $b->{ctime};
	} @{$list};

	my $maxlen = 0;
	foreach my $backup (@sorted) {
	    my $volid = $backup->{volid};
	    $maxlen = length($volid) if length($volid) > $maxlen;
	}
	$maxlen+=1;

	printf("%-${maxlen}s %15s %10s\n", 'Backup', 'Backup-ID', 'Prune-Mark');
	foreach my $backup (@sorted) {
	    my $type = $backup->{type};
	    my $vmid = $backup->{vmid};
	    my $backup_id = defined($vmid) ? "$type/$vmid" : "$type";
	    printf("%-${maxlen}s %15s %10s\n", $backup->{volid}, $backup_id, $backup->{mark});
	}
    }],
};

1;
