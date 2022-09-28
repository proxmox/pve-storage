package PVE::CLI::pvesm;

use strict;
use warnings;

use POSIX qw(O_RDONLY O_WRONLY O_CREAT O_TRUNC);
use Fcntl ':flock';
use File::Path;
use MIME::Base64 qw(encode_base64);

use IO::Socket::IP;
use IO::Socket::UNIX;
use Socket qw(SOCK_STREAM);

use PVE::SafeSyslog;
use PVE::Cluster;
use PVE::INotify;
use PVE::RPCEnvironment;
use PVE::Storage;
use PVE::Tools qw(extract_param);
use PVE::API2::Storage::Config;
use PVE::API2::Storage::Content;
use PVE::API2::Storage::PruneBackups;
use PVE::API2::Storage::Scan;
use PVE::API2::Storage::Status;
use PVE::JSONSchema qw(get_standard_option);
use PVE::PTY;

use PVE::CLIHandler;

use base qw(PVE::CLIHandler);

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

    my $master_key_map = {
	name => 'master-pubkey',
	desc => 'a file containing a PEM-formatted master public key',
	func => sub {
	    my ($value) = @_;
	    return encode_base64(PVE::Tools::file_get_contents($value), '');
	}
    };

    my $keyring_map = {
	name => 'keyring',
	desc => 'file containing the keyring to authenticate in the Ceph cluster',
	func => sub {
	    my ($value) = @_;
	    return PVE::Tools::file_get_contents($value);
	},
    };

    my $mapping = {
	'cifsscan' => [ $password_map ],
	'cifs' => [ $password_map ],
	'pbs' => [ $password_map ],
	'create' => [ $password_map, $enc_key_map, $master_key_map, $keyring_map ],
	'update' => [ $password_map, $enc_key_map, $master_key_map, $keyring_map ],
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
	PVE::Storage::check_volume_access(
	    $rpcenv,
	    $authuser,
	    $storage_cfg,
	    undef,
	    $volume,
	    'backup',
	);

	if (PVE::Storage::parse_volume_id($volume, 1)) {
	    my (undef, undef, $ownervm) = PVE::Storage::parse_volname($storage_cfg, $volume);
	    $rpcenv->check($authuser, "/vms/$ownervm", ['VM.Backup']);
	}

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
		enum => $PVE::Storage::KNOWN_EXPORT_FORMATS,
	    },
	    filename => {
		description => "Destination file name",
		type => 'string',
	    },
	    base => {
		description => "Snapshot to start an incremental stream from",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/i,
		maxLength => 40,
		optional => 1,
	    },
	    snapshot => {
		description => "Snapshot to export",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/i,
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
	    'snapshot-list' => {
		description => "Ordered list of snapshots to transfer",
		type => 'string',
		format => 'string-list',
		optional => 1,
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $with_snapshots = $param->{'with-snapshots'};
	if (defined(my $list = $param->{'snapshot-list'})) {
	    $with_snapshots = PVE::Tools::split_list($list);
	}

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
		$param->{snapshot}, $param->{base}, $with_snapshots);
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
		enum => $PVE::Storage::KNOWN_EXPORT_FORMATS,
	    },
	    filename => {
		description => "Source file name. For '-' stdin is used, the " .
		  "tcp://<IP-or-CIDR> format allows to use a TCP connection, " .
		  "the unix://PATH-TO-SOCKET format a UNIX socket as input." .
		  "Else, the file is treated as common file.",
		type => 'string',
	    },
	    base => {
		description => "Base snapshot of an incremental stream",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/i,
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
		pattern => qr/[a-z0-9_\-]{1,80}/i,
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
	    snapshot => {
		description => "The current-state snapshot if the stream contains snapshots",
		type => 'string',
		pattern => qr/[a-z0-9_\-]{1,40}/i,
		maxLength => 40,
		optional => 1,
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
	} elsif ($filename =~ m!^unix://(.*)$!) {
	    my $socket_path = $1;
	    my $socket = IO::Socket::UNIX->new(
		Type => SOCK_STREAM(),
		Local => $socket_path,
		Listen => 1,
	    ) or die "failed to open socket: $!\n";

	    print "ready\n";
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
	    $param->{snapshot}, $param->{base}, $param->{'with-snapshots'},
	    $param->{'allow-rename'});
	PVE::Storage::volume_snapshot_delete($cfg, $imported_volid, $delete)
	    if defined($delete);
	return $imported_volid;
    }
});

__PACKAGE__->register_method ({
    name => 'prunebackups',
    path => 'prunebackups',
    method => 'GET',
    description => "Prune backups. Only those using the standard naming scheme are considered. " .
		   "If no keep options are specified, those from the storage configuration are used.",
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
	    %{$PVE::Storage::Plugin::prune_backups_format},
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

	my $keep_opts;
	foreach my $keep (keys %{$PVE::Storage::Plugin::prune_backups_format}) {
	    $keep_opts->{$keep} = extract_param($param, $keep) if defined($param->{$keep});
	}
	$param->{'prune-backups'} = PVE::JSONSchema::print_property_string(
	    $keep_opts, $PVE::Storage::Plugin::prune_backups_format) if $keep_opts;

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

my $print_api_result = sub {
    my ($data, $schema, $options) = @_;
    PVE::CLIFormatter::print_api_result($data, $schema, undef, $options);
};

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
	nfs => [ "PVE::API2::Storage::Scan", 'nfsscan', ['server'], { node => $nodename }, sub  {
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
	cifs => [ "PVE::API2::Storage::Scan", 'cifsscan', ['server'], { node => $nodename }, sub  {
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
	glusterfs => [ "PVE::API2::Storage::Scan", 'glusterfsscan', ['server'], { node => $nodename }, sub  {
	    my $res = shift;

	    foreach my $rec (@$res) {
		printf "%s\n", $rec->{volname};
	    }
	}],
	iscsi => [ "PVE::API2::Storage::Scan", 'iscsiscan', ['portal'], { node => $nodename }, sub  {
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
	lvm => [ "PVE::API2::Storage::Scan", 'lvmscan', [], { node => $nodename }, sub  {
	    my $res = shift;
	    foreach my $rec (@$res) {
		printf "$rec->{vg}\n";
	    }
	}],
	lvmthin => [ "PVE::API2::Storage::Scan", 'lvmthinscan', ['vg'], { node => $nodename }, sub  {
	    my $res = shift;
	    foreach my $rec (@$res) {
		printf "$rec->{lv}\n";
	    }
	}],
	pbs => [
	    "PVE::API2::Storage::Scan",
	    'pbsscan',
	    ['server', 'username'],
	    { node => $nodename },
	    $print_api_result,
	    $PVE::RESTHandler::standard_output_options,
	],
	zfs => [ "PVE::API2::Storage::Scan", 'zfsscan', [], { node => $nodename }, sub  {
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

	if (!scalar(@{$list})) {
	    print "No backups found\n";
	    return;
	}

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
