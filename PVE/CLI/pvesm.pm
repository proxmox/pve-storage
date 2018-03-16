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
use PVE::API2::Storage::Config;
use PVE::API2::Storage::Content;
use PVE::API2::Storage::Status;
use PVE::API2::Storage::Scan;
use PVE::JSONSchema qw(get_standard_option);
use PVE::PTY;

use PVE::CLIHandler;

use base qw(PVE::CLIHandler);

my $KNOWN_EXPORT_FORMATS = ['raw+size', 'tar+size', 'qcow2+size', 'vmdk+size', 'zfs'];

my $nodename = PVE::INotify::nodename();

sub read_password {
    return PVE::PTY::read_password("Enter Password: ");
}

sub setup_environment {
    PVE::RPCEnvironment->setup_default_cli_env();
}

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

    my $maxlenname = 0;
    foreach my $info (@$list) {

	my $volid = $info->{volid};
	my $sidlen =  length ($volid);
	$maxlenname = $sidlen if $sidlen > $maxlenname;
    }

    foreach my $info (@$list) {
	next if !$info->{vmid};
	my $volid = $info->{volid};

	printf "%-${maxlenname}s %5s %10d %d\n", $volid,
	$info->{format}, $info->{size}, $info->{vmid};
    }

    foreach my $info (sort { $a->{format} cmp $b->{format} } @$list) {
	next if $info->{vmid};
	my $volid = $info->{volid};

	printf "%-${maxlenname}s %5s %10d\n", $volid,
	$info->{format}, $info->{size};
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
    description => "Export a volume.",
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
    description => "Import a volume.",
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
		description => "Source file name",
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
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $filename = $param->{filename};

	my $infh;
	if ($filename eq '-') {
	    $infh = \*STDIN;
	} else {
	    sysopen($infh, $filename, O_RDONLY)
		or die "open($filename): $!\n";
	}

	my $cfg = PVE::Storage::config();
	my $volume = $param->{volume};
	my $delete = $param->{'delete-snapshot'};
	PVE::Storage::volume_import($cfg, $infh, $volume, $param->{format},
	    $param->{base}, $param->{'with-snapshots'});
	PVE::Storage::volume_snapshot_delete($cfg, $volume, $delete)
	    if defined($delete);
	return;
    }
});

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
    nfsscan => [ "PVE::API2::Storage::Scan", 'nfsscan', ['server'],
		 { node => $nodename }, sub  {
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
    cifsscan => [ "PVE::API2::Storage::Scan", 'cifsscan', ['server'],
		 { node => $nodename }, sub  {
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
    glusterfsscan => [ "PVE::API2::Storage::Scan", 'glusterfsscan', ['server'],
		 { node => $nodename }, sub  {
		     my $res = shift;

		     foreach my $rec (@$res) {
			 printf "%s\n", $rec->{volname};
		     }
		 }],
    iscsiscan => [ "PVE::API2::Storage::Scan", 'iscsiscan', ['server'],
		   { node => $nodename }, sub  {
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
    lvmscan => [ "PVE::API2::Storage::Scan", 'lvmscan', [],
		 { node => $nodename }, sub  {
		     my $res = shift;
		     foreach my $rec (@$res) {
			 printf "$rec->{vg}\n";
		     }
		 }],
    lvmthinscan => [ "PVE::API2::Storage::Scan", 'lvmthinscan', ['vg'],
		 { node => $nodename }, sub  {
		     my $res = shift;
		     foreach my $rec (@$res) {
			 printf "$rec->{lv}\n";
		     }
		 }],
    zfsscan => [ "PVE::API2::Storage::Scan", 'zfsscan', [],
		 { node => $nodename }, sub  {
		     my $res = shift;

		     foreach my $rec (@$res) {
			 printf "$rec->{pool}\n";
		     }
		 }],
    path => [ __PACKAGE__, 'path', ['volume']],
    extractconfig => [__PACKAGE__, 'extractconfig', ['volume']],
    export => [ __PACKAGE__, 'export', ['volume', 'format', 'filename']],
    import => [ __PACKAGE__, 'import', ['volume', 'format', 'filename']],
};

1;
