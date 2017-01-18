package PVE::CLI::pvesm;

use strict;
use warnings;

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

use PVE::CLIHandler;

use base qw(PVE::CLIHandler);

my $nodename = PVE::INotify::nodename();

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

    foreach my $res (sort { $a->{storage} cmp $b->{storage} } @$res) {
	my $storeid = $res->{storage};

	my $sum = $res->{used} + $res->{avail};
	my $per = $sum ? (0.5 + ($res->{used}*100)/$sum) : 100;

	printf "%-${maxlen}s %5s %1d %15d %15d %15d %.2f%%\n", $storeid,
	$res->{type}, $res->{active},
	$res->{total}/1024, $res->{used}/1024, $res->{avail}/1024, $per;
    }
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
};

1;
