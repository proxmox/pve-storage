package PVE::API2::Storage::Status;

use strict;
use warnings;

use File::Path;
use File::Basename;
use PVE::Tools;
use PVE::INotify;
use PVE::Cluster qw(cfs_read_file);
use PVE::Storage;
use PVE::API2::Storage::Content;
use PVE::RESTHandler;
use PVE::RPCEnvironment;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Exception qw(raise_param_exc);

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    subclass => "PVE::API2::Storage::Content", 
    # set fragment delimiter (no subdirs) - we need that, because volume
    # IDs may contain a slash '/' 
    fragmentDelimiter => '', 
    path => '{storage}/content',
});

__PACKAGE__->register_method ({
    name => 'index', 
    path => '',
    method => 'GET',
    description => "Get status for all datastores.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option
		('pve-storage-id', {
		    description => "Only list status for  specified storage",
		    optional => 1,
		 }),
	    content => { 
		description => "Only list stores which support this content type.",
		type => 'string', format => 'pve-storage-content',
		optional => 1,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => { storage => { type => 'string' } },
	},
	links => [ { rel => 'child', href => "{storage}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	my $info = PVE::Storage::storage_info($cfg, $param->{content});

	if ($param->{storage}) {
	    my $data = $info->{$param->{storage}};

	    raise_param_exc({ storage => "No such storage." })
		if !defined($data);

	    $data->{storage} = $param->{storage};

	    return [ $data ];
	}
	return PVE::RESTHandler::hash_to_array($info, 'storage');
    }});

__PACKAGE__->register_method ({
    name => 'diridx',
    path => '{storage}', 
    method => 'GET',
    description => "",
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		subdir => { type => 'string' },
	    },
	},
	links => [ { rel => 'child', href => "{subdir}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $res = [
	    { subdir => 'status' },
	    { subdir => 'content' },
	    { subdir => 'upload' },
	    { subdir => 'rrd' },
	    { subdir => 'rrddata' },
	    ];
	
	return $res;
    }});

__PACKAGE__->register_method ({
    name => 'read_status',
    path => '{storage}/status', 
    method => 'GET',
    description => "Read storage status.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => {
	type => "object",
	properties => {},
    },
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	my $info = PVE::Storage::storage_info($cfg, $param->{content});

	my $data = $info->{$param->{storage}};

	raise_param_exc({ storage => "No such storage." })
	    if !defined($data);
    
	return $data;
    }});

__PACKAGE__->register_method ({
    name => 'rrd',
    path => '{storage}/rrd', 
    method => 'GET',
    description => "Read storage RRD statistics (returns PNG).",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    timeframe => {
		description => "Specify the time frame you are interested in.",
		type => 'string',
		enum => [ 'hour', 'day', 'week', 'month', 'year' ],
	    },
	    ds => {
		description => "The list of datasources you want to display.",
 		type => 'string', format => 'pve-configid-list',
	    },
	    cf => {
		description => "The RRD consolidation function",
 		type => 'string',
		enum => [ 'AVERAGE', 'MAX' ],
		optional => 1,
	    },
	},
    },
    returns => {
	type => "object",
	properties => {
	    filename => { type => 'string' },
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Cluster::create_rrd_graph(
	    "pve2-storage/$param->{node}/$param->{storage}", 
	    $param->{timeframe}, $param->{ds}, $param->{cf});
					      
    }});

__PACKAGE__->register_method ({
    name => 'rrddata',
    path => '{storage}/rrddata', 
    method => 'GET',
    description => "Read storage RRD statistics.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    timeframe => {
		description => "Specify the time frame you are interested in.",
		type => 'string',
		enum => [ 'hour', 'day', 'week', 'month', 'year' ],
	    },
	    cf => {
		description => "The RRD consolidation function",
 		type => 'string',
		enum => [ 'AVERAGE', 'MAX' ],
		optional => 1,
	    },
	},
    },
    returns => {
	type => "array",
	items => {
	    type => "object",
	    properties => {},
	},
    },
    code => sub {
	my ($param) = @_;

	return PVE::Cluster::create_rrd_data(
	    "pve2-storage/$param->{node}/$param->{storage}", 
	    $param->{timeframe}, $param->{cf});	      
    }});

__PACKAGE__->register_method ({
    name => 'upload',
    path => '{storage}/upload', 
    method => 'POST',
    description => "Upload file.",
    protected => 1,
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    content => { 
		description => "Content type.",
		type => 'string', format => 'pve-storage-content',
	    },
	    filename => { 
		description => "The name of the file to create.",
		type => 'string',
	    },
	    tmpfilename => { 
		description => "The source file name. This parameter is usually set by the REST handler. You can only overwrite it when connecting to the trustet port on localhost.",
		type => 'string',
		optional => 1,
	    },
	},
    },
    returns => { type => "string" },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $user = $rpcenv->get_user();

	my $cfg = cfs_read_file("storage.cfg");

	my $node = $param->{node};
	my $scfg = PVE::Storage::storage_check_enabled($cfg, $param->{storage}, $node);

	die "cant upload to storage type '$scfg->{type}'" 
	    if !($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs');

	my $content = $param->{content};

	my $tmpfilename = $param->{tmpfilename};
	die "missing temporary file name\n" if !$tmpfilename;

	my $size = -s $tmpfilename;
	die "temporary file '$tmpfilename' does not exists\n" if !defined($size);

	my $filename = $param->{filename};

	chomp $filename;
	$filename =~ s/^.*[\/\\]//;
	$filename =~ s/\s/_/g;

	my $path;

	if ($content eq 'iso') {
	    if ($filename !~ m![^/]+\.[Ii][Ss][Oo]$!) {
		raise_param_exc({ filename => "missing '.iso' extension" });
	    }
	    $path = PVE::Storage::get_iso_dir($cfg, $param->{storage});
	} elsif ($content eq 'vztmpl') {
	    if ($filename !~ m![^/]+\.tar\.gz$!) {
		raise_param_exc({ filename => "missing '.tar.gz' extension" });
	    }
	    $path = get_vztmpl_dir ($cfg, $param->{storage});
	} elsif ($content eq 'backup') {
	    if ($filename !~  m!/([^/]+\.(tar|tgz))$!) {
		raise_param_exc({ filename => "missing '.(tar|tgz)' extension" });
	    }
	    $path = get_backup_dir($cfg, $param->{storage});
	} else {
	    raise_param_exc({ content => "upload content type '$content' not implemented" });
	}

	die "storage '$param->{storage}' does not support '$content' content\n" 
	    if !$scfg->{content}->{$content};

	my $dest = "$path/$filename";
	my $dirname = dirname($dest);

	# we simply overwrite when destination when file already exists

	my $cmd;
	if ($node ne 'localhost' && $node ne PVE::INotify::nodename()) {
	    my $remip = PVE::Cluster::remote_node_ip($node);

	    my @ssh_options = ('-o', 'BatchMode=yes', '-c', 'blowfish-cbc');

	    my @remcmd = ('/usr/bin/ssh', @ssh_options, $remip);

	    eval { 
		# activate remote storage
		PVE::Tools::run_command([@remcmd, '/usr/sbin/pvesm', 'status', 
					 '--storage', $param->{storage}]); 
	    };
	    die "can't activate storage '$param->{storage}' on node '$node'\n" if $@;

 	    PVE::Tools::run_command([@remcmd, '/bin/mkdir', '-p', $dirname],
				    errmsg => "mkdir failed");
 
	    $cmd = ['/usr/bin/scp', @ssh_options, $tmpfilename, "$remip:$dest"];
	} else {
	    PVE::Storage::activate_storage($cfg, $param->{storage});
	    make_path($dirname);
	    $cmd = ['cp', $tmpfilename, $dest];
	}

	my $worker = sub  {
	    my $upid = shift;
	    
	    print "starting file import from: $tmpfilename\n";
	    print "target node: $node\n";
	    print "target file: $dest\n";
	    print "file size is: $size\n";
	    print "command: " . join(' ', @$cmd) . "\n";

	    eval { PVE::Tools::run_command($cmd, errmsg => 'import failed'); };
	    if (my $err = $@) {
		unlink $dest;
		die $err;
	    }
	    print "finished file import successfully\n";
	};

	return $rpcenv->fork_worker('imgcopy', undef, $user, $worker);
   }});
    
1;
