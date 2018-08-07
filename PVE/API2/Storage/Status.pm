package PVE::API2::Storage::Status;

use strict;
use warnings;

use File::Path;
use File::Basename;
use PVE::Tools;
use PVE::INotify;
use PVE::Cluster;
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
    permissions => { 
	description => "Only list entries where you have 'Datastore.Audit' or 'Datastore.AllocateSpace' permissions on '/storage/<storage>'",
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
		description => "Only list status for  specified storage",
		optional => 1,
		completion => \&PVE::Storage::complete_storage_enabled,
	    }),
	    content => { 
		description => "Only list stores which support this content type.",
		type => 'string', format => 'pve-storage-content-list',
		optional => 1,
		completion => \&PVE::Storage::complete_content_type,
	    },
	    enabled => {
		description => "Only list stores which are enabled (not disabled in config).",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	    target => get_standard_option('pve-node', {
		description => "If target is different to 'node', we only lists shared storages which " .
		    "content is accessible on this 'node' and the specified 'target' node.",
		optional => 1,
		completion => \&PVE::Cluster::get_nodelist,
	    }),
	    'format' => {
		description => "Include information about formats",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => {
		storage => get_standard_option('pve-storage-id'),
		type => {
		    description => "Storage type.",
		    type => 'string',
		},
		content => {
		    description => "Allowed storage content types.",
		    type => 'string', format => 'pve-storage-content-list',
		},
		enabled => {
		    description => "Set when storage is enabled (not disabled).",
		    type => 'boolean',
		    optional => 1,
		},
		active => {
		    description => "Set when storage is accessible.",
		    type => 'boolean',
		    optional => 1,
		},
		shared => {
		    description => "Shared flag from storage configuration.",
		    type => 'boolean',
		    optional => 1,
		},
		total => {
		    description => "Total storage space in bytes.",
		    type => 'integer',
		    renderer => 'bytes',
		    optional => 1,
		},
		used => {
		    description => "Used storage space in bytes.",
		    type => 'integer',
		    renderer => 'bytes',
		    optional => 1,
		},
		avail => {
		    description => "Available storage space in bytes.",
		    type => 'integer',
		    renderer => 'bytes',
		    optional => 1,
		},
		used_fraction => {
		    description => "Used fraction (used/total).",
		    type => 'number',
		    renderer => 'fraction_as_percentage',
		    optional => 1,
		},
	    },
	},
	links => [ { rel => 'child', href => "{storage}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my $localnode = PVE::INotify::nodename();

	my $target = $param->{target};

	undef $target if $target && ($target eq $localnode || $target eq 'localhost');

	my $cfg = PVE::Storage::config();

	my $info = PVE::Storage::storage_info($cfg, $param->{content}, $param->{format});

	raise_param_exc({ storage => "No such storage." })
	    if $param->{storage} && !defined($info->{$param->{storage}});

	my $res = {};
	my @sids = PVE::Storage::storage_ids($cfg);
	foreach my $storeid (@sids) {
	    my $data = $info->{$storeid};
	    next if !$data;
	    my $privs = [ 'Datastore.Audit', 'Datastore.AllocateSpace' ];
	    next if !$rpcenv->check_any($authuser, "/storage/$storeid", $privs, 1);
	    next if $param->{storage} && $param->{storage} ne $storeid;

	    my $scfg = PVE::Storage::storage_config($cfg, $storeid);

	    next if $param->{enabled} && $scfg->{disable};

	    if ($target) {
		# check if storage content is accessible on local node and specified target node
		# we use this on the Clone GUI

		next if !$scfg->{shared};
		next if !PVE::Storage::storage_check_node($cfg, $storeid, undef, 1);
		next if !PVE::Storage::storage_check_node($cfg, $storeid, $target, 1);
	    }

	    if ($data->{total}) {
		$data->{used_fraction} = ($data->{used} // 0) / $data->{total};
	    }

	    $res->{$storeid} = $data;
	}

	return PVE::RESTHandler::hash_to_array($res, 'storage');
    }});

__PACKAGE__->register_method ({
    name => 'diridx',
    path => '{storage}', 
    method => 'GET',
    description => "",
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
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
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
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

	my $cfg = PVE::Storage::config();

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
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
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
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
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

# makes no sense for big images and backup files (because it 
# create a copy of the file).
__PACKAGE__->register_method ({
    name => 'upload',
    path => '{storage}/upload', 
    method => 'POST',
    description => "Upload templates and ISO images.",
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.AllocateTemplate']],
    },
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

	my $cfg = PVE::Storage::config();

	my $node = $param->{node};
	my $scfg = PVE::Storage::storage_check_enabled($cfg, $param->{storage}, $node);

	die "can't upload to storage type '$scfg->{type}'\n"
	    if !defined($scfg->{path});

	my $content = $param->{content};

	my $tmpfilename = $param->{tmpfilename};
	die "missing temporary file name\n" if !$tmpfilename;

	my $size = -s $tmpfilename;
	die "temporary file '$tmpfilename' does not exists\n" if !defined($size);

	my $filename = $param->{filename};

	chomp $filename;
	$filename =~ s/^.*[\/\\]//;
	$filename =~ s/[^-a-zA-Z0-9_.]/_/g;

	my $path;

	if ($content eq 'iso') {
	    if ($filename !~ m![^/]+\.[Ii][Ss][Oo]$!) {
		raise_param_exc({ filename => "missing '.iso' extension" });
	    }
	    $path = PVE::Storage::get_iso_dir($cfg, $param->{storage});
	} elsif ($content eq 'vztmpl') {
	    if ($filename !~ m![^/]+\.tar\.[gx]z$!) {
		raise_param_exc({ filename => "missing '.tar.gz' or '.tar.xz' extension" });
	    }
	    $path = PVE::Storage::get_vztmpl_dir($cfg, $param->{storage});
	} else {
	    raise_param_exc({ content => "upload content type '$content' not allowed" });
	}

	die "storage '$param->{storage}' does not support '$content' content\n"
	    if !$scfg->{content}->{$content};

	my $dest = "$path/$filename";
	my $dirname = dirname($dest);

	# we simply overwrite when destination when file already exists

	my $cmd;
	if ($node ne 'localhost' && $node ne PVE::INotify::nodename()) {
	    my $remip = PVE::Cluster::remote_node_ip($node);

	    my @ssh_options = ('-o', 'BatchMode=yes');

	    my @remcmd = ('/usr/bin/ssh', @ssh_options, $remip, '--');

	    eval { 
		# activate remote storage
		PVE::Tools::run_command([@remcmd, '/usr/sbin/pvesm', 'status', 
					 '--storage', $param->{storage}]); 
	    };
	    die "can't activate storage '$param->{storage}' on node '$node'\n" if $@;

	    PVE::Tools::run_command([@remcmd, '/bin/mkdir', '-p', '--', PVE::Tools::shell_quote($dirname)],
				    errmsg => "mkdir failed");
 
	    $cmd = ['/usr/bin/scp', @ssh_options, '--', $tmpfilename, "[$remip]:" . PVE::Tools::shell_quote($dest)];
	} else {
	    PVE::Storage::activate_storage($cfg, $param->{storage});
	    File::Path::make_path($dirname);
	    $cmd = ['cp', '--', $tmpfilename, $dest];
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

	my $upid = $rpcenv->fork_worker('imgcopy', undef, $user, $worker);

	# apache removes the temporary file on return, so we need
	# to wait here to make sure the worker process starts and
	# opens the file before it gets removed.
	sleep(1);

	return $upid;
   }});
    
1;
