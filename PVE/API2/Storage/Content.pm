package PVE::API2::Storage::Content;

use strict;
use warnings;
use Data::Dumper;

use PVE::SafeSyslog;
use PVE::Cluster;
use PVE::Storage;
use PVE::INotify;
use PVE::Exception qw(raise_param_exc);
use PVE::RPCEnvironment;
use PVE::RESTHandler;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method ({
    name => 'index', 
    path => '',
    method => 'GET',
    description => "List storage content.",
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.Audit', 'Datastore.AllocateSpace'], any => 1],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
		completion => \&PVE::Storage::complete_storage_enabled,
            }),
	    content => { 
		description => "Only list content of this type.",
		type => 'string', format => 'pve-storage-content',
		optional => 1,
		completion => \&PVE::Storage::complete_content_type,
	    },
	    vmid => get_standard_option('pve-vmid', {
		description => "Only list images for this VM",
		optional => 1,
		completion => \&PVE::Cluster::complete_vmid,
	    }),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => { 
		volid => { 
		    type => 'string' 
		} 
	    },
	},
	links => [ { rel => 'child', href => "{volid}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $authuser = $rpcenv->get_user();

	my $storeid = $param->{storage};

	my $cfg = PVE::Storage::config();

	my $vollist = PVE::Storage::volume_list($cfg, $storeid, $param->{vmid}, $param->{content});

	my $res = [];
	foreach my $item (@$vollist) {
	    eval {  PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, undef, $item->{volid}); };
	    next if $@;
	    push @$res, $item;
	}

	return $res;    
    }});

__PACKAGE__->register_method ({
    name => 'create', 
    path => '',
    method => 'POST',
    description => "Allocate disk images.",
    permissions => { 
	check => ['perm', '/storage/{storage}', ['Datastore.AllocateSpace']],
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
		completion => \&PVE::Storage::complete_storage_enabled,
	    }),
	    filename => { 
		description => "The name of the file to create.",
		type => 'string',
	    },
	    vmid => get_standard_option('pve-vmid', {
		description => "Specify owner VM",
		completion => \&PVE::Cluster::complete_vmid,
	    }),
	    size => {
		description => "Size in kilobyte (1024 bytes). Optional suffixes 'M' (megabyte, 1024K) and 'G' (gigabyte, 1024M)",
		type => 'string',
		pattern => '\d+[MG]?',
	    },
	    'format' => {
		type => 'string',
		enum => ['raw', 'qcow2', 'subvol'],
		requires => 'size',
		optional => 1,
	    },
	},
    },
    returns => {
	description => "Volume identifier",
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $storeid = $param->{storage};
	my $name = $param->{filename};
	my $sizestr = $param->{size};

	my $size;
	if ($sizestr =~ m/^\d+$/) {
	    $size = $sizestr;
	} elsif ($sizestr =~ m/^(\d+)M$/) {
	    $size = $1 * 1024;
	} elsif ($sizestr =~ m/^(\d+)G$/) {
	    $size = $1 * 1024 * 1024;
	} else {
	    raise_param_exc({ size => "unable to parse size '$sizestr'" });
	}

	# extract FORMAT from name
	if ($name =~ m/\.(raw|qcow2|vmdk)$/) {
	    my $fmt = $1;

	    raise_param_exc({ format => "different storage formats ($param->{format} != $fmt)" }) 
		if $param->{format} && $param->{format} ne $fmt;

	    $param->{format} = $fmt;
	}

	my $cfg = PVE::Storage::config();
    
	my $volid = PVE::Storage::vdisk_alloc ($cfg, $storeid, $param->{vmid}, 
					       $param->{format}, 
					       $name, $size);

	return $volid;
    }});

# we allow to pass volume names (without storage prefix) if the storage
# is specified as separate parameter.
my $real_volume_id = sub {
    my ($storeid, $volume) = @_;

    my $volid;

    if ($volume =~ m/:/) {
	eval {
	    my ($sid, $volname) = PVE::Storage::parse_volume_id ($volume);
	    die "storage ID missmatch ($sid != $storeid)\n"
		if $storeid && $sid ne $storeid;
	    $volid = $volume;
	    $storeid = $sid;
	};
	raise_param_exc({ volume => $@ }) if $@; 
	   
    } else {
	raise_param_exc({ volume => "no storage speficied - incomplete volume ID" }) 
	    if !$storeid;
	
	$volid = "$storeid:$volume";
    }

    return wantarray ? ($volid, $storeid) : $volid;
};

__PACKAGE__->register_method ({
    name => 'info',
    path => '{volume}',
    method => 'GET',
    description => "Get volume attributes",
    permissions => { 
	description => "You need read access for the volume.",
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', { optional => 1 }),
	    volume => {
		description => "Volume identifier",
		type => 'string', 
	    },
	},
    },
    returns => { type => 'object' },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my ($volid, $storeid) = &$real_volume_id($param->{storage}, $param->{volume});

	my $cfg = PVE::Storage::config();

	PVE::Storage::check_volume_access($rpcenv, $authuser, $cfg, undef, $volid);

	my $path = PVE::Storage::path($cfg, $volid);
	my ($size, $format, $used, $parent) =  PVE::Storage::file_size_info($path);
	die "file_size_info on '$volid' failed\n" if !($format && $size);

	# fixme: return more attributes?
	return {
	    path => $path,
	    size => $size,
            used => $used,
	    format => $format,
	};
    }});

__PACKAGE__->register_method ({
    name => 'delete',
    path => '{volume}',
    method => 'DELETE',
    description => "Delete volume",
    permissions => { 
	description => "You need 'Datastore.Allocate' privilege on the storage (or 'Datastore.AllocateSpace' for backup volumes if you have VM.Backup privilege on the VM).",
	user => 'all',
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', {
                optional => 1,
                completion => \&PVE::Storage::complete_storage,
            }),
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

	my $rpcenv = PVE::RPCEnvironment::get();
	my $authuser = $rpcenv->get_user();

	my $cfg = PVE::Storage::config();

	my ($volid, $storeid) = &$real_volume_id($param->{storage}, $param->{volume});

	my ($path, $ownervm, $vtype) = PVE::Storage::path($cfg, $volid);
	if ($vtype eq 'backup' && $ownervm) {
	    $rpcenv->check($authuser, "/storage/$storeid", ['Datastore.AllocateSpace']);
	    $rpcenv->check($authuser, "/vms/$ownervm", ['VM.Backup']);
	} else {
	    $rpcenv->check($authuser, "/storage/$storeid", ['Datastore.Allocate']);
	}

	PVE::Storage::vdisk_free ($cfg, $volid);

	return undef;
    }});

__PACKAGE__->register_method ({
    name => 'copy',
    path => '{volume}',
    method => 'POST',
    description => "Copy a volume. This is experimental code - do not use.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', { optional => 1}),
	    volume => {
		description => "Source volume identifier",
		type => 'string', 
	    },
	    target => {
		description => "Target volume identifier",
		type => 'string', 
	    },
	    target_node => get_standard_option('pve-node',  { 
		description => "Target node. Default is local node.",
		optional => 1,
	    }),
	},
    },
    returns => { 
	type => 'string',
    },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();

	my $user = $rpcenv->get_user();

	my $target_node = $param->{target_node} || PVE::INotify::nodename();
	# pvesh examples
	# cd /nodes/localhost/storage/local/content
	# pve:/> create local:103/vm-103-disk-1.raw -target local:103/vm-103-disk-2.raw
	# pve:/> create 103/vm-103-disk-1.raw -target 103/vm-103-disk-3.raw

	my $src_volid = &$real_volume_id($param->{storage}, $param->{volume});
	my $dst_volid = &$real_volume_id($param->{storage}, $param->{target});

	print "DEBUG: COPY $src_volid TO $dst_volid\n";

	my $cfg = PVE::Storage::config();

	# do all parameter checks first

	# then do all short running task (to raise errors befor we go to background)

	# then start the worker task
	my $worker = sub  {
	    my $upid = shift;

	    print "DEBUG: starting worker $upid\n";

	    my ($target_sid, $target_volname) = PVE::Storage::parse_volume_id($dst_volid);
	    #my $target_ip = PVE::Cluster::remote_node_ip($target_node);

	    # you need to get this working (fails currently, because storage_migrate() uses
	    # ssh to connect to local host (which is not needed
	    my $sshinfo = PVE::Cluster::get_ssh_info($target_node);
	    PVE::Storage::storage_migrate($cfg, $src_volid, $sshinfo, $target_sid, $target_volname);

	    print "DEBUG: end worker $upid\n";

	};

	return $rpcenv->fork_worker('imgcopy', undef, $user, $worker);
    }});

1;
