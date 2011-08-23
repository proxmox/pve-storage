package PVE::API2::Storage::Content;

use strict;
use warnings;

use PVE::SafeSyslog;
use PVE::Cluster qw(cfs_read_file);
use PVE::Storage;
use PVE::INotify;
use PVE::Exception qw(raise_param_exc);
use PVE::RPCEnvironment;
use PVE::RESTHandler;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::RESTHandler);

my @ctypes = qw(images vztmpl iso backup);

__PACKAGE__->register_method ({
    name => 'index', 
    path => '',
    method => 'GET',
    description => "List storage content.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    content => { 
		description => "Only list content of this type.",
		type => 'string', format => 'pve-storage-content',
		optional => 1,
	    },
	    vmid => get_standard_option
		('pve-vmid', { 
		    description => "Only list images for this VM",
		    optional => 1,		
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

	my $cts = $param->{content} ? [ $param->{content} ] : [ @ctypes ];

	my $storeid = $param->{storage};

	my $cfg = cfs_read_file("storage.cfg");

	my $scfg = PVE::Storage::storage_config ($cfg, $storeid);

	my $res = [];
	foreach my $ct (@$cts) {
	    my $data;
	    if ($ct eq 'images') {
		$data = PVE::Storage::vdisk_list ($cfg, $storeid, $param->{vmid});
	    } elsif ($ct eq 'iso') {
		$data = PVE::Storage::template_list ($cfg, $storeid, 'iso') 
		    if !$param->{vmid};
	    } elsif ($ct eq 'vztmpl') {
		$data = PVE::Storage::template_list ($cfg, $storeid, 'vztmpl') 
		    if !$param->{vmid};
	    } elsif ($ct eq 'backup') {
		$data = PVE::Storage::template_list ($cfg, $storeid, 'backup') 
		    if !$param->{vmid};
	    }

	    next if !$data || !$data->{$storeid};

	    foreach my $item (@{$data->{$storeid}}) {
		push @$res, $item;
	    }
	}

	return $res;    
    }});

__PACKAGE__->register_method ({
    name => 'create', 
    path => '',
    method => 'POST',
    description => "Allocate disk images.",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id'),
	    filename => { 
		description => "The name of the file to create/upload.",
		type => 'string',
	    },
	    vmid => get_standard_option('pve-vmid', { description => "Specify owner VM" } ),
	    size => {
		description => "Size in kilobyte (1024 bytes). Optional suffixes 'M' (megabyte, 1024K) and 'G' (gigabyte, 1024M)",
		type => 'string',
		pattern => '\d+[MG]?',
	    },
	    'format' => {
		type => 'string',
		enum => ['raw', 'qcow2'],
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
	if ($name =~ m/\.(raw|qcow2)$/) {
	    my $fmt = $1;

	    raise_param_exc({ format => "different storage formats ($param->{format} != $fmt)" }) 
		if $param->{format} && $param->{format} ne $fmt;

	    $param->{format} = $fmt;
	}

	my $cfg = cfs_read_file('storage.cfg');
    
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
	    raise_param_exc({ storage => "storage ID missmatch" }) 
		if $storeid && $sid ne $storeid;
	    $volid = $volume;
	};
	raise_param_exc({ volume => $@}) if $@; 
	   
    } else {
	raise_param_exc({ volume => "no storage speficied - incomplete volume ID" }) 
	    if !$storeid;
	
	$volid = "$storeid:$volume";
    }

    return $volid;
};

__PACKAGE__->register_method ({
    name => 'info',
    path => '{volume}',
    method => 'GET',
    description => "Get volume attributes",
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

	my $volid = &$real_volume_id($param->{storage}, $param->{volume});

	my $cfg = cfs_read_file('storage.cfg');

	my $path = PVE::Storage::path($cfg, $volid);
	my ($size, $format, $used) = PVE::Storage::file_size_info ($path);

	# fixme: return more attributes?
	return {
	    path => $path,
	    size => $size,
            used => $used,
	};
    }});

__PACKAGE__->register_method ({
    name => 'delete',
    path => '{volume}',
    method => 'DELETE',
    description => "Delete volume",
    protected => 1,
    proxyto => 'node',
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    node => get_standard_option('pve-node'),
	    storage => get_standard_option('pve-storage-id', { optional => 1}),
	    volume => {
		description => "Volume identifier",
		type => 'string', 
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $volid = &$real_volume_id($param->{storage}, $param->{volume});
	
	my $cfg = cfs_read_file('storage.cfg');

	PVE::Storage::vdisk_free ($cfg, $volid);

	return undef;
    }});

1;
