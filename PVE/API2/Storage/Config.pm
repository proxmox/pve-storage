package PVE::API2::Storage::Config;

use strict;
use warnings;

use PVE::SafeSyslog;
use PVE::Cluster qw(cfs_read_file cfs_write_file);
use PVE::Storage;
use HTTP::Status qw(:constants);
use Storable qw(dclone);
use PVE::JSONSchema qw(get_standard_option);

use Data::Dumper; # fixme: remove

use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

my @ctypes = qw(images vztmpl iso backup);

my $storage_type_enum = ['dir', 'nfs', 'lvm', 'iscsi'];

my $api_storage_config = sub {
    my ($cfg, $storeid) = @_;

    my $scfg = dclone(PVE::Storage::storage_config ($cfg, $storeid));
    $scfg->{storage} = $storeid;
    delete $scfg->{priority};
    $scfg->{digest} = $cfg->{digest};
    $scfg->{content} = PVE::Storage::content_hash_to_string($scfg->{content});

    if ($scfg->{nodes}) {
	$scfg->{nodes} = join(',', keys(%{$scfg->{nodes}}));
    }

    return $scfg;
};

__PACKAGE__->register_method ({
    name => 'index', 
    path => '',
    method => 'GET',
    description => "Storage index.",
    parameters => {
    	additionalProperties => 0,
	properties => {
	    type => { 
		description => "Only list storage of specific type",
		type => 'string', 
		enum => $storage_type_enum,
		optional => 1,
	    },

	},
    },
    returns => {
	type => 'array',
	items => {
	    type => "object",
	    properties => { storage => { type => 'string'} },
	},
	links => [ { rel => 'child', href => "{storage}" } ],
    },
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	my @sids =  PVE::Storage::storage_ids($cfg);

	my $res = [];
	foreach my $storeid (@sids) {
	    my $scfg = &$api_storage_config($cfg, $storeid);
	    next if $param->{type} && $param->{type} ne $scfg->{type};
	    push @$res, $scfg;
	}

	return $res;
    }});

__PACKAGE__->register_method ({
    name => 'read', 
    path => '{storage}',
    method => 'GET',
    description => "Read storage configuration.",
    parameters => {
    	additionalProperties => 0,
	properties => {
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => {},
    code => sub {
	my ($param) = @_;

	my $cfg = cfs_read_file("storage.cfg");

	return &$api_storage_config($cfg, $param->{storage});
    }});

__PACKAGE__->register_method ({
    name => 'create',
    protected => 1,
    path => '', 
    method => 'POST',
    description => "Create a new storage.",
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    storage => get_standard_option('pve-storage-id'),
	    nodes => get_standard_option('pve-node-list', { optional => 1 }),
	    type => { 
		type => 'string', 
		enum => $storage_type_enum,
	    },
	    path => {
		type => 'string', format => 'pve-storage-path',
		optional => 1,
	    },
	    export => {
		type => 'string', format => 'pve-storage-path',
		optional => 1,
	    },
            server => {
		type => 'string', format => 'pve-storage-server',
		optional => 1,
            },
	    options => {
		type => 'string',  format => 'pve-storage-options',
		optional => 1,
	    },
            target => {
		type => 'string',
		optional => 1,
            },
            vgname => {
		type => 'string', format => 'pve-storage-vgname',
		optional => 1,
            },
	    base => {
		type => 'string', format => 'pve-volume-id',
		optional => 1,
	    },
            portal => {
		type => 'string', format => 'pve-storage-portal-dns',
		optional => 1,
            },
	    content => {
		type => 'string', format => 'pve-storage-content-list',
		optional => 1,
	    },
	    disable => {
		type => 'boolean',
		optional => 1,
	    },
	    shared => {
		type => 'boolean',
		optional => 1,
	    },
	    'format' => { 
		type => 'string', format => 'pve-storage-format',
		optional => 1,
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $type = $param->{type};
	delete $param->{type};

	my $storeid = $param->{storage};
	delete $param->{storage};

	if ($param->{portal}) {
	    $param->{portal} = PVE::Storage::resolv_portal($param->{portal});
	}

	my $opts = PVE::Storage::parse_options($storeid, $type, $param, 1);

        PVE::Storage::lock_storage_config(
	    sub {

		my $cfg = cfs_read_file('storage.cfg');

		if (my $scfg = PVE::Storage::storage_config ($cfg, $storeid, 1)) {
		    die "storage ID '$storeid' already defined\n";
		}

		$cfg->{ids}->{$storeid} = $opts;

		if ($type eq 'lvm' && $opts->{base}) {

		    my ($baseid, $volname) = PVE::Storage::parse_volume_id ($opts->{base});

		    my $basecfg = PVE::Storage::storage_config ($cfg, $baseid, 1);
		    die "base storage ID '$baseid' does not exist\n" if !$basecfg;
       
		    # we only support iscsi for now
		    if (!($basecfg->{type} eq 'iscsi')) {
			die "unsupported base type '$basecfg->{type}'";
		    }

		    my $path = PVE::Storage::path ($cfg, $opts->{base});

		    PVE::Storage::activate_storage($cfg, $baseid);

		    PVE::Storage::lvm_create_volume_group ($path, $opts->{vgname}, $opts->{shared});
		}

		# try to activate if enabled on local node,
		# we only do this to detect errors/problems sooner
		if (PVE::Storage::storage_check_enabled($cfg, $storeid, undef, 1)) {
		    PVE::Storage::activate_storage($cfg, $storeid);
		}

		cfs_write_file('storage.cfg', $cfg);
	    
	    }, "create storage failed");

    }});

__PACKAGE__->register_method ({
    name => 'update',
    protected => 1,
    path => '{storage}',
    method => 'PUT',
    description => "Update storage configuration.",
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    storage => get_standard_option('pve-storage-id'),
	    nodes => get_standard_option('pve-node-list', { optional => 1 }),
	    content => {
		type => 'string', format => 'pve-storage-content-list',
		optional => 1,
	    },
	    'format' => { 
		type => 'string', format => 'pve-storage-format',
		optional => 1,
	    },
	    disable => {
		type => 'boolean',
		optional => 1,
	    },
	    shared => {
		type => 'boolean',
		optional => 1,
	    },
	    options => {
		type => 'string', format => 'pve-storage-options',
		optional => 1,
	    },
	    digest => {
		type => 'string',
		description => 'Prevent changes if current configuration file has different SHA1 digest. This can be used to prevent concurrent modifications.',
		maxLength => 40,
		optional => 1,
	    }
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $storeid = $param->{storage};
	delete($param->{storage});
 
	my $digest = $param->{digest};
	delete($param->{digest});

        PVE::Storage::lock_storage_config(
	 sub {

	    my $cfg = cfs_read_file('storage.cfg');

	    PVE::Storage::assert_if_modified ($cfg, $digest);

	    my $scfg = PVE::Storage::storage_config ($cfg, $storeid);

	    my $opts = PVE::Storage::parse_options($storeid, $scfg->{type}, $param);

	    foreach my $k (%$opts) {
		$scfg->{$k} = $opts->{$k};
	    }

	    cfs_write_file('storage.cfg', $cfg);

	    }, "update storage failed");

	return undef;
    }});

__PACKAGE__->register_method ({
    name => 'delete',
    protected => 1,
    path => '{storage}', # /storage/config/{storage}
    method => 'DELETE',
    description => "Delete storage configuration.",
    parameters => {
    	additionalProperties => 0,
	properties => { 
	    storage => get_standard_option('pve-storage-id'),
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $storeid = $param->{storage};
	delete($param->{storage});
 
        PVE::Storage::lock_storage_config(
	    sub {

		my $cfg = cfs_read_file('storage.cfg');

		die "can't remove storage - storage is used as base of another storage\n"
		    if PVE::Storage::storage_is_used ($cfg, $storeid);

		delete ($cfg->{ids}->{$storeid});

		cfs_write_file('storage.cfg', $cfg);

	    }, "delete storage failed");
  
	return undef;
    }});

1;
