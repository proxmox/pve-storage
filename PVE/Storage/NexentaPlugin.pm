package PVE::Storage::NexentaPlugin;

use strict;
use warnings;
use IO::File;
use HTTP::Request;
use LWP::UserAgent;
use MIME::Base64;
use JSON;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

sub nexenta_request {
    my ($scfg, $method, $object, @params) = @_;

    my $apicall = { method => $method, object => $object, params => [ @params ] };

    my $json = encode_json($apicall);

    my $uri = ($scfg->{ssl} ? "https" : "http") . "://" . $scfg->{portal} . ":2000/rest/nms/";
    my $req = HTTP::Request->new('POST', $uri);

    $req->header('Content-Type' => 'application/json');
    $req->content($json);
    my $token = encode_base64("$scfg->{login}:$scfg->{password}");
    $req->header(Authorization => "Basic $token");

    my $ua = LWP::UserAgent->new; # You might want some options here
    my $res = $ua->request($req);
    die $res->content if !$res->is_success;

    my $obj = eval { from_json($res->content); };
    die "JSON not valid. Content: " . $res->content if ($@);
    die "Nexenta API Error: $obj->{error}->{message}\n" if $obj->{error}->{message};
    return $obj->{result};
}


sub nexenta_get_zvol_size {
    my ($scfg, $zvol) = @_;

    return nexenta_request($scfg, 'get_child_prop', 'zvol', $zvol, 'size_bytes');
}

sub nexenta_get_zvol_props {
    my ($scfg, $zvol) = @_;

    my $props = nexenta_request($scfg, 'get_child_props', 'zvol', $zvol, '');
    return $props;
}

sub nexenta_list_lun_mapping_entries {
    my ($scfg, $zvol) = @_;

    return nexenta_request($scfg, 'list_lun_mapping_entries', 'scsidisk', "$scfg->{pool}/$zvol");
}

sub nexenta_add_lun_mapping_entry {
    my ($scfg, $zvol) = @_;

    nexenta_request($scfg, 'add_lun_mapping_entry', 'scsidisk', 
			   "$scfg->{pool}/$zvol", { target_group => "All" });
}

sub nexenta_delete_lu {
    my ($scfg, $zvol) = @_;

    nexenta_request($scfg, 'delete_lu', 'scsidisk', "$scfg->{pool}/$zvol");
}

sub nexenta_create_lu {
    my ($scfg, $zvol) = @_;

    nexenta_request($scfg, 'create_lu', 'scsidisk', "$scfg->{pool}/$zvol", {});
}

sub nexenta_import_lu {
    my ($scfg, $zvol) = @_;

    nexenta_request($scfg, 'import_lu', 'scsidisk', "$scfg->{pool}/$zvol");
}

sub nexenta_create_zvol {
    my ($scfg, $zvol, $size) = @_;

    nexenta_request($scfg, 'create', 'zvol', "$scfg->{pool}/$zvol", "${size}KB",
		    $scfg->{blocksize}, 1);
}

sub nexenta_delete_zvol {
    my ($scfg, $zvol) = @_;

    nexenta_request($scfg, 'destroy', 'zvol', "$scfg->{pool}/$zvol", '-r');
}

sub nexenta_list_zvol {
    my ($scfg) = @_;

    my $zvols = nexenta_request($scfg, 'get_names', 'zvol', '');
    return undef if !$zvols;

    my $list = {};
    foreach my $zvol (@$zvols) {
	my @values = split('/', $zvol);

	my $pool = $values[0];
	my $image = $values[1];
	my $owner;

	if ($image =~ m/^((vm|base)-(\d+)-\S+)$/) {
	    $owner = $3;
	}

	my $props = nexenta_get_zvol_props($scfg, $zvol);
	my $parent = $props->{origin};
	if($parent && $parent =~ m/^$scfg->{pool}\/(\S+)$/){
	    $parent = $1;
	}

	$list->{$pool}->{$image} = {
	    name => $image,
	    size => $props->{size_bytes},
	    parent => $parent,
	    format => 'raw',
	    vmid => $owner
	};
    }

    return $list;
}

# Configuration

sub type {
    return 'nexenta';
}

sub plugindata {
    return {
	content => [ {images => 1}, { images => 1 }],
    };
}

sub properties {
    return {
	login => {
	    description => "login",
	    type => 'string',
	},
	password => {
	    description => "password",
	    type => 'string',
	},
	blocksize => {
	    description => "block size",
	    type => 'string',
	},
	ssl => {
	    description => "ssl",
	    type => 'boolean',
	},
    };
}

sub options {
    return {
        nodes => { optional => 1 },
        disable => { optional => 1 },
	target => { fixed => 1 },
        portal => { fixed => 1 },
	login => { fixed => 1 },
	password => { fixed => 1 },
        pool => { fixed => 1 },
        blocksize => { fixed => 1 },
        ssl => { optional => 1 },
	content => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^(((base|vm)-(\d+)-\S+)\/)?((base)?(vm)?-(\d+)-\S+)$/) {
	return ('images', $5, $8, $2, $4, $6);
    }

    die "unable to parse nexenta volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $map = nexenta_list_lun_mapping_entries($scfg, $name);
    die "could not find lun number" if !$map;
    my $lun = @$map[0]->{lun};
    $lun =~ m/^(\d+)$/ or die "lun is not OK\n";
    $lun = $1;    
    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}

my $find_free_diskname = sub {
    my ($storeid, $scfg, $vmid) = @_;

    my $name = undef;
    my $volumes = nexenta_list_zvol($scfg);
	die "unable de get zvol list" if !$volumes;

    my $disk_ids = {};
    my $dat = $volumes->{$scfg->{pool}};

    foreach my $image (keys %$dat) {
        my $volname = $dat->{$image}->{name};
        if ($volname =~ m/(vm|base)-$vmid-disk-(\d+)/){
            $disk_ids->{$2} = 1;
        }
    }

    #fix: can we search in $rbd hash key with a regex to find (vm|base) ?
    for (my $i = 1; $i < 100; $i++) {
        if (!$disk_ids->{$i}) {
            return "vm-$vmid-disk-$i";
        }
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"

};

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $snap = '__base__';

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
        $class->parse_volname($volname);

    die "create_base not possible with base image\n" if $isBase;

#    die "volname '$volname' contains wrong information about parent $parent $basename\n"
#        if $basename && (!$parent || $parent ne $basename."@".$snap);

    my $newname = $name;
    $newname =~ s/^vm-/base-/;

    my $newvolname = $basename ? "$basename/$newname" : "$newname";

    #we can't rename a nexenta volume, so clone it to a new volname
    nexenta_request($scfg, 'create_snapshot', 'zvol', "$scfg->{pool}/$name", $snap, '');
    nexenta_request($scfg, 'clone', 'zvol', "$scfg->{pool}/$name\@$snap", "$scfg->{pool}/$newname");
    nexenta_create_lu($scfg, $newname);
    nexenta_add_lun_mapping_entry($scfg, $newname);

    my $running  = undef; #fixme : is create_base always offline ?

    $class->volume_snapshot($scfg, $storeid, $newname, $snap, $running);

    return $newvolname;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid) = @_;

    my $snap = '__base__';

    my ($vtype, $basename, $basevmid, undef, undef, $isBase) =
        $class->parse_volname($volname);

    die "clone_image only works on base images\n" if !$isBase;

    my $name = &$find_free_diskname($storeid, $scfg, $vmid);

    warn "clone $volname: $basename to $name\n";

    my $newvol = "$basename/$name";

    nexenta_request($scfg, 'clone', 'zvol', "$scfg->{pool}/$basename\@$snap", "$scfg->{pool}/$name");

    nexenta_create_lu($scfg, $name);
    nexenta_add_lun_mapping_entry($scfg, $name);

    return $newvol;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n"
        if $name && $name !~ m/^vm-$vmid-/;

    $name = &$find_free_diskname($storeid, $scfg, $vmid);

    nexenta_create_zvol($scfg, $name, $size);
    nexenta_create_lu($scfg, $name);
    nexenta_add_lun_mapping_entry($scfg, $name);

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_delete_lu($scfg, $name);
    nexenta_delete_zvol($scfg, $name);

    #if base volume, we delete also the original cloned volume
    if ($isBase) {
	$name =~ s/^base-/vm-/;
	nexenta_delete_lu($scfg, $name);
	nexenta_delete_zvol($scfg, $name);
    }

    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    $cache->{nexenta} = nexenta_list_zvol($scfg) if !$cache->{nexenta};
    my $nexentapool = $scfg->{pool};
    my $res = [];
    if (my $dat = $cache->{nexenta}->{$nexentapool}) {
	foreach my $image (keys %$dat) {

            my $volname = $dat->{$image}->{name};
            my $parent = $dat->{$image}->{parent};

            my $volid = undef;
            if ($parent && $parent =~ m/^(\S+)@(\S+)$/) {
                my ($basename) = ($1);
                $volid = "$storeid:$basename/$volname";
            } else {
                $volid = "$storeid:$volname";
            }

            my $owner = $dat->{$volname}->{vmid};
            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                next if defined ($vmid) && ($owner ne $vmid);
            }

            my $info = $dat->{$volname};
            $info->{volid} = $volid;

            push @$res, $info;

	}
    }

    return $res;
}

sub nexenta_parse_size {
    my ($text) = @_;

    return 0 if !$text;

    if ($text =~ m/^(\d+)([TGMK])?$/) {
	my ($size, $unit) = ($1, $2);
	return $size if !$unit;
	if ($unit eq 'K') {
	    $size *= 1024;
	} elsif ($unit eq 'M') {
	    $size *= 1024*1024;
	} elsif ($unit eq 'G') {
	    $size *= 1024*1024*1024;
	} elsif ($unit eq 'T') {
	    $size *= 1024*1024*1024*1024;
	}
	return $size;
    } else {
	return 0;
    }
}
sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 0;

    eval {
	my $map = nexenta_request($scfg, 'get_child_props', 'volume', $scfg->{pool}, '');
	$active = 1;
	$total = nexenta_parse_size($map->{size});
	$used = nexenta_parse_size($map->{used});
	$free = $total - $used;
    };
    warn $@ if $@;

    return ($total, $free, $used, $active);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $exclusive, $cache) = @_;
    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    return nexenta_get_zvol_size($scfg, "$scfg->{pool}/$name"),
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_request($scfg, 'set_child_prop', 'zvol', "$scfg->{pool}/$name", 'volsize', ($size/1024) . 'KB');
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_request($scfg, 'create_snapshot', 'zvol', "$scfg->{pool}/$name", $snap, '');
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_delete_lu($scfg, $name);

    nexenta_request($scfg, 'rollback', 'snapshot', "$scfg->{pool}/$name\@$snap", '');
    
    nexenta_import_lu($scfg, $name);
    
    nexenta_add_lun_mapping_entry($scfg, $name);
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_request($scfg, 'destroy', 'snapshot', "$scfg->{pool}/$name\@$snap", '');
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	snapshot => { current => 1, snap => 1},
	clone => { base => 1},
	template => { current => 1},
	copy => { base => 1, current => 1},
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) =
	$class->parse_volname($volname);

    my $key = undef;
    if($snapname){
	$key = 'snap';
    }else{
	$key =  $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

1;
