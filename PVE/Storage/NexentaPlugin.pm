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
 my ($scfg, $json) = @_;

        my $uri = "http://".$scfg->{portal}.":2000/rest/nms/";
        my $req = HTTP::Request->new( 'POST', $uri );

        $req->header( 'Content-Type' => 'application/json' );
        $req->content( $json );
        my $token = encode_base64("$scfg->{login}:$scfg->{password}");
        $req->header( Authorization => "Basic $token" );

        my $ua = LWP::UserAgent->new; # You might want some options here
        my $res = $ua->request($req);
        if (!$res->is_success) {
                die $res->content;

        }
        my $obj = from_json($res->content);
        print $obj->{error}->{message} if $obj->{error}->{message};
        return undef if $obj->{error}->{message};
        return $obj->{result} if $obj->{result};
        return 1;
}


sub nexenta_list_lun_mapping_entries {
 my ($zvol, $scfg) = @_;
       

       my $json = '{"method": "list_lun_mapping_entries","object" : "scsidisk","params": ["'.$scfg->{pool}.'/'.$zvol.'"]}';
       my $map = nexenta_request($scfg,$json);
       return $map if $map;
       return undef;

}

sub nexenta_add_lun_mapping_entry {
 my ($zvol, $scfg) = @_;

      
	my $json = '{"method": "add_lun_mapping_entry","object" : "scsidisk","params": ["'.$scfg->{pool}.'/'.$zvol.'",{"target_group": "All"}]}';
 
        return undef if !nexenta_request($scfg, $json);
	return 1;

}


sub nexenta_delete_lu {
 my ($zvol, $scfg) = @_;

        my $json = '{"method": "delete_lu","object" : "scsidisk","params": ["'.$scfg->{pool}.'/'.$zvol.'"]}';
        return undef if !nexenta_request($scfg, $json);
	return 1;

}

sub nexenta_create_lu {
    my ($zvol, $scfg) = @_;

	my $json = '{"method": "create_lu","object" : "scsidisk","params": ["'.$scfg->{pool}.'/'.$zvol.'",{}]}';

        return undef if !nexenta_request($scfg, $json);
	return 1;

}

sub nexenta_create_zvol {
   my ($zvol, $size, $scfg) = @_;

        
        my $blocksize = $scfg->{blocksize};
        my $nexentapool = $scfg->{pool};

	my $json = '{"method": "create","object" : "zvol","params": ["'.$nexentapool.'/'.$zvol.'", "'.$size.'KB", "'.$blocksize.'", "1"]}';
        
        return undef if !nexenta_request($scfg, $json);
	return 1;

}
sub nexenta_delete_zvol {
    my ($zvol, $scfg) = @_;
	sleep 5;
        my $json = '{"method": "destroy","object" : "zvol","params": ["'.$scfg->{pool}.'/'.$zvol.'", ""]}';
	return undef if !nexenta_request($scfg, $json);
	return 1;

}
 

sub nexenta_list_zvol {
    my ($scfg) = @_;



	my $json = '{"method": "get_names","object" : "zvol","params": [""]}';
	my $volumes = {};
	
	my $zvols = nexenta_request($scfg, $json);
	return undef if !$zvols;

	my $list = {};

	foreach my $zvol (@$zvols) {
	  my @values = split('/', $zvol);
	  #$volumes->{$values[0]}->{$values[1]}->{volname} = $values[1];

	  my $pool = $values[0];
          my $image = $values[1];
          my $owner;
          if ($image =~ m/^(vm-(\d+)-\S+)$/) {
                $owner = $2;
          }

          $list->{$pool}->{$image} = {
             name => $image,
             size => "",
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

    };
}

sub options {
    return {
	target => { fixed => 1 },
        portal => { fixed => 1 },
	login => { fixed => 1 },
	password => { fixed => 1 },
        pool => { fixed => 1 },
        blocksize => { fixed => 1 },
	content => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

   
    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
	return ('images', $1, $2);
    }

    return('images',$volname,'');
    #die "unable to parse lvm volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $map = nexenta_list_lun_mapping_entries($name,$scfg);
    die "could not find lun number" if !$map;
    my $lun = @$map[0]->{lun};


    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}


sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw';

    die "illegal name '$name' - sould be 'vm-$vmid-*'\n" 
	if  $name && $name !~ m/^vm-$vmid-/;

  

    my $nexentapool = $scfg->{'pool'};

    if (!$name) {
	
	my $volumes = nexenta_list_zvol($scfg);
	die "unable de get zvol list" if !$volumes;

	for (my $i = 1; $i < 100; $i++) {

	    my $tn = "vm-$vmid-disk-$i";
	    if (!defined ($volumes->{$nexentapool}->{$tn})) {
		$name = $tn;
		last;
	    }
	}
    }

    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
	if !$name;

    nexenta_create_zvol($name, $size, $scfg);
    sleep 1;
    nexenta_create_lu($name, $scfg);
    sleep 1;
    die "error create zvol" if !nexenta_add_lun_mapping_entry($name, $scfg);

    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    nexenta_delete_lu($name, $scfg);
    sleep 5;
    die "error deleting volume" if !nexenta_delete_zvol($name, $scfg);


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

            my $volid = "$storeid:$volname";


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

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 1;
    return ($total,$free,$used,$active);

    return undef;
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


1;
