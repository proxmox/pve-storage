package PVE::Storage::ISCSIDirectPlugin;

use strict;
use warnings;
use IO::File;
use HTTP::Request;
use LWP::UserAgent;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);


sub iscsi_ls{
 my ($scfg, $storeid) = @_;


    my $portal = $scfg->{portal};
    my $cmd = ['/usr/bin/iscsi-ls', '-s', 'iscsi://'.$portal ];
    my $list = {};
    my $test  = "";
 
     my $errfunc = sub {
         my $line = shift;
         $line = trim($line);

         die $line if $line;
    };

    eval {

	    run_command($cmd, errmsg => "iscsi error", errfunc => $errfunc,outfunc => sub {
	        my $line = shift;
	        $line = trim($line);
	        if( $line =~ /Lun:(\d+)\s+([A-Za-z0-9\-\_\.\:]*)\s+\(Size:(\d+)G\)/ ) {
		$test = $1;
	
	            my $image = $1;
	            my $size = $3;
		    
	            $list->{$storeid}->{$image} = {
	                name => $image,
	                size => $size,
	            };
	        }
	    });
    };

    my $err = $@;
    die $err if $err && $err !~ m/TESTUNITREADY failed with SENSE KEY/ ;
    return $list;

}

# Configuration

sub type {
    return 'iscsidirect';
}

sub plugindata {
    return {
        content => [ {images => 1, none => 1}, { images => 1 }],
    };
}

sub options {
    return {
        portal => { fixed => 1 },
        target => { fixed => 1 },
        nodes => { optional => 1},
        disable => { optional => 1},
        content => { optional => 1},
    };
}


# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

   
    if ($volname =~ m/^(\d+)$/) {
	return ('images', $1, undef);
    }

    die "unable to parse iscsi volume name '$volname'\n";

}

sub path {
    my ($class, $scfg, $volname) = @_;

    my ($vtype, $lun, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}


sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "can't allocate space in iscsi storage\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't free space in iscsi storage\n";
}


sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $res = [];

    $cache->{directiscsi} = iscsi_ls($scfg,$storeid) if !$cache->{directiscsi};

    # we have no owner for iscsi devices

    my $target = $scfg->{target};

    if (my $dat = $cache->{directiscsi}->{$storeid}) {

        foreach my $volname (keys %$dat) {

            my $volid = "$storeid:$volname";

            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                # we have no owner for iscsi devices
                next if defined($vmid);
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
