package PVE::Storage::ISCSIPlugin;

use strict;
use warnings;
use File::stat;
use IO::Dir;
use IO::File;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach $IPV4RE $IPV6RE);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# iscsi helper function

my $ISCSIADM = '/usr/bin/iscsiadm';
$ISCSIADM = undef if ! -X $ISCSIADM;

sub check_iscsi_support {
    my $noerr = shift;

    if (!$ISCSIADM) {
	my $msg = "no iscsi support - please install open-iscsi";
	if ($noerr) {
	    warn "warning: $msg\n";
	    return 0;
	}

	die "error: $msg\n";
    }

    return 1;
}

sub iscsi_session_list {

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'session'];

    my $res = {};

    eval {
	run_command($cmd, errmsg => 'iscsi session scan failed', outfunc => sub {
	    my $line = shift;
	    
	    if ($line =~ m/^tcp:\s+\[(\S+)\]\s+\S+\s+(\S+)(\s+\S+)?\s*$/) {
		my ($session, $target) = ($1, $2);
		# there can be several sessions per target (multipath)
		push @{$res->{$target}}, $session;   
	    }
	});
    };
    if (my $err = $@) {
	die $err if $err !~ m/: No active sessions.$/i;
    }

    return $res;
}

sub iscsi_test_portal {
    my ($portal) = @_;

    my ($server, $port) = PVE::Tools::parse_host_and_port($portal);
    return 0 if !$server;
    return PVE::Network::tcp_ping($server, $port || 3260, 2);
}

sub iscsi_discovery {
    my ($portal) = @_;

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'discovery', '--type', 'sendtargets', 
	       '--portal', $portal];

    my $res = {};

    return $res if !iscsi_test_portal($portal); # fixme: raise exception here?

    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~ m/^((?:$IPV4RE|\[$IPV6RE\]):\d+)\,\S+\s+(\S+)\s*$/) {
	    my $portal = $1;
	    my $target = $2;
	    # one target can have more than one portal (multipath).
	    push @{$res->{$target}}, $portal;
	}
    });

    return $res;
}

sub iscsi_login {
    my ($target, $portal_in) = @_;

    check_iscsi_support ();

    eval { iscsi_discovery ($portal_in); };
    warn $@ if $@;

    my $cmd = [$ISCSIADM, '--mode', 'node', '--targetname',  $target, '--login'];
    run_command($cmd);
}

sub iscsi_logout {
    my ($target, $portal) = @_;

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'node', '--targetname', $target, '--logout'];
    run_command($cmd);
}

my $rescan_filename = "/var/run/pve-iscsi-rescan.lock";

sub iscsi_session_rescan {
    my $session_list = shift;

    check_iscsi_support();

    my $rstat = stat($rescan_filename);

    if (!$rstat) {
	if (my $fh = IO::File->new($rescan_filename, "a")) {
	    utime undef, undef, $fh;
	    close($fh);
	}
    } else {
	my $atime = $rstat->atime;
	my $tdiff = time() - $atime;
	# avoid frequent rescans
	return if !($tdiff < 0 || $tdiff > 10);
	utime undef, undef, $rescan_filename;
    }

    foreach my $session (@$session_list) {
	my $cmd = [$ISCSIADM, '--mode', 'session', '--sid', $session, '--rescan'];
	eval { run_command($cmd, outfunc => sub {}); };
	warn $@ if $@;
    }
}

sub load_stable_scsi_paths {

    my $stable_paths = {};

    my $stabledir = "/dev/disk/by-id";

    if (my $dh = IO::Dir->new($stabledir)) {
       while (defined(my $tmp = $dh->read)) {
           # exclude filenames with part in name (same disk but partitions)
           # use only filenames with scsi(with multipath i have the same device 
	   # with dm-uuid-mpath , dm-name and scsi in name)
           if($tmp !~ m/-part\d+$/ && $tmp =~ m/^scsi-/) {
                 my $path = "$stabledir/$tmp";
                 my $bdevdest = readlink($path);
		 if ($bdevdest && $bdevdest =~ m|^../../([^/]+)|) {
		     $stable_paths->{$1}=$tmp;
		 }
	   }
       }
       $dh->close;
    }
    return $stable_paths;
}

sub iscsi_device_list {

    my $res = {};

    my $dirname = '/sys/class/iscsi_session';

    my $stable_paths = load_stable_scsi_paths();

    dir_glob_foreach($dirname, 'session(\d+)', sub {
	my ($ent, $session) = @_;

	my $target = file_read_firstline("$dirname/$ent/targetname");
	return if !$target;

	my (undef, $host) = dir_glob_regex("$dirname/$ent/device", 'target(\d+):.*');
	return if !defined($host);

	dir_glob_foreach("/sys/bus/scsi/devices", "$host:" . '(\d+):(\d+):(\d+)', sub {
	    my ($tmp, $channel, $id, $lun) = @_;

	    my $type = file_read_firstline("/sys/bus/scsi/devices/$tmp/type");
	    return if !defined($type) || $type ne '0'; # list disks only

	    my $bdev;
	    if (-d "/sys/bus/scsi/devices/$tmp/block") { # newer kernels
		(undef, $bdev) = dir_glob_regex("/sys/bus/scsi/devices/$tmp/block/", '([A-Za-z]\S*)');
	    } else {
		(undef, $bdev) = dir_glob_regex("/sys/bus/scsi/devices/$tmp", 'block:(\S+)');
	    }
	    return if !$bdev;

	    #check multipath           
	    if (-d "/sys/block/$bdev/holders") { 
		my $multipathdev = dir_glob_regex("/sys/block/$bdev/holders", '[A-Za-z]\S*');
		$bdev = $multipathdev if $multipathdev;
	    }

	    my $blockdev = $stable_paths->{$bdev};
	    return if !$blockdev;

	    my $size = file_read_firstline("/sys/block/$bdev/size");
	    return if !$size;

	    my $volid = "$channel.$id.$lun.$blockdev";

	    $res->{$target}->{$volid} = {
		'format' => 'raw', 
		'size' => int($size * 512), 
		'vmid' => 0, # not assigned to any vm
		'channel' => int($channel),
		'id' => int($id),
		'lun' => int($lun),
	    };

	    #print "TEST: $target $session $host,$bus,$tg,$lun $blockdev\n"; 
	});

    });

    return $res;
}

# Configuration

sub type {
    return 'iscsi';
}

sub plugindata {
    return {
	content => [ {images => 1, none => 1}, { images => 1 }],
	select_existing => 1,
    };
}

sub properties {
    return {
	target => {
	    description => "iSCSI target.",
	    type => 'string',
	},
	portal => {
	    description => "iSCSI portal (IP or DNS name with optional port).",
	    type => 'string', format => 'pve-storage-portal-dns',
	},
    };
}

sub options {
    return {
        portal => { fixed => 1 },
        target => { fixed => 1 },
        nodes => { optional => 1},
	disable => { optional => 1},
	content => { optional => 1},
	bwlimit => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^\d+\.\d+\.\d+\.(\S+)$!) {
	return ('images', $1, undef, undef, undef, undef, 'raw');
    }

    die "unable to parse iscsi volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    die "snapshot is not possible on iscsi storage\n" if defined($snapname);

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    
    my $path = "/dev/disk/by-id/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in iscsi storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in iscsi storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "can't allocate space in iscsi storage\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    die "can't free space in iscsi storage\n";
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $res = [];

    $cache->{iscsi_devices} = iscsi_device_list() if !$cache->{iscsi_devices};

    # we have no owner for iscsi devices

    my $target = $scfg->{target};

    if (my $dat = $cache->{iscsi_devices}->{$target}) {

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

    $cache->{iscsi_sessions} = iscsi_session_list() if !$cache->{iscsi_sessions};

    my $active = defined($cache->{iscsi_sessions}->{$scfg->{target}});

    return (0, 0, 0, $active);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return if !check_iscsi_support(1);

    $cache->{iscsi_sessions} = iscsi_session_list() if !$cache->{iscsi_sessions};

    my $iscsi_sess = $cache->{iscsi_sessions}->{$scfg->{target}};
    if (!defined ($iscsi_sess)) {
	eval { iscsi_login($scfg->{target}, $scfg->{portal}); };
	warn $@ if $@;
    } else {
	# make sure we get all devices
	iscsi_session_rescan($iscsi_sess);
    }
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    return if !check_iscsi_support(1);

    $cache->{iscsi_sessions} = iscsi_session_list() if !$cache->{iscsi_sessions};

    my $iscsi_sess = $cache->{iscsi_sessions}->{$scfg->{target}};

    if (defined ($iscsi_sess)) {
	iscsi_logout($scfg->{target}, $scfg->{portal});
    }
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    my $portal = $scfg->{portal};
    return iscsi_test_portal($portal);
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    die "volume resize is not possible on iscsi device";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
	copy => { current => 1},
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
