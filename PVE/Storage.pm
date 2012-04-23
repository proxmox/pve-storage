package PVE::Storage;

use strict;
use POSIX;
use IO::Select;
use IO::Dir;
use IO::File;
use Fcntl ':flock';
use File::stat;
use File::Basename;
use File::Path;
use IPC::Open2;
use Cwd 'abs_path';
use Getopt::Long qw(GetOptionsFromArray);
use Socket;
use Digest::SHA;
use Net::Ping;

use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Cluster qw(cfs_register_file cfs_read_file cfs_write_file cfs_lock_file);
use PVE::Exception qw(raise_param_exc);
use PVE::JSONSchema;
use PVE::INotify;
use PVE::RPCEnvironment;

my $ISCSIADM = '/usr/bin/iscsiadm';
my $UDEVADM = '/sbin/udevadm';

$ISCSIADM = undef if ! -X $ISCSIADM;

# fixme: always_call_parser => 1 ??
cfs_register_file ('storage.cfg', 
		   \&parse_config, 
		   \&write_config); 

# generic utility function

sub config {
    return cfs_read_file("storage.cfg");
}

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

sub read_proc_mounts {
    
    local $/; # enable slurp mode
    
    my $data = "";
    if (my $fd = IO::File->new ("/proc/mounts", "r")) {
	$data = <$fd>;
	close ($fd);
    }

    return $data;
}

#  PVE::Storage utility functions

sub lock_storage_config {
    my ($code, $errmsg) = @_;

    cfs_lock_file("storage.cfg", undef, $code);
    my $err = $@;
    if ($err) {
	$errmsg ? die "$errmsg: $err" : die $err;
    }
}

my $confvars = {
    path => 'path',
    shared => 'bool',
    disable => 'bool',
    saferemove => 'bool',
    format => 'format',
    content => 'content',
    server => 'server',
    export => 'path',
    vgname => 'vgname',
    base   => 'volume',
    portal => 'portal',
    target => 'target',
    nodes => 'nodes',
    options => 'options',
    maxfiles => 'natural',
};

my $required_config = {
    dir => ['path'],
    nfs => ['path', 'server', 'export'],
    lvm => ['vgname'],
    iscsi => ['portal', 'target'],
};

my $fixed_config = {
    dir => ['path'],
    nfs => ['path', 'server', 'export'],
    lvm => ['vgname', 'base'],
    iscsi => ['portal', 'target'],
};

my $default_config = {
    dir => {
	path => 1,
        nodes => 0,
	shared => 0,
	disable => 0,
        maxfiles => 0, 
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1, none => 1 },
		     { images => 1,  rootdir => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    },

    nfs => {
	path => 1,
        nodes => 0,
	disable => 0,
        server => 1,
        export => 1,
        options => 0,
        maxfiles => 0, 
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1, backup => 1},
		     { images => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    },

    lvm => {
	vgname => 1,
        nodes => 0,
	shared => 0,
	disable => 0,
        saferemove => 0,
	content => [ {images => 1}, { images => 1 }],
        base => 1,
    },

    iscsi => {
        portal => 1,
        target => 1,
        nodes => 0,
	disable => 0,
	content => [ {images => 1, none => 1}, { images => 1 }],
    },
};

sub valid_content_types {
    my ($stype) = @_;

    my $def = $default_config->{$stype};

    return {} if !$def;

    return $def->{content}->[0];
}

sub content_hash_to_string {
    my $hash = shift;

    my @cta;
    foreach my $ct (keys %$hash) {
	push @cta, $ct if $hash->{$ct};
    } 

    return join(',', @cta);
}

PVE::JSONSchema::register_format('pve-storage-path', \&verify_path);
sub verify_path {
    my ($path, $noerr) = @_;

    # fixme: exclude more shell meta characters?
    # we need absolute paths
    if ($path !~ m|^/[^;\(\)]+|) {
	return undef if $noerr;
	die "value does not look like a valid absolute path\n";
    }
    return $path;
}

PVE::JSONSchema::register_format('pve-storage-server', \&verify_server);
sub verify_server {
    my ($server, $noerr) = @_;

    # fixme: use better regex ?
    # IP or DNS name
    if ($server !~ m/^[[:alnum:]\-\.]+$/) {
	return undef if $noerr;
	die "value does not look like a valid server name or IP address\n";
    }
    return $server;
}

PVE::JSONSchema::register_format('pve-storage-portal', \&verify_portal);
sub verify_portal {
    my ($portal, $noerr) = @_;

    # IP with optional port
    if ($portal !~ m/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?$/) {
	return undef if $noerr;
	die "value does not look like a valid portal address\n";
    }
    return $portal;
}

PVE::JSONSchema::register_format('pve-storage-portal-dns', \&verify_portal_dns);
sub verify_portal_dns {
    my ($portal, $noerr) = @_;

    # IP or DNS name with optional port
    if ($portal !~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[[:alnum:]\-\.]+)(:\d+)?$/) {
	return undef if $noerr;
	die "value does not look like a valid portal address\n";
    }
    return $portal;
}

PVE::JSONSchema::register_format('pve-storage-content', \&verify_content);
sub verify_content {
    my ($ct, $noerr) = @_;

    my $valid_content = valid_content_types('dir'); # dir includes all types
 
    if (!$valid_content->{$ct}) {
	return undef if $noerr;
	die "invalid content type '$ct'\n";
    }

    return $ct;
}

PVE::JSONSchema::register_format('pve-storage-format', \&verify_format);
sub verify_format {
    my ($fmt, $noerr) = @_;

    if ($fmt !~ m/(raw|qcow2|vmdk)/) {
	return undef if $noerr;
	die "invalid format '$fmt'\n";
    }

    return $fmt;
}

PVE::JSONSchema::register_format('pve-storage-options', \&verify_options);
sub verify_options {
    my ($value, $noerr) = @_;

    # mount options (see man fstab)
    if ($value !~ m/^\S+$/) {
	return undef if $noerr;
	die "invalid options '$value'\n";
    }

    return $value;
}

sub check_type {
    my ($stype, $ct, $key, $value, $storeid, $noerr) = @_;

    my $def = $default_config->{$stype};

    if (!$def) { # should not happen
	return undef if $noerr;	
	die "unknown storage type '$stype'\n"; 
    }

    if (!defined($def->{$key})) {
	return undef if $noerr;
	die "unexpected property\n";
    }

    if (!defined ($value)) {
	return undef if $noerr;
	die "got undefined value\n";
    }

    if ($value =~ m/[\n\r]/) {
	return undef if $noerr;
	die "property contains a line feed\n";
    }

    if ($ct eq 'bool') {
	return 1 if ($value eq '1') || ($value =~ m/^(on|yes|true)$/i); 
	return 0 if ($value eq '0') || ($value =~ m/^(off|no|false)$/i); 
	return undef if $noerr;
	die "type check ('boolean') failed - got '$value'\n";	
    } elsif ($ct eq 'options') {
	return verify_options($value, $noerr);
    } elsif ($ct eq 'path') {
	return verify_path($value, $noerr);
    } elsif ($ct eq 'server') {
	return verify_server($value, $noerr);
    } elsif ($ct eq 'vgname') {
	return parse_lvm_name ($value, $noerr);
    } elsif ($ct eq 'portal') {
	return verify_portal($value, $noerr);
    } elsif ($ct eq 'natural') {
	return int($value) if $value =~ m/^\d+$/; 
	return undef if $noerr;
	die "type check ('natural') failed - got '$value'\n";
    } elsif ($ct eq 'nodes') {
	my $res = {};

	foreach my $node (PVE::Tools::split_list($value)) {
	    if (PVE::JSONSchema::pve_verify_node_name($node, $noerr)) {
		$res->{$node} = 1;
	    }
	}

	# no node restrictions for local storage
	if ($storeid && $storeid eq 'local' && scalar(keys(%$res))) {
	    return undef if $noerr;
	    die "storage '$storeid' does not allow node restrictions\n";
	}

	return $res;
    } elsif ($ct eq 'target') {
	return $value;
    } elsif ($ct eq 'string') {
	return $value;
    } elsif ($ct eq 'format') {
	my $valid_formats = $def->{format}->[0];

	if (!$valid_formats->{$value}) {
	    return undef if $noerr;
	    die "storage does not support format '$value'\n";
	}

	return $value;

    } elsif ($ct eq 'content') {
	my $valid_content = $def->{content}->[0];
	
	my $res = {};

	foreach my $c (PVE::Tools::split_list($value)) {
	    if (!$valid_content->{$c}) {
		return undef if $noerr;
		die "storage does not support content type '$c'\n";
	    }
	    $res->{$c} = 1;
	} 

	if ($res->{none} && scalar (keys %$res) > 1) {
		return undef if $noerr;
		die "unable to combine 'none' with other content types\n";
	}

	return $res;	
    } elsif ($ct eq 'volume') {
	return $value if parse_volume_id ($value, $noerr);
    }

    return undef if $noerr;
    die "type check not implemented - internal error\n";
}

sub parse_config {
    my ($filename, $raw) = @_;

    my $ids = {};

    my $digest = Digest::SHA::sha1_hex(defined($raw) ? $raw : '');

    my $pri = 0;

    while ($raw && $raw =~ s/^(.*?)(\n|$)//) {
	my $line = $1;

	next if $line =~ m/^\#/;
	next if $line =~ m/^\s*$/;

	if ($line =~ m/^(\S+):\s*(\S+)\s*$/) {
	    my $storeid = $2;
	    my $type = $1;
	    my $ignore = 0;

	    if (!PVE::JSONSchema::parse_storage_id($storeid, 1)) {
		$ignore = 1;
		warn "ignoring storage '$storeid' - (illegal characters)\n";
	    } elsif (!$default_config->{$type}) {
		$ignore = 1;
		warn "ignoring storage '$storeid' (unsupported type '$type')\n";
	    } else {
		$ids->{$storeid}->{type} = $type;
		$ids->{$storeid}->{priority} = $pri++;
	    }

	    while ($raw && $raw =~ s/^(.*?)(\n|$)//) {
		$line = $1;

		next if $line =~ m/^\#/;
		last if $line =~ m/^\s*$/;

		next if $ignore; # skip

		if ($line =~ m/^\s+(\S+)(\s+(.*\S))?\s*$/) {
		    my ($k, $v) = ($1, $3);
		    if (my $ct = $confvars->{$k}) {
			$v = 1 if $ct eq 'bool' && !defined($v);
			eval {
			    $ids->{$storeid}->{$k} = check_type ($type, $ct, $k, $v, $storeid);
			};
			warn "storage '$storeid' - unable to parse value of '$k': $@" if $@;
		    } else {
			warn "storage '$storeid' - unable to parse value of '$k'\n";
		    }

		} else {
		    warn "storage '$storeid' - ignore config line: $line\n";
		}
	    }
	} else {
	    warn "ignore config line: $line\n";
	}
    }

    # make sure we have a reasonable 'local:' storage
    # openvz expects things to be there
    if (!$ids->{local} || $ids->{local}->{type} ne 'dir' ||
	$ids->{local}->{path} ne '/var/lib/vz') {
	$ids->{local} = {
	    type => 'dir',
	    priority => $pri++,
	    path => '/var/lib/vz',
	    maxfiles => 0,
	    content => { images => 1, rootdir => 1, vztmpl => 1, iso => 1},
	};
    }

    # we always need this for OpenVZ
    $ids->{local}->{content}->{rootdir} = 1;
    $ids->{local}->{content}->{vztmpl} = 1;
    delete ($ids->{local}->{disable});

    # remove node restrictions for local storage
    delete($ids->{local}->{nodes});

    foreach my $storeid (keys %$ids) {
	my $d = $ids->{$storeid};

	my $req_keys = $required_config->{$d->{type}};
	foreach my $k (@$req_keys) {
	    if (!defined ($d->{$k})) {
		warn "ignoring storage '$storeid' - missing value " .
		    "for required option '$k'\n";
		delete $ids->{$storeid};
		next;		
	    }
	}

	my $def = $default_config->{$d->{type}};

	if ($def->{content}) {
	    $d->{content} = $def->{content}->[1] if !$d->{content};
	}

	if ($d->{type} eq 'iscsi' || $d->{type} eq 'nfs') {
	    $d->{shared} = 1;
	}
    }

    my $cfg = { ids => $ids, digest => $digest};

    return $cfg;
}

sub parse_options {
    my ($storeid, $stype, $param, $create) = @_;

    my $settings = { type => $stype };

    die "unknown storage type '$stype'\n"
	if !$default_config->{$stype};

    foreach my $opt (keys %$param) {
	my $value = $param->{$opt};

	my $ct = $confvars->{$opt};
	if (defined($value)) {
	    eval {
		$settings->{$opt} = check_type ($stype, $ct, $opt, $value, $storeid);
	    };
	    raise_param_exc({ $opt => $@ }) if $@;
	} else {
	    raise_param_exc({ $opt => "got undefined value" });
	}
    }

    if ($create) {
	my $req_keys = $required_config->{$stype};
	foreach my $k (@$req_keys) {

	    if ($stype eq 'nfs' && !$settings->{path}) {
		$settings->{path} = "/mnt/pve/$storeid";
	    }

	    # check if we have a value for all required options
	    if (!defined ($settings->{$k})) {
		raise_param_exc({ $k => "property is missing and it is not optional" });
	    }
	}
    } else {
	my $fixed_keys = $fixed_config->{$stype};
	foreach my $k (@$fixed_keys) {

	    # only allow to change non-fixed values
	    
	    if (defined ($settings->{$k})) {
		raise_param_exc({$k => "can't change value (fixed parameter)"});
	    }
	}
    }

    return $settings;
}

sub cluster_lock_storage {
    my ($storeid, $shared, $timeout, $func, @param) = @_;

    my $res;
    if (!$shared) {
	my $lockid = "pve-storage-$storeid";
	my $lockdir = "/var/lock/pve-manager";
	mkdir $lockdir;
	$res = PVE::Tools::lock_file("$lockdir/$lockid", $timeout, $func, @param); 
	die $@ if $@;
    } else {
	$res = PVE::Cluster::cfs_lock_storage($storeid, $timeout, $func, @param);
	die $@ if $@;
    }	
    return $res;
}

sub storage_config {
    my ($cfg, $storeid, $noerr) = @_;

    die "no storage id specified\n" if !$storeid;
 
    my $scfg = $cfg->{ids}->{$storeid};

    die "storage '$storeid' does not exists\n" if (!$noerr && !$scfg);

    return $scfg;
}

sub storage_check_node {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config ($cfg, $storeid);

    if ($scfg->{nodes}) {
	$node = PVE::INotify::nodename() if !$node || ($node eq 'localhost');
	if (!$scfg->{nodes}->{$node}) {
	    die "storage '$storeid' is not available on node '$node'\n" if !$noerr;
	    return undef;
	}
    }

    return $scfg;
}

sub storage_check_enabled {
    my ($cfg, $storeid, $node, $noerr) = @_;

    my $scfg = storage_config ($cfg, $storeid);

    if ($scfg->{disable}) {
	die "storage '$storeid' is disabled\n" if !$noerr;
	return undef;
    }

    return storage_check_node($cfg, $storeid, $node, $noerr);
}

sub storage_ids {
    my ($cfg) = @_;

    my $ids = $cfg->{ids};

    my @sa = sort {$ids->{$a}->{priority} <=> $ids->{$b}->{priority}} keys %$ids;

    return @sa;
}

sub assert_if_modified {
    my ($cfg, $digest) = @_;

    if ($digest && ($cfg->{digest} ne $digest)) {
	die "detected modified storage configuration - try again\n";
    }
}

sub sprint_config_line {
    my ($k, $v) = @_;

    my $ct = $confvars->{$k};

    if ($ct eq 'bool') {
	return $v ? "\t$k\n" : '';
    } elsif ($ct eq 'nodes') {
	my $nlist = join(',', keys(%$v));
	return $nlist ? "\tnodes $nlist\n" : ''; 
    } elsif ($ct eq 'content') {
	my $clist = content_hash_to_string($v);
	if ($clist) {
	    return "\t$k $clist\n";
	} else {
	    return "\t$k none\n";
	}
    } else {
	return "\t$k $v\n";
    }
}

sub write_config {
    my ($filename, $cfg) = @_;

    my $out = '';

    my $ids = $cfg->{ids};

    my $maxpri = 0;
    foreach my $storeid (keys %$ids) {
	my $pri = $ids->{$storeid}->{priority}; 
	$maxpri = $pri if $pri && $pri > $maxpri;
    }
    foreach my $storeid (keys %$ids) {
	if (!defined ($ids->{$storeid}->{priority})) {
	    $ids->{$storeid}->{priority} = ++$maxpri;
	} 
    }

    foreach my $storeid (sort {$ids->{$a}->{priority} <=> $ids->{$b}->{priority}} keys %$ids) {
	my $scfg = $ids->{$storeid};
	my $type = $scfg->{type};
	my $def = $default_config->{$type};

	die "unknown storage type '$type'\n" if !$def;

	my $data = "$type: $storeid\n";

	$data .= "\tdisable\n" if $scfg->{disable};

	my $done_hash = { disable => 1};
	foreach my $k (@{$required_config->{$type}}) {
	    $done_hash->{$k} = 1;
	    my $v =  $ids->{$storeid}->{$k};
	    die "storage '$storeid' - missing value for required option '$k'\n"
		if !defined ($v);
	    $data .= sprint_config_line ($k, $v);
	}

	foreach my $k (keys %$def) {
	    next if defined ($done_hash->{$k});
	    my $v = $ids->{$storeid}->{$k};
	    next if !defined($v);
	    $data .= sprint_config_line ($k, $v);
	}

	$out .= "$data\n";
    }

    return $out;
}

sub get_image_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $path = $cfg->{ids}->{$storeid}->{path};
    return $vmid ? "$path/images/$vmid" : "$path/images";
}

sub get_private_dir {
    my ($cfg, $storeid, $vmid) = @_;

    my $path = $cfg->{ids}->{$storeid}->{path};
    return $vmid ? "$path/private/$vmid" : "$path/private";
}

sub get_iso_dir {
    my ($cfg, $storeid) = @_;

    my $isodir =  $cfg->{ids}->{$storeid}->{path};
    $isodir .= '/template/iso';

    return $isodir;
}

sub get_vztmpl_dir {
    my ($cfg, $storeid) = @_;

    my $tmpldir =  $cfg->{ids}->{$storeid}->{path};
    $tmpldir .= '/template/cache';

    return $tmpldir;
}

sub get_backup_dir {
    my ($cfg, $storeid) = @_;

    my $dir =  $cfg->{ids}->{$storeid}->{path};
    $dir .= '/dump';

    return $dir;
}

# iscsi utility functions

sub iscsi_session_list {

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'session'];

    my $res = {};

    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~ m/^tcp:\s+\[(\S+)\]\s+\S+\s+(\S+)\s*$/) {
	    my ($session, $target) = ($1, $2);
	    # there can be several sessions per target (multipath)
	    push @{$res->{$target}}, $session;

	}
    });

    return $res;
}

sub iscsi_test_portal {
    my ($portal) = @_;

    my ($server, $port) = split(':', $portal);
    my $p = Net::Ping->new("tcp", 2);
    $p->port_number($port || 3260);
    return $p->ping($server);
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

	if ($line =~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)\,\S+\s+(\S+)\s*$/) {
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

    check_iscsi_support ();

    my $rstat = stat ($rescan_filename);

    if (!$rstat) {
	if (my $fh = IO::File->new ($rescan_filename, "a")) {
	    utime undef, undef, $fh;
	    close ($fh);
	}
    } else {
	my $atime = $rstat->atime;
	my $tdiff = time() - $atime;
	# avoid frequent rescans
	return if !($tdiff < 0 || $tdiff > 10);
	utime undef, undef, $rescan_filename;
    }

    foreach my $session (@$session_list) {
	my $cmd = [$ISCSIADM, '--mode', 'session', '-r', $session, '-R'];
	eval { run_command($cmd, outfunc => sub {}); };
	warn $@ if $@;
    }
}

sub iscsi_device_list {

    my $res = {};

    my $dirname = '/sys/class/iscsi_session';

    my $stable_paths = load_stable_scsi_paths();

    dir_glob_foreach ($dirname, 'session(\d+)', sub {
	my ($ent, $session) = @_;

	my $target = file_read_firstline ("$dirname/$ent/targetname");
	return if !$target;

	my (undef, $host) = dir_glob_regex ("$dirname/$ent/device", 'target(\d+):.*');
	return if !defined($host);

	dir_glob_foreach ("/sys/bus/scsi/devices", "$host:" . '(\d+):(\d+):(\d+)', sub {
	    my ($tmp, $channel, $id, $lun) = @_;

	    my $type = file_read_firstline ("/sys/bus/scsi/devices/$tmp/type");
	    return if !defined($type) || $type ne '0'; # list disks only

	    my $bdev;
	    if (-d "/sys/bus/scsi/devices/$tmp/block") { # newer kernels
		(undef, $bdev) = dir_glob_regex ("/sys/bus/scsi/devices/$tmp/block/", '([A-Za-z]\S*)');
	    } else {
		(undef, $bdev) = dir_glob_regex ("/sys/bus/scsi/devices/$tmp", 'block:(\S+)');
	    }
	    return if !$bdev;

	    #check multipath           
	    if (-d "/sys/block/$bdev/holders") { 
		my $multipathdev = dir_glob_regex ("/sys/block/$bdev/holders", '[A-Za-z]\S*');
		$bdev = $multipathdev if $multipathdev;
	    }

	    my $blockdev = $stable_paths->{$bdev};
	    return if !$blockdev;

	    my $size = file_read_firstline ("/sys/block/$bdev/size");
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

# library implementation

PVE::JSONSchema::register_format('pve-storage-vgname', \&parse_lvm_name);
sub parse_lvm_name {
    my ($name, $noerr) = @_;

    if ($name !~ m/^[a-z][a-z0-9\-\_\.]*[a-z0-9]$/i) {
	return undef if $noerr;
	die "lvm name '$name' contains illegal characters\n";
    }

    return $name;
}

sub parse_vmid {
    my $vmid = shift;

    die "VMID '$vmid' contains illegal characters\n" if $vmid !~ m/^\d+$/;

    return int($vmid);
}

PVE::JSONSchema::register_format('pve-volume-id', \&parse_volume_id);
sub parse_volume_id {
    my ($volid, $noerr) = @_;

    if ($volid =~ m/^([a-z][a-z0-9\-\_\.]*[a-z0-9]):(.+)$/i) {
	return wantarray ? ($1, $2) : $1;
    }
    return undef if $noerr;
    die "unable to parse volume ID '$volid'\n";
}

sub parse_name_dir {
    my $name = shift;

    if ($name =~ m!^([^/\s]+\.(raw|qcow2|vmdk))$!) {
	return ($1, $2);
    }

    die "unable to parse volume filename '$name'\n";
}

sub parse_volname_dir {
    my $volname = shift;

    if ($volname =~ m!^(\d+)/(\S+)$!) {
	my ($vmid, $name) = ($1, $2);
	parse_name_dir ($name);
	return ('image', $name, $vmid);
    } elsif ($volname =~ m!^iso/([^/]+\.[Ii][Ss][Oo])$!) {
	return ('iso', $1);
    } elsif ($volname =~ m!^vztmpl/([^/]+\.tar\.gz)$!) {
	return ('vztmpl', $1);
    } elsif ($volname =~ m!^rootdir/(\d+)$!) {
	return ('rootdir', $1, $1);
    } elsif ($volname =~ m!^backup/([^/]+(\.(tar|tar\.gz|tar\.lzo|tgz)))$!) {
	my $fn = $1;
	if ($fn =~ m/^vzdump-(openvz|qemu)-(\d+)-.+/) {
	    return ('backup', $fn, $2);
	}
	return ('backup', $fn);
   }
    die "unable to parse directory volume name '$volname'\n";
}

sub parse_volname_lvm {
    my $volname = shift;

    parse_lvm_name ($volname);

    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
	return ($1, $2);
    }

    die "unable to parse lvm volume name '$volname'\n";    
}

sub parse_volname_iscsi {
    my $volname = shift;

    if ($volname =~ m!^\d+\.\d+\.\d+\.(\S+)$!) {
	my $byid = $1;
	return $byid;
    }

    die "unable to parse iscsi volume name '$volname'\n";
}

# try to map a filesystem path to a volume identifier
sub path_to_volume_id {
    my ($cfg, $path) = @_;

    my $ids = $cfg->{ids};

    my ($sid, $volname) = parse_volume_id ($path, 1);
    if ($sid) {
	if ($ids->{$sid} && (my $type = $ids->{$sid}->{type})) {
	    if ($type eq 'dir' || $type eq 'nfs') {
		my ($vtype, $name, $vmid) = parse_volname_dir ($volname);
		return ($vtype, $path);
	    }
	}
	return ('');
    }

    # Note: abs_path() return undef if $path doesn not exist 
    # for example when nfs storage is not mounted
    $path = abs_path($path) || $path;

    foreach my $sid (keys %$ids) {
	my $type = $ids->{$sid}->{type};
	next if !($type eq 'dir' || $type eq 'nfs');
	
	my $imagedir = get_image_dir($cfg, $sid);
	my $isodir = get_iso_dir($cfg, $sid);
	my $tmpldir = get_vztmpl_dir($cfg, $sid);
	my $backupdir = get_backup_dir($cfg, $sid);
	my $privatedir = get_private_dir($cfg, $sid);

	if ($path =~ m!^$imagedir/(\d+)/([^/\s]+)$!) {
	    my $vmid = $1;
	    my $name = $2;
	    return ('image', "$sid:$vmid/$name");
	} elsif ($path =~ m!^$isodir/([^/]+\.[Ii][Ss][Oo])$!) {
	    my $name = $1;
	    return ('iso', "$sid:iso/$name");	
	} elsif ($path =~ m!^$tmpldir/([^/]+\.tar\.gz)$!) {
	    my $name = $1;
	    return ('vztmpl', "$sid:vztmpl/$name");
	} elsif ($path =~ m!^$privatedir/(\d+)$!) {
	    my $vmid = $1;
	    return ('rootdir', "$sid:rootdir/$vmid");
	} elsif ($path =~ m!^$backupdir/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz))$!) {
	    my $name = $1;
	    return ('iso', "$sid:backup/$name");	
	}
    }

    # can't map path to volume id
    return ('');
}

sub path {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id ($volid);

    my $scfg = storage_config ($cfg, $storeid);

    my $path;
    my $owner;
    my $vtype = 'image';

    if ($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs') {
	my ($name, $vmid);
	($vtype, $name, $vmid) = parse_volname_dir ($volname);
	$owner = $vmid;

	my $imagedir = get_image_dir($cfg, $storeid, $vmid);
	my $isodir = get_iso_dir($cfg, $storeid);
	my $tmpldir = get_vztmpl_dir($cfg, $storeid);
	my $backupdir = get_backup_dir($cfg, $storeid);
	my $privatedir = get_private_dir($cfg, $storeid);

	if ($vtype eq 'image') {
	    $path = "$imagedir/$name";
	} elsif ($vtype eq 'iso') {
	    $path = "$isodir/$name";
	} elsif ($vtype eq 'vztmpl') {
	    $path = "$tmpldir/$name";
	} elsif ($vtype eq 'rootdir') {
	    $path = "$privatedir/$name";
	} elsif ($vtype eq 'backup') {
	    $path = "$backupdir/$name";
	} else {
	    die "should not be reached";
	}

    } elsif ($scfg->{type} eq 'lvm') {

	my $vg = $scfg->{vgname};

	my ($name, $vmid) = parse_volname_lvm ($volname);
	$owner = $vmid;

	$path = "/dev/$vg/$name";

    } elsif ($scfg->{type} eq 'iscsi') {
	my $byid = parse_volname_iscsi ($volname);
	$path = "/dev/disk/by-id/$byid";
    } else {
	die "unknown storage type '$scfg->{type}'";
    }

    return wantarray ? ($path, $owner, $vtype) : $path;
}

sub storage_migrate {
    my ($cfg, $volid, $target_host, $target_storeid, $target_volname) = @_;

    my ($storeid, $volname) = parse_volume_id ($volid);
    $target_volname = $volname if !$target_volname;

    my $scfg = storage_config ($cfg, $storeid);

    # no need to migrate shared content
    return if $storeid eq $target_storeid && $scfg->{shared};

    my $tcfg = storage_config ($cfg, $target_storeid);

    my $target_volid = "${target_storeid}:${target_volname}";

    my $errstr = "unable to migrate '$volid' to '${target_volid}' on host '$target_host'";

    # blowfish is a fast block cipher, much faster then 3des
    my $sshoptions = "-c blowfish -o 'BatchMode=yes'";
    my $ssh = "/usr/bin/ssh $sshoptions";

    local $ENV{RSYNC_RSH} = $ssh;

    if ($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs') {
	if ($tcfg->{type} eq 'dir' || $tcfg->{type} eq 'nfs') {

	    my $src = path ($cfg, $volid);
	    my $dst = path ($cfg, $target_volid);

	    my $dirname = dirname ($dst);

	    if ($tcfg->{shared}) { # we can do a local copy
		
		run_command(['/bin/mkdir', '-p', $dirname]);

		run_command(['/bin/cp', $src, $dst]);

	    } else {

		run_command(['/usr/bin/ssh', "root\@${target_host}", 
			     '/bin/mkdir', '-p', $dirname]);

		# we use rsync with --sparse, so we can't use --inplace,
		# so we remove file on the target if it already exists to
		# save space
		my ($size, $format) = file_size_info($src);
		if ($format && ($format eq 'raw') && $size) {
		    run_command(['/usr/bin/ssh', "root\@${target_host}", 
				 'rm', '-f', $dst],
				outfunc => sub {});
		}

		my $cmd = ['/usr/bin/rsync', '--progress', '--sparse', '--whole-file', 
			   $src, "root\@${target_host}:$dst"];

		my $percent = -1;

		run_command($cmd, outfunc => sub {
		    my $line = shift;

		    if ($line =~ m/^\s*(\d+\s+(\d+)%\s.*)$/) {
			if ($2 > $percent) {
			    $percent = $2;
			    print "rsync status: $1\n";
			    *STDOUT->flush();
			}
		    } else {
			print "$line\n";
			*STDOUT->flush();
		    }
		});
	    }


	} else {

	    die "$errstr - target type '$tcfg->{type}' not implemented\n";
	}

    } else {
	die "$errstr - source type '$scfg->{type}' not implemented\n";
    }
}

sub vdisk_alloc {
    my ($cfg, $storeid, $vmid, $fmt, $name, $size) = @_;

    die "no storage id specified\n" if !$storeid;

    PVE::JSONSchema::parse_storage_id($storeid);

    my $scfg = storage_config($cfg, $storeid);

    die "no VMID specified\n" if !$vmid;

    $vmid = parse_vmid ($vmid);

    my $defformat = storage_default_format ($cfg, $storeid);

    $fmt = $defformat if !$fmt;

    activate_storage ($cfg, $storeid);

    # lock shared storage
    return cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {

	if ($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs') {

	    my $imagedir = get_image_dir ($cfg, $storeid, $vmid);

	    mkpath $imagedir;

	    if (!$name) {

		for (my $i = 1; $i < 100; $i++) {
		    my @gr = <$imagedir/vm-$vmid-disk-$i.*>;
		    if (!scalar(@gr)) {
			$name = "vm-$vmid-disk-$i.$fmt";
			last;
		    }
		}
	    }

	    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
		if !$name;

	    my (undef, $tmpfmt) = parse_name_dir ($name);

	    die "illegal name '$name' - wrong extension for format ('$tmpfmt != '$fmt')\n" 
		if $tmpfmt ne $fmt;

	    my $path = "$imagedir/$name";

	    die "disk image '$path' already exists\n" if -e $path;

	    run_command("/usr/bin/qemu-img create -f $fmt '$path' ${size}K", 
			errmsg => "unable to create image");

	    return "$storeid:$vmid/$name";

	} elsif ($scfg->{type} eq 'lvm') {
	
	    die "unsupported format '$fmt'" if $fmt ne 'raw';

	    die "illegal name '$name' - sould be 'vm-$vmid-*'\n" 
		if  $name && $name !~ m/^vm-$vmid-/;

	    my $vgs = lvm_vgs ();

	    my $vg = $scfg->{vgname};

	    die "no such volume gruoup '$vg'\n" if !defined ($vgs->{$vg});

	    my $free = int ($vgs->{$vg}->{free});

	    die "not enough free space ($free < $size)\n" if $free < $size;

	    if (!$name) {
		my $lvs = lvm_lvs ($vg);

		for (my $i = 1; $i < 100; $i++) {
		    my $tn = "vm-$vmid-disk-$i";
		    if (!defined ($lvs->{$vg}->{$tn})) {
			$name = $tn;
			last;
		    }
		}
	    }

	    die "unable to allocate an image name for VM $vmid in storage '$storeid'\n"
		if !$name;

	    my $cmd = ['/sbin/lvcreate', '-aly', '--addtag', "pve-vm-$vmid", '--size', "${size}k", '--name', $name, $vg];

	    run_command($cmd, errmsg => "lvcreate '$vg/pve-vm-$vmid' error");

	    return "$storeid:$name";

	} elsif ($scfg->{type} eq 'iscsi') {
	    die "can't allocate space in iscsi storage\n";
	} else {
	    die "unknown storage type '$scfg->{type}'";
	}
    });
}

sub vdisk_free {
    my ($cfg, $volid) = @_;

    my ($storeid, $volname) = parse_volume_id ($volid);

    my $scfg = storage_config ($cfg, $storeid);

    activate_storage ($cfg, $storeid);

    # we need to zero out LVM data for security reasons
    # and to allow thin provisioning

    my $vg;

    # lock shared storage
    cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {

	if ($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs') {
	    my $path = path ($cfg, $volid); 

	    if (! -f $path) {
		warn "disk image '$path' does not exists\n";
	    } else {
		unlink $path;
	    }
	} elsif ($scfg->{type} eq 'lvm') {

	    if ($scfg->{saferemove}) {
		# avoid long running task, so we only rename here
		$vg = $scfg->{vgname};
		my $cmd = ['/sbin/lvrename', $vg, $volname, "del-$volname"];
		run_command($cmd, errmsg => "lvrename '$vg/$volname' error");
	    } else {
		my $tmpvg = $scfg->{vgname};
		my $cmd = ['/sbin/lvremove', '-f', "$tmpvg/$volname"];
		run_command($cmd, errmsg => "lvremove '$tmpvg/$volname' error");
	    }

	} elsif ($scfg->{type} eq 'iscsi') {
	    die "can't free space in iscsi storage\n";
	} else {
	    die "unknown storage type '$scfg->{type}'";
	}
    });

    return if !$vg;

    my $zero_out_worker = sub {
	print "zero-out data on image $volname\n";
	my $cmd = ['dd', "if=/dev/zero", "of=/dev/$vg/del-$volname", "bs=1M"];
	eval { run_command($cmd, errmsg => "zero out failed"); };
	warn $@ if $@;

	cluster_lock_storage($storeid, $scfg->{shared}, undef, sub {
	    my $cmd = ['/sbin/lvremove', '-f', "$vg/del-$volname"];
	    run_command($cmd, errmsg => "lvremove '$vg/del-$volname' error");
	});
	print "successfully removed volume $volname\n";
    };

    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();

    $rpcenv->fork_worker('imgdel', undef, $authuser, $zero_out_worker);
}

# lvm utility functions

sub lvm_pv_info {
    my ($device) = @_;

    die "no device specified" if !$device;

    my $has_label = 0;

    my $cmd = ['/usr/bin/file', '-L', '-s', $device];
    run_command($cmd, outfunc => sub {
	my $line = shift;
	$has_label = 1 if $line =~ m/LVM2/;
    });

    return undef if !$has_label;

    $cmd = ['/sbin/pvs', '--separator', ':', '--noheadings', '--units', 'k',
	    '--unbuffered', '--nosuffix', '--options',
	    'pv_name,pv_size,vg_name,pv_uuid', $device];

    my $pvinfo;
    run_command($cmd, outfunc => sub {
	my $line = shift;

	$line = trim($line);

	my ($pvname, $size, $vgname, $uuid) = split (':', $line);

	die "found multiple pvs entries for device '$device'\n" 
	    if $pvinfo;

	$pvinfo = {
	    pvname => $pvname,
	    size => $size,
	    vgname => $vgname,
	    uuid => $uuid,
	};
    });

    return $pvinfo;
}

sub clear_first_sector {
    my ($dev) = shift;

    if (my $fh = IO::File->new ($dev, "w")) {
	my $buf = 0 x 512;
	syswrite $fh, $buf;
	$fh->close();	
    }
}

sub lvm_create_volume_group {
    my ($device, $vgname, $shared) = @_;
    
    my $res = lvm_pv_info ($device);
    
    if ($res->{vgname}) {
	return if $res->{vgname} eq $vgname; # already created
	die "device '$device' is already used by volume group '$res->{vgname}'\n";
    }

    clear_first_sector ($device); # else pvcreate fails

    # we use --metadatasize 250k, which reseults in "pe_start = 512"
    # so pe_start is aligned on a 128k boundary (advantage for SSDs)
    my $cmd = ['/sbin/pvcreate', '--metadatasize', '250k', $device];

    run_command($cmd, errmsg => "pvcreate '$device' error");

    $cmd = ['/sbin/vgcreate', $vgname, $device];
    # push @$cmd, '-c', 'y' if $shared; # we do not use this yet

    run_command($cmd, errmsg => "vgcreate $vgname $device error");
}

sub lvm_vgs {

    my $cmd = ['/sbin/vgs', '--separator', ':', '--noheadings', '--units', 'b',
	       '--unbuffered', '--nosuffix', '--options',
	       'vg_name,vg_size,vg_free'];

    my $vgs = {};
    eval {
	run_command($cmd, outfunc => sub {
	    my $line = shift;

	    $line = trim($line);

	    my ($name, $size, $free) = split (':', $line);

	    $vgs->{$name} = { size => int ($size), free => int ($free) };
        });
    };
    my $err = $@;

    # just warn (vgs return error code 5 if clvmd does not run)
    # but output is still OK (list without clustered VGs)
    warn $err if $err;

    return $vgs;
}

sub lvm_lvs {
    my ($vgname) = @_;

    my $cmd = ['/sbin/lvs', '--separator', ':', '--noheadings', '--units', 'b',
	       '--unbuffered', '--nosuffix', '--options',
	       'vg_name,lv_name,lv_size,uuid,tags'];

    push @$cmd, $vgname if $vgname;

    my $lvs = {};
    run_command($cmd, outfunc => sub {
	my $line = shift;

	$line = trim($line);

	my ($vg, $name, $size, $uuid, $tags) = split (':', $line);

	return if $name !~ m/^vm-(\d+)-/;
	my $nid = $1;

	my $owner;
	foreach my $tag (split (/,/, $tags)) {
	    if ($tag =~ m/^pve-vm-(\d+)$/) {
		$owner = $1;
		last;
	    }
	}
	
	if ($owner) {
	    if ($owner ne $nid) {
		warn "owner mismatch name = $name, owner = $owner\n";
	    }
   
	    $lvs->{$vg}->{$name} = { format => 'raw', size => $size, 
				     uuid => $uuid,  tags => $tags, 
				     vmid => $owner };
	}
    });

    return $lvs;
}

#list iso or openvz template ($tt = <iso|vztmpl|backup>)
sub template_list {
    my ($cfg, $storeid, $tt) = @_;

    die "unknown template type '$tt'\n" if !($tt eq 'iso' || $tt eq 'vztmpl' || $tt eq 'backup'); 

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # query the storage

    foreach my $sid (keys %$ids) {
	next if $storeid && $storeid ne $sid;

	my $scfg = $ids->{$sid};
	my $type = $scfg->{type};

	next if !storage_check_enabled($cfg, $sid, undef, 1);

	next if $tt eq 'iso' && !$scfg->{content}->{iso};
	next if $tt eq 'vztmpl' && !$scfg->{content}->{vztmpl};
	next if $tt eq 'backup' && !$scfg->{content}->{backup};

	activate_storage ($cfg, $sid);

	if ($type eq 'dir' || $type eq 'nfs') {

	    my $path;
	    if ($tt eq 'iso') {
		$path = get_iso_dir($cfg, $sid);
	    } elsif ($tt eq 'vztmpl') {
		$path = get_vztmpl_dir($cfg, $sid);
	    } elsif ($tt eq 'backup') {
		$path = get_backup_dir($cfg, $sid);
	    } else {
		die "unknown template type '$tt'\n";
	    }

	    foreach my $fn (<$path/*>) {

		my $info;

		if ($tt eq 'iso') {
		    next if $fn !~ m!/([^/]+\.[Ii][Ss][Oo])$!;

		    $info = { volid => "$sid:iso/$1", format => 'iso' };

		} elsif ($tt eq 'vztmpl') {
		    next if $fn !~ m!/([^/]+\.tar\.gz)$!;

		    $info = { volid => "$sid:vztmpl/$1", format => 'tgz' };

		} elsif ($tt eq 'backup') {
		    next if $fn !~ m!/([^/]+\.(tar|tar\.gz|tar\.lzo|tgz))$!;
		    
		    $info = { volid => "$sid:backup/$1", format => $2 };
		}

		$info->{size} = -s $fn;

		push @{$res->{$sid}}, $info;
	    }

	}

	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}

sub file_size_info {
    my ($filename, $timeout) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'info', $filename];

    my $format;
    my $size = 0;
    my $used = 0;

    eval {
	run_command($cmd, timeout => $timeout, outfunc => sub {
	    my $line = shift;

	    if ($line =~ m/^file format:\s+(\S+)\s*$/) {
		$format = $1;
	    } elsif ($line =~ m/^virtual size:\s\S+\s+\((\d+)\s+bytes\)$/) {
		$size = int($1);
	    } elsif ($line =~ m/^disk size:\s+(\d+(.\d+)?)([KMGT])\s*$/) {
		$used = $1;
		my $u = $3;

		$used *= 1024 if $u eq 'K';
		$used *= (1024*1024) if $u eq 'M';
		$used *= (1024*1024*1024) if $u eq 'G';
		$used *= (1024*1024*1024*1024) if $u eq 'T';

		$used = int($used);
	    }
	});
    };

    return wantarray ? ($size, $format, $used) : $size;
}

sub vdisk_list {
    my ($cfg, $storeid, $vmid, $vollist) = @_;

    my $ids = $cfg->{ids};

    storage_check_enabled($cfg, $storeid) if ($storeid);

    my $res = {};

    # prepare/activate/refresh all storages

    my $stypes = {};

    my $storage_list = [];
    if ($vollist) {
	foreach my $volid (@$vollist) {
	    my ($sid, undef) = parse_volume_id ($volid);
	    next if !defined ($ids->{$sid});
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    push @$storage_list, $sid;
	    $stypes->{$ids->{$sid}->{type}} = 1;
	}
    } else {
	foreach my $sid (keys %$ids) {
	    next if $storeid && $storeid ne $sid;
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	    push @$storage_list, $sid;
	    $stypes->{$ids->{$sid}->{type}} = 1;
	}
    }

    activate_storage_list ($cfg, $storage_list);

    my $lvs = $stypes->{lvm} ? lvm_lvs () : {};

    my $iscsi_devices = iscsi_device_list() if $stypes->{iscsi};

    # query the storage

    foreach my $sid (keys %$ids) {
	if ($storeid) {
	    next if $storeid ne $sid;
	    next if !storage_check_enabled($cfg, $sid, undef, 1);
	}
	my $scfg = $ids->{$sid};
	my $type = $scfg->{type};

	if ($type eq 'dir' || $type eq 'nfs') {

	    my $path = $scfg->{path};

	    my $fmts = join ('|', keys %{$default_config->{$type}->{format}->[0]}); 

	    foreach my $fn (<$path/images/[0-9][0-9]*/*>) {

		next if $fn !~ m!^(/.+/images/(\d+)/([^/]+\.($fmts)))$!;
		$fn = $1; # untaint

		my $owner = $2;
		my $name = $3;
		my $volid = "$sid:$owner/$name";

		if ($vollist) {
		    my $found = grep { $_ eq $volid } @$vollist;
		    next if !$found;
		} else {
		    next if defined ($vmid) && ($owner ne $vmid);
		}

		my ($size, $format, $used) = file_size_info ($fn);

		if ($format && $size) {
		    push @{$res->{$sid}}, { 
			volid => $volid, format => $format,
			size => $size, vmid => $owner, used => $used };
		}

	    }

	} elsif ($type eq 'lvm') {

	    my $vgname = $scfg->{vgname};

	    if (my $dat = $lvs->{$vgname}) {

		foreach my $volname (keys %$dat) {

		    my $owner = $dat->{$volname}->{vmid};

		    my $volid = "$sid:$volname";

		    if ($vollist) {
			my $found = grep { $_ eq $volid } @$vollist;
			next if !$found;
		    } else {
			next if defined ($vmid) && ($owner ne $vmid);
		    }

		    my $info = $dat->{$volname};
		    $info->{volid} = $volid;

		    push @{$res->{$sid}}, $info;
		}
	    }

	} elsif ($type eq 'iscsi') {

	    # we have no owner for iscsi devices

	    my $target = $scfg->{target};

	    if (my $dat = $iscsi_devices->{$target}) {

		foreach my $volname (keys %$dat) {

		    my $volid = "$sid:$volname";

		    if ($vollist) {
			my $found = grep { $_ eq $volid } @$vollist;
			next if !$found;
		    } else {
			next if !($storeid && ($storeid eq $sid));
		    }

		    my $info = $dat->{$volname};
		    $info->{volid} = $volid;

		    push @{$res->{$sid}}, $info;
		}
	    }

	} else {
	    die "implement me";
	}

	@{$res->{$sid}} = sort {lc($a->{volid}) cmp lc ($b->{volid}) } @{$res->{$sid}} if $res->{$sid};
    }

    return $res;
}

sub nfs_is_mounted {
    my ($server, $export, $mountpoint, $mountdata) = @_;

    my $source = "$server:$export";

    $mountdata = read_proc_mounts() if !$mountdata;

    if ($mountdata =~ m|^$source/?\s$mountpoint\snfs|m) {
	return $mountpoint;
    } 

    return undef;
}

sub nfs_mount {
    my ($server, $export, $mountpoint, $options) = @_;

    my $source = "$server:$export";

    my $cmd = ['/bin/mount', '-t', 'nfs', $source, $mountpoint];
    if ($options) {
	push @$cmd, '-o', $options;
    } 

    run_command($cmd, errmsg => "mount error");
}

sub uevent_seqnum {

    my $filename = "/sys/kernel/uevent_seqnum";

    my $seqnum = 0;
    if (my $fh = IO::File->new ($filename, "r")) {
	my $line = <$fh>;
	if ($line =~ m/^(\d+)$/) {
	    $seqnum = int ($1);
	}
	close ($fh);
    }
    return $seqnum;
}

sub __activate_storage_full {
    my ($cfg, $storeid, $session) = @_;

    my $scfg = storage_check_enabled($cfg, $storeid);

    return if $session->{activated}->{$storeid};

    if (!$session->{mountdata}) {
	$session->{mountdata} = read_proc_mounts();
    }

    if (!$session->{uevent_seqnum}) {
	$session->{uevent_seqnum} = uevent_seqnum ();
    }

    my $mountdata = $session->{mountdata};

    my $type = $scfg->{type};

    if ($type eq 'dir' || $type eq 'nfs') {

	my $path = $scfg->{path};

	if ($type eq 'nfs') {
	    my $server = $scfg->{server};
	    my $export = $scfg->{export};

	    if (!nfs_is_mounted ($server, $export, $path, $mountdata)) {    
		    
		# NOTE: only call mkpath when not mounted (avoid hang 
		# when NFS server is offline 
		    
		mkpath $path;

		die "unable to activate storage '$storeid' - " .
		    "directory '$path' does not exist\n" if ! -d $path;

		nfs_mount ($server, $export, $path, $scfg->{options});
	    }

	} else {

	    mkpath $path;

	    die "unable to activate storage '$storeid' - " .
		"directory '$path' does not exist\n" if ! -d $path;
	}

	my $imagedir = get_image_dir($cfg, $storeid);
	my $isodir = get_iso_dir($cfg, $storeid);
	my $tmpldir = get_vztmpl_dir($cfg, $storeid);
	my $backupdir = get_backup_dir($cfg, $storeid);
	my $privatedir = get_private_dir($cfg, $storeid);

	if (defined($scfg->{content})) {
	    mkpath $imagedir if $scfg->{content}->{images} &&
		$imagedir ne $path;
	    mkpath $isodir if $scfg->{content}->{iso} &&
		$isodir ne $path;
	    mkpath $tmpldir if $scfg->{content}->{vztmpl} &&
		$tmpldir ne $path;
	    mkpath $privatedir if $scfg->{content}->{rootdir} &&
		$privatedir ne $path;
	    mkpath $backupdir if $scfg->{content}->{backup} &&
		$backupdir ne $path;
	}

    } elsif ($type eq 'lvm') {

	if ($scfg->{base}) {
	    my ($baseid, undef) = parse_volume_id ($scfg->{base});
	    __activate_storage_full ($cfg, $baseid, $session);
	}

	if (!$session->{vgs}) {
	    $session->{vgs} = lvm_vgs();
	}

	# In LVM2, vgscans take place automatically;
	# this is just to be sure
	if ($session->{vgs} && !$session->{vgscaned} && 
	    !$session->{vgs}->{$scfg->{vgname}}) {
	    $session->{vgscaned} = 1;
	    my $cmd = ['/sbin/vgscan', '--ignorelockingfailure', '--mknodes'];
	    eval { run_command($cmd, outfunc => sub {}); };
	    warn $@ if $@;
	}

	# we do not acticate any volumes here ('vgchange -aly')
	# instead, volumes are activate individually later

    } elsif ($type eq 'iscsi') {

	return if !check_iscsi_support(1);

	$session->{iscsi_sessions} = iscsi_session_list()
	    if !$session->{iscsi_sessions};

	my $iscsi_sess = $session->{iscsi_sessions}->{$scfg->{target}};
	if (!defined ($iscsi_sess)) {
	    eval { iscsi_login ($scfg->{target}, $scfg->{portal}); };
	    warn $@ if $@;
	} else {
	    # make sure we get all devices
	    iscsi_session_rescan ($iscsi_sess);
	}

    } else {
	die "implement me";
    }

    my $newseq = uevent_seqnum ();

    # only call udevsettle if there are events
    if ($newseq > $session->{uevent_seqnum}) {
	my $timeout = 30;
	system ("$UDEVADM settle --timeout=$timeout"); # ignore errors
	$session->{uevent_seqnum} = $newseq;
    }

    $session->{activated}->{$storeid} = 1;
}

sub activate_storage_list {
    my ($cfg, $storeid_list, $session) = @_;

    $session = {} if !$session;

    foreach my $storeid (@$storeid_list) {
	__activate_storage_full ($cfg, $storeid, $session);
    }
}

sub activate_storage {
    my ($cfg, $storeid) = @_;

    my $session = {};

    __activate_storage_full ($cfg, $storeid, $session);
}

sub activate_volumes {
    my ($cfg, $vollist, $exclusive) = @_;

    return if !($vollist && scalar(@$vollist));

    my $lvm_activate_mode = $exclusive ? 'ey' : 'ly';

    my $storagehash = {};
    foreach my $volid (@$vollist) {
	my ($storeid, undef) = parse_volume_id ($volid);
	$storagehash->{$storeid} = 1;
    }

    activate_storage_list ($cfg, [keys %$storagehash]);

    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id ($volid);

	my $scfg = storage_config ($cfg, $storeid);

	my $path = path ($cfg, $volid);

	if ($scfg->{type} eq 'lvm') {
	    my $cmd = ['/sbin/lvchange', "-a$lvm_activate_mode", $path];
	    run_command($cmd, errmsg => "can't activate LV '$volid'");
	}

	# check is volume exists
	if ($scfg->{type} eq 'dir' || $scfg->{type} eq 'nfs') {
	    die "volume '$volid' does not exist\n" if ! -e $path;
	} else {
	    die "volume '$volid' does not exist\n" if ! -b $path;
	}
    }
}

sub deactivate_volumes {
    my ($cfg, $vollist) = @_;

    return if !($vollist && scalar(@$vollist));

    my @errlist = ();
    foreach my $volid (@$vollist) {
	my ($storeid, $volname) = parse_volume_id ($volid);

	my $scfg = storage_config ($cfg, $storeid);

	if ($scfg->{type} eq 'lvm') {
	    my $path = path ($cfg, $volid);
	    next if ! -b $path;

	    my $cmd = ['/sbin/lvchange', '-aln', $path];
	    eval { run_command($cmd, errmsg => "can't deactivate LV '$volid'"); };
	    if (my $err = $@) {
		warn $err;
		push @errlist, $volid;
	    }
	}
    }

    die "volume deativation failed: " . join(' ', @errlist)
	if scalar(@errlist);
}

sub deactivate_storage {
    my ($cfg, $storeid) = @_;

    my $iscsi_sessions;

    my $scfg = storage_config ($cfg, $storeid);

    my $type = $scfg->{type};

    if ($type eq 'dir') {
	# nothing to do
    } elsif ($type eq 'nfs') {
	my $mountdata = read_proc_mounts();
	my $server = $scfg->{server};
	my $export = $scfg->{export};
	my $path = $scfg->{path};

	my $cmd = ['/bin/umount', $path];

	run_command($cmd, errmsg => 'umount error') 
	    if nfs_is_mounted ($server, $export, $path, $mountdata); 

    } elsif ($type eq 'lvm') {
	my $cmd = ['/sbin/vgchange', '-aln', $scfg->{vgname}];
	run_command($cmd, errmsg => "can't deactivate VG '$scfg->{vgname}'");
    } elsif ($type eq 'iscsi') {
	my $portal = $scfg->{portal};
	my $target = $scfg->{target};

	my $iscsi_sessions = iscsi_session_list();
	iscsi_logout ($target, $portal)
	    if defined ($iscsi_sessions->{$target});

    } else {
	die "implement me";
    }
}

sub storage_info { 
    my ($cfg, $content) = @_;

    my $ids = $cfg->{ids};

    my $info = {};
    my $stypes = {};

    my $slist = [];
    foreach my $storeid (keys %$ids) {

	next if $content && !$ids->{$storeid}->{content}->{$content};

	next if !storage_check_enabled($cfg, $storeid, undef, 1);

	my $type = $ids->{$storeid}->{type};

	$info->{$storeid} = { 
	    type => $type,
	    total => 0, 
	    avail => 0, 
	    used => 0, 
	    shared => $ids->{$storeid}->{shared} ? 1 : 0,
	    content => content_hash_to_string($ids->{$storeid}->{content}),
	    active => 0,
	};

	$stypes->{$type} = 1;

	push @$slist, $storeid;
    }

    my $session = {};
    my $mountdata = '';
    my $iscsi_sessions = {};
    my $vgs = {};

    if ($stypes->{lvm}) {
	$session->{vgs} = lvm_vgs();
	$vgs = $session->{vgs};
    }
    if ($stypes->{nfs}) {
	$mountdata = read_proc_mounts();
	$session->{mountdata} = $mountdata;
    }
    if ($stypes->{iscsi}) {
	$iscsi_sessions = iscsi_session_list();
	$session->{iscsi_sessions} = $iscsi_sessions;
    } 
 
    eval { activate_storage_list ($cfg, $slist, $session); };

    foreach my $storeid (keys %$ids) {
	my $scfg = $ids->{$storeid};

	next if !$info->{$storeid};

	my $type = $scfg->{type};

	if ($type eq 'dir' || $type eq 'nfs') {

	    my $path = $scfg->{path};
	    
	    if ($type eq 'nfs') {
		my $server = $scfg->{server};
		my $export = $scfg->{export};

		next if !nfs_is_mounted ($server, $export, $path, $mountdata); 
	    }

	    my $timeout = 2;
	    my $res = PVE::Tools::df($path, $timeout);

	    next if !$res || !$res->{total};

	    $info->{$storeid}->{total} = $res->{total}; 
	    $info->{$storeid}->{avail} = $res->{avail}; 
	    $info->{$storeid}->{used} = $res->{used}; 
	    $info->{$storeid}->{active} = 1;

	} elsif ($type eq 'lvm') {

	    my $vgname = $scfg->{vgname};

	    my $total = 0;
	    my $free = 0;

	    if (defined ($vgs->{$vgname})) {
		$total = $vgs->{$vgname}->{size};
		$free = $vgs->{$vgname}->{free};

		$info->{$storeid}->{total} = $total; 
		$info->{$storeid}->{avail} = $free; 
		$info->{$storeid}->{used} = $total - $free; 
		$info->{$storeid}->{active} = 1;
	    }

	} elsif ($type eq 'iscsi') {

	    $info->{$storeid}->{total} = 0; 
	    $info->{$storeid}->{avail} = 0; 
	    $info->{$storeid}->{used} = 0; 
	    $info->{$storeid}->{active} = 
		defined ($iscsi_sessions->{$scfg->{target}});

	} else {
	    die "implement me";
	}
    }

    return $info;
}

sub resolv_server {
    my ($server) = @_;
    
    my $packed_ip = gethostbyname($server);
    if (defined $packed_ip) {
	return inet_ntoa($packed_ip);
    }
    return undef;
}

sub scan_nfs {
    my ($server_in) = @_;

    my $server;
    if (!($server = resolv_server ($server_in))) {
	die "unable to resolve address for server '${server_in}'\n";
    }

    my $cmd = ['/sbin/showmount',  '--no-headers', '--exports', $server];

    my $res = {};
    run_command($cmd, outfunc => sub {
	my $line = shift;

	# note: howto handle white spaces in export path??
	if ($line =~ m!^(/\S+)\s+(.+)$!) {
	    $res->{$1} = $2;
	}
    });

    return $res;
}

sub resolv_portal {
    my ($portal, $noerr) = @_;

    if ($portal =~ m/^([^:]+)(:(\d+))?$/) {
	my $server = $1;
	my $port = $3;

	if (my $ip = resolv_server($server)) {
	    $server = $ip;
	    return $port ? "$server:$port" : $server;
	}
    }
    return undef if $noerr;

    raise_param_exc({ portal => "unable to resolve portal address '$portal'" });
}

# idea is from usbutils package (/usr/bin/usb-devices) script
sub __scan_usb_device {
    my ($res, $devpath, $parent, $level) = @_;

    return if ! -d $devpath;
    return if $level && $devpath !~ m/^.*[-.](\d+)$/;
    my $port = $level ? int($1 - 1) : 0;

    my $busnum = int(file_read_firstline("$devpath/busnum"));
    my $devnum = int(file_read_firstline("$devpath/devnum"));

    my $d = {
	port => $port,
	level => $level,
	busnum => $busnum,
	devnum => $devnum,
	speed => file_read_firstline("$devpath/speed"),
	class => hex(file_read_firstline("$devpath/bDeviceClass")),
	vendid => file_read_firstline("$devpath/idVendor"),
	prodid => file_read_firstline("$devpath/idProduct"),
    };

    if ($level) {
	my $usbpath = $devpath;
	$usbpath =~ s|^.*/\d+\-||;
	$d->{usbpath} = $usbpath;
    }

    my $product = file_read_firstline("$devpath/product");
    $d->{product} = $product if $product;
    
    my $manu = file_read_firstline("$devpath/manufacturer");
    $d->{manufacturer} = $manu if $manu;

    my $serial => file_read_firstline("$devpath/serial");
    $d->{serial} = $serial if $serial;

    push @$res, $d;

    foreach my $subdev (<$devpath/$busnum-*>) {
	next if $subdev !~ m|/$busnum-[0-9]+(\.[0-9]+)*$|;
	__scan_usb_device($res, $subdev, $devnum, $level + 1);
    }

};

sub scan_usb {

    my $devlist = [];

    foreach my $device (</sys/bus/usb/devices/usb*>) {
	__scan_usb_device($devlist, $device, 0, 0);
    }

    return $devlist;
}

sub scan_iscsi {
    my ($portal_in) = @_;

    my $portal;
    if (!($portal = resolv_portal ($portal_in))) {
	die "unable to parse/resolve portal address '${portal_in}'\n";
    }

    return iscsi_discovery($portal);
}

sub storage_default_format {
    my ($cfg, $storeid) = @_;

    my $scfg = storage_config ($cfg, $storeid);

    my $def = $default_config->{$scfg->{type}};

    my $def_format = 'raw';
    my $valid_formats = [ $def_format ];

    if (defined ($def->{format})) {
	$def_format = $scfg->{format} || $def->{format}->[1];
	$valid_formats = [ sort keys %{$def->{format}->[0]} ];
    }
   
    return wantarray ? ($def_format, $valid_formats) : $def_format;
}

sub vgroup_is_used {
    my ($cfg, $vgname) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config ($cfg, $storeid);
	if ($scfg->{type} eq 'lvm' && $scfg->{vgname} eq $vgname) {
	    return 1;
	}
    }

    return undef;
}

sub target_is_used {
    my ($cfg, $target) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config ($cfg, $storeid);
	if ($scfg->{type} eq 'iscsi' && $scfg->{target} eq $target) {
	    return 1;
	}
    }

    return undef;
}

sub volume_is_used {
    my ($cfg, $volid) = @_;

    foreach my $storeid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config ($cfg, $storeid);
	if ($scfg->{base} && $scfg->{base} eq $volid) {
	    return 1;
	}
    }

    return undef;
}

sub storage_is_used {
    my ($cfg, $storeid) = @_;

    foreach my $sid (keys %{$cfg->{ids}}) {
	my $scfg = storage_config ($cfg, $sid);
	next if !$scfg->{base};
	my ($st) = parse_volume_id ($scfg->{base});
	return 1 if $st && $st eq $storeid;
    }

    return undef;
}

sub foreach_volid {
    my ($list, $func) = @_;

    return if !$list;

    foreach my $sid (keys %$list) {
       foreach my $info (@{$list->{$sid}}) {
           my $volid = $info->{volid};
	   my ($sid1, $volname) = parse_volume_id ($volid, 1);
	   if ($sid1 && $sid1 eq $sid) {
	       &$func ($volid, $sid, $info);
	   } else {
	       warn "detected strange volid '$volid' in volume list for '$sid'\n";
	   }
       }
    }
}

1;
