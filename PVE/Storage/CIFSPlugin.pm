package PVE::Storage::CIFSPlugin;

use strict;
use warnings;
use Net::IP;
use PVE::Tools qw(run_command);
use PVE::ProcFSTools;
use File::Path;
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# CIFS helper functions

sub cifs_is_mounted {
    my ($server, $share, $mountpoint, $mountdata) = @_;

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "//${server}/$share";
    $mountdata = PVE::ProcFSTools::parse_proc_mounts() if !$mountdata;

    return $mountpoint if grep {
	$_->[2] =~ /^cifs/ &&
	$_->[0] =~ m|^\Q$source\E/?$| &&
	$_->[1] eq $mountpoint
    } @$mountdata;
    return undef;
}

sub cifs_cred_file_name {
    my ($storeid) = @_;

    return "/etc/pve/priv/${storeid}.cred";
}

sub cifs_set_credentials {
    my ($password, $storeid) = @_;

    my $cred_file = cifs_cred_file_name($storeid);

    PVE::Tools::file_set_contents($cred_file, "password=$password\n");

    return $cred_file;
}

sub get_cred_file {
    my ($storeid) = @_;

    my $cred_file = cifs_cred_file_name($storeid);

    return -e $cred_file ? $cred_file : undef;
}

sub cifs_mount {
    my ($server, $share, $mountpoint, $storeid, $smbver, $user, $domain) = @_;

    $server = "[$server]" if Net::IP::ip_is_ipv6($server);
    my $source = "//${server}/$share";

    my $cmd = ['/bin/mount', '-t', 'cifs', $source, $mountpoint, '-o', 'soft', '-o'];

    if (my $cred_file = get_cred_file($storeid)) {
	push @$cmd, "username=$user", '-o', "credentials=$cred_file";
	push @$cmd, '-o', "domain=$domain" if defined($domain);
    } else {
	push @$cmd, 'guest,username=guest';
    }

    push @$cmd, '-o', defined($smbver) ? "vers=$smbver" : "vers=3.0";

    run_command($cmd, errmsg => "mount error");
}

# Configuration

sub type {
    return 'cifs';
}

sub plugindata {
    return {
	content => [ { images => 1, rootdir => 1, vztmpl => 1, iso => 1,
		   backup => 1}, { images => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    };
}

sub properties {
    return {
	share => {
	    description => "CIFS share.",
	    type => 'string',
	},
	password => {
	    description => "Password for CIFS share.",
	    type => 'string',
	    maxLength => 256,
	},
	domain => {
	    description => "CIFS domain.",
	    type => 'string',
	    optional => 1,
	    maxLength => 256,
	},
	smbversion => {
	    description => "SMB protocol version",
	    type => 'string',
	    enum => ['2.0', '2.1', '3.0'],
	    optional => 1,
	},
    };
}

sub options {
    return {
	path => { fixed => 1 },
	server => { fixed => 1 },
	share => { fixed => 1 },
	nodes => { optional => 1 },
	disable => { optional => 1 },
	maxfiles => { optional => 1 },
	content => { optional => 1 },
	format => { optional => 1 },
	username => { optional => 1 },
	password => { optional => 1},
	domain => { optional => 1},
	smbversion => { optional => 1},
	mkdir => { optional => 1 },
    };
}


sub check_config {
    my ($class, $sectionId, $config, $create, $skipSchemaCheck) = @_;

    $config->{path} = "/mnt/pve/$sectionId" if $create && !$config->{path};

    return $class->SUPER::check_config($sectionId, $config, $create, $skipSchemaCheck);
}

# Storage implementation

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (my $password = $param{password}) {
	cifs_set_credentials($password, $storeid);
    }
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    my $cred_file = cifs_cred_file_name($storeid);
    if (-f $cred_file) {
	unlink($cred_file) or warn "removing cifs credientials '$cred_file' failed: $!\n";
    }
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $share = $scfg->{share};

    return undef
	if !cifs_is_mounted($server, $share, $path, $cache->{mountdata});

    return $class->SUPER::status($storeid, $scfg, $cache);
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $share = $scfg->{share};

    if (!cifs_is_mounted($server, $share, $path, $cache->{mountdata})) {

	mkpath $path if !(defined($scfg->{mkdir}) && !$scfg->{mkdir});

	die "unable to activate storage '$storeid' - " .
	    "directory '$path' does not exist\n" if ! -d $path;

	cifs_mount($server, $share, $path, $storeid, $scfg->{smbversion},
	    $scfg->{username}, $scfg->{domain});
    }

    $class->SUPER::activate_storage($storeid, $scfg, $cache);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{mountdata} = PVE::ProcFSTools::parse_proc_mounts()
	if !$cache->{mountdata};

    my $path = $scfg->{path};
    my $server = $scfg->{server};
    my $share = $scfg->{share};

    if (cifs_is_mounted($server, $share, $path, $cache->{mountdata})) {
	my $cmd = ['/bin/umount', $path];
	run_command($cmd, errmsg => 'umount error');
    }
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    my $servicename = '//'.$scfg->{server}.'/'.$scfg->{share};

    my $cmd = ['/usr/bin/smbclient', $servicename, '-d', '0', '-m'];

    push @$cmd, $scfg->{smbversion} ? "smb".int($scfg->{smbversion}) : 'smb3';

    if (my $cred_file = get_cred_file($storeid)) {
	push @$cmd, '-U', $scfg->{username}, '-A', $cred_file;
	push @$cmd, '-W', $scfg->{domain} if defined($scfg->{domain});
    } else {
	push @$cmd, '-U', 'Guest','-N';
    }

    push @$cmd, '-c', 'echo 1 0';

    my $out_str;
    eval {
	run_command($cmd, timeout => 2, outfunc => sub {$out_str .= shift;},
		    errfunc => sub {});
    };

    if (my $err = $@) {
	die "$out_str\n" if defined($out_str) &&
	    ($out_str =~ m/NT_STATUS_ACCESS_DENIED/);
	return 0;
    }

    return 1;
}

1;
