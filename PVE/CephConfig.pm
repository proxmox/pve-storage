package PVE::CephConfig;

use strict;
use warnings;
use Net::IP;
use PVE::Tools qw(run_command);
use PVE::Cluster qw(cfs_register_file);

cfs_register_file('ceph.conf',
		  \&parse_ceph_config,
		  \&write_ceph_config);

sub parse_ceph_config {
    my ($filename, $raw) = @_;

    my $cfg = {};
    return $cfg if !defined($raw);

    my @lines = split /\n/, $raw;

    my $section;

    foreach my $line (@lines) {
	$line =~ s/[;#].*$//;
	$line =~ s/^\s+//;
	$line =~ s/\s+$//;
	next if !$line;

	$section = $1 if $line =~ m/^\[(\S+)\]$/;
	if (!$section) {
	    warn "no section - skip: $line\n";
	    next;
	}

	if ($line =~ m/^(.*?\S)\s*=\s*(\S.*)$/) {
	    $cfg->{$section}->{$1} = $2;
	}

    }

    return $cfg;
}

my $parse_ceph_file = sub {
    my ($filename) = @_;

    my $cfg = {};

    return $cfg if ! -f $filename;

    my $content = PVE::Tools::file_get_contents($filename);

    return parse_ceph_config($filename, $content);
};

sub write_ceph_config {
    my ($filename, $cfg) = @_;

    my $out = '';

    my $cond_write_sec = sub {
	my $re = shift;

	foreach my $section (keys %$cfg) {
	    next if $section !~ m/^$re$/;
	    $out .= "[$section]\n";
	    foreach my $key (sort keys %{$cfg->{$section}}) {
		$out .= "\t $key = $cfg->{$section}->{$key}\n";
	    }
	    $out .= "\n";
	}
    };

    &$cond_write_sec('global');
    &$cond_write_sec('client');

    &$cond_write_sec('mds');
    &$cond_write_sec('mon');
    &$cond_write_sec('osd');
    &$cond_write_sec('mgr');

    &$cond_write_sec('mds\..*');
    &$cond_write_sec('mon\..*');
    &$cond_write_sec('osd\..*');
    &$cond_write_sec('mgr\..*');

    return $out;
}

my $ceph_get_key = sub {
    my ($keyfile, $username) = @_;

    my $key = $parse_ceph_file->($keyfile);
    my $secret = $key->{"client.$username"}->{key};

    return $secret;
};

sub get_monaddr_list {
    my ($configfile) = shift;

    if (!defined($configfile)) {
	warn "No ceph config specified\n";
	return;
    }

    my $config = $parse_ceph_file->($configfile);

    my @monids = grep { /mon\./ && defined($config->{$_}->{'mon addr'}) } %{$config};

    return join(',', sort map { $config->{$_}->{'mon addr'} } @monids);
};

sub hostlist {
    my ($list_text, $separator) = @_;

    my @monhostlist = PVE::Tools::split_list($list_text);
    return join($separator, map {
	my ($host, $port) = PVE::Tools::parse_host_and_port($_);
	$port = defined($port) ? ":$port" : '';
	$host = "[$host]" if Net::IP::ip_is_ipv6($host);
	"${host}${port}"
    } @monhostlist);
}

my $ceph_check_keyfile = sub {
    my ($filename, $type) = @_;

    return if ! -f $filename;

    my $content = PVE::Tools::file_get_contents($filename);
    eval {
	die if !$content;

	if ($type eq 'rbd') {
	    die if $content !~ /\s*\[\S+\]\s*key\s*=\s*\S+==\s*$/m;
	} elsif ($type eq 'cephfs') {
	    die if $content !~ /\S+==\s*$/;
	}
    };
    die "Not a proper $type authentication file: $filename\n" if $@;

    return undef;
};

sub ceph_connect_option {
    my ($scfg, $storeid, %options) = @_;

    my $cmd_option = {};
    my $ceph_storeid_conf = "/etc/pve/priv/ceph/${storeid}.conf";
    my $pveceph_config = '/etc/pve/ceph.conf';
    my $keyfile = "/etc/pve/priv/ceph/${storeid}.keyring";
    $keyfile = "/etc/pve/priv/ceph/${storeid}.secret" if ($scfg->{type} eq 'cephfs');
    my $pveceph_managed = !defined($scfg->{monhost});

    $cmd_option->{ceph_conf} = $pveceph_config if $pveceph_managed;

    $ceph_check_keyfile->($keyfile, $scfg->{type});

    if (-e $ceph_storeid_conf) {
	if ($pveceph_managed) {
	    warn "ignoring custom ceph config for storage '$storeid', 'monhost' is not set (assuming pveceph managed cluster)!\n";
	} else {
	    $cmd_option->{ceph_conf} = $ceph_storeid_conf;
	}
    }

    $cmd_option->{keyring} = $keyfile if (-e $keyfile);
    $cmd_option->{auth_supported} = (defined $cmd_option->{keyring}) ? 'cephx' : 'none';
    $cmd_option->{userid} =  $scfg->{username} ? $scfg->{username} : 'admin';
    $cmd_option->{mon_host} = hostlist($scfg->{monhost}, ',') if (defined($scfg->{monhost}));

    if (%options) {
	foreach my $k (keys %options) {
	    $cmd_option->{$k} = $options{$k};
	}
    }

    return $cmd_option;

}

sub ceph_create_keyfile {
    my ($type, $storeid) = @_;

    my $extension = 'keyring';
    $extension = 'secret' if ($type eq 'cephfs');

    my $ceph_admin_keyring = '/etc/pve/priv/ceph.client.admin.keyring';
    my $ceph_storage_keyring = "/etc/pve/priv/ceph/${storeid}.$extension";

    die "ceph authx keyring file for storage '$storeid' already exists!\n"
	if -e $ceph_storage_keyring;

    if (-e $ceph_admin_keyring) {
	eval {
	    if ($type eq 'rbd') {
		mkdir '/etc/pve/priv/ceph';
		PVE::Tools::file_copy($ceph_admin_keyring, $ceph_storage_keyring);
	    } elsif ($type eq 'cephfs') {
		my $secret = $ceph_get_key->($ceph_admin_keyring, 'admin');
		mkdir '/etc/pve/priv/ceph';
		PVE::Tools::file_set_contents($ceph_storage_keyring, $secret, 0400);
	   }
	};
	if (my $err = $@) {
	   unlink $ceph_storage_keyring;
	   die "failed to copy ceph authx $extension for storage '$storeid': $err\n";
	}
    } else {
	warn "$ceph_admin_keyring not found, authentication is disabled.\n";
    }
}

sub ceph_remove_keyfile {
    my ($type, $storeid) = @_;

    my $extension = 'keyring';
    $extension = 'secret' if ($type eq 'cephfs');
    my $ceph_storage_keyring = "/etc/pve/priv/ceph/${storeid}.$extension";

    if (-f $ceph_storage_keyring) {
	unlink($ceph_storage_keyring) or warn "removing keyring of storage failed: $!\n";
    }
}

1;
