package PVE::Storage::CephTools;

use strict;
use warnings;
use Net::IP;
use PVE::Tools qw(run_command);

my $ceph_check_keyfile = sub {
    my ($filename, $scfg) = @_;

    if (-f $filename) {
	my $content = PVE::Tools::file_get_contents($filename);
	my @lines = split /\n/, $content;

	my $section;

	foreach my $line (@lines) {
	    next if !$line;

	    $section = $1 if $line =~ m/^\[(\S+)\]$/;

	    if ($scfg->{type} eq 'rbd') {
		if ((!$section) && (!$section =~ m/^$/)) {
		    warn "Not a proper $scfg->{type} authentication file: $filename\n";
		}
	    } elsif ($scfg->{type} eq 'cephfs') {
		if ($section || ($line =~ s/^\s+//)) {
		    warn "Not a proper $scfg->{type} authentication file: $filename\n";
		}
	    }
	}
    }

    return undef;
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

sub ceph_connect_option {
    my ($scfg, $storeid, %options) = @_;

    my $cmd_option = {};
    my $ceph_storeid_conf = "/etc/pve/priv/ceph/${storeid}.conf";
    my $pveceph_config = '/etc/pve/ceph.conf';
    my $keyfile = "/etc/pve/priv/ceph/${storeid}.keyring";
    $keyfile = "/etc/pve/priv/ceph/${storeid}.secret" if ($scfg->{type} eq 'cephfs');
    my $pveceph_managed = !defined($scfg->{monhost});

    $cmd_option->{ceph_conf} = $pveceph_config if $pveceph_managed;

    if (-e $keyfile) {
	$ceph_check_keyfile->($keyfile, $scfg);
    }

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

1;
