package PVE::Storage::PBSPlugin;

# Plugin to access Proxmox Backup Server

use strict;
use warnings;
use POSIX qw(strftime);
use IO::File;
use HTTP::Request;
use LWP::UserAgent;
use JSON;
use Data::Dumper; # fixme: remove

use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

# Configuration

sub type {
    return 'pbs';
}

sub plugindata {
    return {
	content => [ {backup => 1, none => 1}, { backup => 1 }],
    };
}

sub properties {
    return {
	datastore => {
	    description => "Proxmox backup server datastore name.",
	    type => 'string',
	},
	# openssl s_client -connect <host>:8007 2>&1 |openssl x509 -fingerprint -sha256
	fingerprint => get_standard_option('fingerprint-sha256'),
    };
}

sub options {
    return {
	server => { fixed => 1 },
	datastore => { fixed => 1 },
	nodes => { optional => 1},
	disable => { optional => 1},
	content => { optional => 1},
	username => { optional => 1 },
	password => { optional => 1},
	maxfiles => { optional => 1 },
	fingerprint => { optional => 1 },
    };
}

# Helpers

sub pbs_password_file_name {
    my ($scfg, $storeid) = @_;

    return "/etc/pve/priv/storage/${storeid}.pw";
}

sub pbs_set_password {
    my ($scfg, $storeid, $password) = @_;

    my $pwfile = pbs_password_file_name($scfg, $storeid);
    mkdir "/etc/pve/priv/storage";

    PVE::Tools::file_set_contents($pwfile, "$password\n");
}

sub pbs_delete_password {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_password_file_name($scfg, $storeid);

    unlink $pwfile;
}

sub pbs_get_password {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_password_file_name($scfg, $storeid);

    return PVE::Tools::file_read_firstline($pwfile);
}


sub run_raw_client_cmd {
    my ($scfg, $storeid, $client_cmd, $param, %opts) = @_;

    my $client_exe = '/usr/bin/proxmox-backup-client';
    die "executable not found '$client_exe'! Proxmox backup client not installed?\n"
	if ! -x $client_exe;

    my $server = $scfg->{server};
    my $datastore = $scfg->{datastore};
    my $username = $scfg->{username} // 'root@pam';

    my $userns_cmd = delete $opts{userns_cmd};

    my $cmd = [];

    push @$cmd, @$userns_cmd if defined($userns_cmd);

    push @$cmd, $client_exe, $client_cmd;

    push @$cmd, @$param if defined($param);

    push @$cmd, "--repository", "$username\@$server:$datastore";

    local $ENV{PBS_PASSWORD} = pbs_get_password($scfg, $storeid);

    local $ENV{PBS_FINGERPRINT} = $scfg->{fingerprint};

    if (my $logfunc = $opts{logfunc}) {
	$logfunc->("run bps command: " . join(' ', @$cmd));
    }

    run_command($cmd, %opts);
}

sub run_client_cmd {
    my ($scfg, $storeid, $client_cmd, $param, $no_output) = @_;

    my $json_str = '';
    my $outfunc = sub { $json_str .= "$_[0]\n" };

    $param = [] if !defined($param);
    $param = [ $param ] if !ref($param);

    $param = [@$param, '--output-format=json'] if !$no_output;

    run_raw_client_cmd($scfg, $storeid, $client_cmd, $param,
		       outfunc => $outfunc, errmsg => 'proxmox-backup-client failed');

    return undef if $no_output;

    my $res = decode_json($json_str);

    return $res;
}

# Storage implementation

sub extract_vzdump_config {
    my ($class, $scfg, $volname, $storeid) = @_;

    my ($vtype, $name, $vmid, undef, undef, undef, $format) = $class->parse_volname($volname);

    my $config = '';
    my $outfunc = sub { $config .= "$_[0]\n" };

    my $config_name;
    if ($format eq 'pbs-vm') {
	$config_name = 'qemu-server.conf';
    } elsif  ($format eq 'pbs-ct') {
	$config_name = 'pct.conf';
    } else {
	die "unable to extract configuration for backup format '$format'\n";
    }

    run_raw_client_cmd($scfg, $storeid, 'restore', [ $name, $config_name, '-' ],
		       outfunc => $outfunc, errmsg => 'proxmox-backup-client failed');

    return $config;
}

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (defined($param{password})) {
	pbs_set_password($scfg, $storeid, $param{password});
    } else {
	pbs_delete_password($scfg, $storeid);
    }
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    return if !exists($param{password});

    if (defined($param{password})) {
	pbs_set_password($scfg, $storeid, $param{password});
    } else {
	pbs_delete_password($scfg, $storeid);
    }
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    pbs_delete_password($scfg, $storeid);
}

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m!^backup/([^\s_]+)/([^\s_]+)/([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)$!) {
	my $btype = $1;
	my $bid = $2;
	my $btime = $3;
	my $format = "pbs-$btype";

	my $name = "$btype/$bid/$btime";

	if ($bid =~ m/^\d+$/) {
	    return ('backup', $name, $bid, undef, undef, undef, $format);
	} else {
	    return ('backup', $name, undef, undef, undef, undef, $format);
	}
    }

    die "unable to parse PBS volume name '$volname'\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    die "volume snapshot is not possible on pbs storage"
	if defined($snapname);

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    my $server = $scfg->{server};
    my $datastore = $scfg->{datastore};
    my $username = $scfg->{username} // 'root@pam';

    # artifical url - we currently do not use that anywhere
    my $path = "pbs://$username\@$server:$datastore/$name";

    return ($path, $vmid, $vtype);
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in pbs storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in pbs storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "can't allocate space in pbs storage\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    my ($vtype, $name, $vmid) = $class->parse_volname($volname);

    run_client_cmd($scfg, $storeid, "forget", [ $name ], 1);
}


sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $res = [];

    return $res;
}

sub list_volumes {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    my $res = [];

    return $res if !grep { $_ eq 'backup' } @$content_types;

    my $data = run_client_cmd($scfg, $storeid, "snapshots");

    foreach my $item (@$data) {
	my $btype = $item->{"backup-type"};
	my $bid = $item->{"backup-id"};
	my $epoch = $item->{"backup-time"};
	my $size = $item->{size} // 1;

	next if !($btype eq 'vm' || $btype eq 'ct');
	next if $bid !~ m/^\d+$/;

	my $btime = strftime("%FT%TZ", gmtime($epoch));
	my $volname = "backup/${btype}/${bid}/${btime}";

	my $volid = "$storeid:$volname";

	my $info = {
	    volid => $volid,
	    format => "pbs-$btype",
	    size => $size,
	    content => 'backup',
	    vmid => int($bid),
	    ctime => $epoch,
	};

	push @$res, $info;
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 0;

    eval {
	my $res = run_client_cmd($scfg, $storeid, "status");

	$active = 1;
	$total = $res->{total};
	$used = $res->{used};
	$free = $res->{avail};
    };
    if (my $err = $@) {
	warn $err;
    }

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
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "volume snapshot is not possible on pbs device" if $snapname;

    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "volume snapshot is not possible on pbs device" if $snapname;

    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($vtype, $name,  undef, undef, undef, undef, $format) = $class->parse_volname($volname);

    my $data = run_client_cmd($scfg, $storeid, "files", [ $name ]);

    my $size = 0;
    foreach my $info (@$data) {
	$size += $info->{size} if $info->{size};
    }

    my $used = $size;

    return wantarray ? ($size, $format, $used, undef) : $size;
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    die "volume resize is not possible on pbs device";
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot is not possible on pbs device";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot rollback is not possible on pbs device";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot delete is not possible on pbs device";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    return undef;
}

1;
