package PVE::Storage::PBSPlugin;

# Plugin to access Proxmox Backup Server

use strict;
use warnings;

use Fcntl qw(F_GETFD F_SETFD FD_CLOEXEC);
use IO::File;
use JSON;
use MIME::Base64 qw(decode_base64);
use POSIX qw(strftime ENOENT);

use PVE::APIClient::LWP;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Network;
use PVE::PBSClient;
use PVE::Storage::Plugin;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach $IPV6RE);

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
	    description => "Proxmox Backup Server datastore name.",
	    type => 'string',
	},
	# openssl s_client -connect <host>:8007 2>&1 |openssl x509 -fingerprint -sha256
	fingerprint => get_standard_option('fingerprint-sha256'),
	'encryption-key' => {
	    description => "Encryption key. Use 'autogen' to generate one automatically without passphrase.",
	    type => 'string',
	},
	'master-pubkey' => {
	    description => "Base64-encoded, PEM-formatted public RSA key. Used to encrypt a copy of the encryption-key which will be added to each encrypted backup.",
	    type => 'string',
	},
	port => {
	    description => "For non default port.",
	    type => 'integer',
	    minimum => 1,
	    maximum => 65535,
	    default => 8007,
	},
    };
}

sub options {
    return {
	server => { fixed => 1 },
	datastore => { fixed => 1 },
	port => { optional => 1 },
	nodes => { optional => 1},
	disable => { optional => 1},
	content => { optional => 1},
	username => { optional => 1 },
	password => { optional => 1 },
	'encryption-key' => { optional => 1 },
	'master-pubkey' => { optional => 1 },
	maxfiles => { optional => 1 },
	'prune-backups' => { optional => 1 },
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

sub pbs_encryption_key_file_name {
    my ($scfg, $storeid) = @_;

    return "/etc/pve/priv/storage/${storeid}.enc";
}

sub pbs_set_encryption_key {
    my ($scfg, $storeid, $key) = @_;

    my $pwfile = pbs_encryption_key_file_name($scfg, $storeid);
    mkdir "/etc/pve/priv/storage";

    PVE::Tools::file_set_contents($pwfile, "$key\n");
}

sub pbs_delete_encryption_key {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_encryption_key_file_name($scfg, $storeid);

    if (!unlink $pwfile) {
	return if $! == ENOENT;
	die "failed to delete encryption key! $!\n";
    }
    delete $scfg->{'encryption-key'};
}

sub pbs_get_encryption_key {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_encryption_key_file_name($scfg, $storeid);

    return PVE::Tools::file_get_contents($pwfile);
}

# Returns a file handle if there is an encryption key, or `undef` if there is not. Dies on error.
sub pbs_open_encryption_key {
    my ($scfg, $storeid) = @_;

    my $encryption_key_file = pbs_encryption_key_file_name($scfg, $storeid);

    my $keyfd;
    if (!open($keyfd, '<', $encryption_key_file)) {
	return undef if $! == ENOENT;
	die "failed to open encryption key: $encryption_key_file: $!\n";
    }

    return $keyfd;
}

sub pbs_master_pubkey_file_name {
    my ($scfg, $storeid) = @_;

    return "/etc/pve/priv/storage/${storeid}.master.pem";
}

sub pbs_set_master_pubkey {
    my ($scfg, $storeid, $key) = @_;

    my $pwfile = pbs_master_pubkey_file_name($scfg, $storeid);
    mkdir "/etc/pve/priv/storage";

    PVE::Tools::file_set_contents($pwfile, "$key\n");
}

sub pbs_delete_master_pubkey {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_master_pubkey_file_name($scfg, $storeid);

    if (!unlink $pwfile) {
	return if $! == ENOENT;
	die "failed to delete master public key! $!\n";
    }
    delete $scfg->{'master-pubkey'};
}

sub pbs_get_master_pubkey {
    my ($scfg, $storeid) = @_;

    my $pwfile = pbs_master_pubkey_file_name($scfg, $storeid);

    return PVE::Tools::file_get_contents($pwfile);
}

# Returns a file handle if there is a master key, or `undef` if there is not. Dies on error.
sub pbs_open_master_pubkey {
    my ($scfg, $storeid) = @_;

    my $master_pubkey_file = pbs_master_pubkey_file_name($scfg, $storeid);

    my $keyfd;
    if (!open($keyfd, '<', $master_pubkey_file)) {
	return undef if $! == ENOENT;
	die "failed to open master public key: $master_pubkey_file: $!\n";
    }

    return $keyfd;
}

sub print_volid {
    my ($storeid, $btype, $bid, $btime) = @_;

    my $time_str = strftime("%FT%TZ", gmtime($btime));
    my $volname = "backup/${btype}/${bid}/${time_str}";

    return "${storeid}:${volname}";
}

my $USE_CRYPT_PARAMS = {
    backup => 1,
    restore => 1,
    'upload-log' => 1,
};

my $USE_MASTER_KEY = {
    backup => 1,
};

my sub do_raw_client_cmd {
    my ($scfg, $storeid, $client_cmd, $param, %opts) = @_;

    my $use_crypto = $USE_CRYPT_PARAMS->{$client_cmd};
    my $use_master = $USE_MASTER_KEY->{$client_cmd};

    my $client_exe = '/usr/bin/proxmox-backup-client';
    die "executable not found '$client_exe'! Proxmox backup client not installed?\n"
	if ! -x $client_exe;

    my $repo = PVE::PBSClient::get_repository($scfg);

    my $userns_cmd = delete $opts{userns_cmd};

    my $cmd = [];

    push @$cmd, @$userns_cmd if defined($userns_cmd);

    push @$cmd, $client_exe, $client_cmd;

    # This must live in the top scope to not get closed before the `run_command`
    my ($keyfd, $master_fd);
    if ($use_crypto) {
	if (defined($keyfd = pbs_open_encryption_key($scfg, $storeid))) {
	    my $flags = fcntl($keyfd, F_GETFD, 0)
		// die "failed to get file descriptor flags: $!\n";
	    fcntl($keyfd, F_SETFD, $flags & ~FD_CLOEXEC)
		or die "failed to remove FD_CLOEXEC from encryption key file descriptor\n";
	    push @$cmd, '--crypt-mode=encrypt', '--keyfd='.fileno($keyfd);
	    if ($use_master && defined($master_fd = pbs_open_master_pubkey($scfg, $storeid))) {
		my $flags = fcntl($master_fd, F_GETFD, 0)
		    // die "failed to get file descriptor flags: $!\n";
		fcntl($master_fd, F_SETFD, $flags & ~FD_CLOEXEC)
		    or die "failed to remove FD_CLOEXEC from master public key file descriptor\n";
		push @$cmd, '--master-pubkey-fd='.fileno($master_fd);
	    }
	} else {
	    push @$cmd, '--crypt-mode=none';
	}
    }

    push @$cmd, @$param if defined($param);

    push @$cmd, "--repository", $repo;

    local $ENV{PBS_PASSWORD} = pbs_get_password($scfg, $storeid);

    local $ENV{PBS_FINGERPRINT} = $scfg->{fingerprint};

    # no ascii-art on task logs
    local $ENV{PROXMOX_OUTPUT_NO_BORDER} = 1;
    local $ENV{PROXMOX_OUTPUT_NO_HEADER} = 1;

    if (my $logfunc = $opts{logfunc}) {
	$logfunc->("run: " . join(' ', @$cmd));
    }

    run_command($cmd, %opts);
}

# FIXME: External perl code should NOT have access to this.
#
# There should be separate functions to
# - make backups
# - restore backups
# - restore files
# with a sane API
sub run_raw_client_cmd {
    my ($scfg, $storeid, $client_cmd, $param, %opts) = @_;
    return do_raw_client_cmd($scfg, $storeid, $client_cmd, $param, %opts);
}

sub run_client_cmd {
    my ($scfg, $storeid, $client_cmd, $param, $no_output) = @_;

    my $json_str = '';
    my $outfunc = sub { $json_str .= "$_[0]\n" };

    $param = [] if !defined($param);
    $param = [ $param ] if !ref($param);

    $param = [@$param, '--output-format=json'] if !$no_output;

    do_raw_client_cmd($scfg, $storeid, $client_cmd, $param,
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

    do_raw_client_cmd($scfg, $storeid, 'restore', [ $name, $config_name, '-' ],
		      outfunc => $outfunc, errmsg => 'proxmox-backup-client failed');

    return $config;
}

sub prune_backups {
    my ($class, $scfg, $storeid, $keep, $vmid, $type, $dryrun, $logfunc) = @_;

    $logfunc //= sub { print "$_[1]\n" };

    my $backups = $class->list_volumes($storeid, $scfg, $vmid, ['backup']);

    $type = 'vm' if defined($type) && $type eq 'qemu';
    $type = 'ct' if defined($type) && $type eq 'lxc';

    my $backup_groups = {};
    foreach my $backup (@{$backups}) {
	(my $backup_type = $backup->{format}) =~ s/^pbs-//;

	next if defined($type) && $backup_type ne $type;

	my $backup_group = "$backup_type/$backup->{vmid}";
	$backup_groups->{$backup_group} = 1;
    }

    my @param;

    my $keep_all = delete $keep->{'keep-all'};

    if (!$keep_all) {
	foreach my $opt (keys %{$keep}) {
	    next if $keep->{$opt} == 0;
	    push @param, "--$opt";
	    push @param, "$keep->{$opt}";
	}
    } else { # no need to pass anything to PBS
	$keep = { 'keep-all' => 1 };
    }

    push @param, '--dry-run' if $dryrun;

    my $prune_list = [];
    my $failed;

    foreach my $backup_group (keys %{$backup_groups}) {
	$logfunc->('info', "running 'proxmox-backup-client prune' for '$backup_group'")
	    if !$dryrun;
	eval {
	    my $res = run_client_cmd($scfg, $storeid, 'prune', [ $backup_group, @param ]);

	    foreach my $backup (@{$res}) {
		die "result from proxmox-backup-client is not as expected\n"
		    if !defined($backup->{'backup-time'})
		    || !defined($backup->{'backup-type'})
		    || !defined($backup->{'backup-id'})
		    || !defined($backup->{'keep'});

		my $ctime = $backup->{'backup-time'};
		my $type = $backup->{'backup-type'};
		my $vmid = $backup->{'backup-id'};
		my $volid = print_volid($storeid, $type, $vmid, $ctime);

		push @{$prune_list}, {
		    ctime => $ctime,
		    mark => $backup->{keep} ? 'keep' : 'remove',
		    type => $type eq 'vm' ? 'qemu' : 'lxc',
		    vmid => $vmid,
		    volid => $volid,
		};
	    }
	};
	if (my $err = $@) {
	    $logfunc->('err', "prune '$backup_group': $err\n");
	    $failed = 1;
	}
    }
    die "error pruning backups - check log\n" if $failed;

    return $prune_list;
}

my $autogen_encryption_key = sub {
    my ($scfg, $storeid) = @_;
    my $encfile = pbs_encryption_key_file_name($scfg, $storeid);
    if (-f $encfile) {
	rename $encfile, "$encfile.old";
    }
    my $cmd = ['proxmox-backup-client', 'key', 'create', '--kdf', 'none', $encfile];
    run_command($cmd, errmsg => 'failed to create encryption key');
    return PVE::Tools::file_get_contents($encfile);
};

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    my $res = {};

    if (defined(my $password = $param{password})) {
	pbs_set_password($scfg, $storeid, $password);
    } else {
	pbs_delete_password($scfg, $storeid);
    }

    if (defined(my $encryption_key = $param{'encryption-key'})) {
	my $decoded_key;
	if ($encryption_key eq 'autogen') {
	    $res->{'encryption-key'} = $autogen_encryption_key->($scfg, $storeid);
	    $decoded_key = decode_json($res->{'encryption-key'});
	} else {
	    $decoded_key = eval { decode_json($encryption_key) };
	    if ($@ || !exists($decoded_key->{data})) {
		die "Value does not seems like a valid, JSON formatted encryption key!\n";
	    }
	    pbs_set_encryption_key($scfg, $storeid, $encryption_key);
	    $res->{'encryption-key'} = $encryption_key;
	}
	$scfg->{'encryption-key'} = $decoded_key->{fingerprint} || 1;
    } else {
	pbs_delete_encryption_key($scfg, $storeid);
    }

    if (defined(my $master_key = delete $param{'master-pubkey'})) {
	die "'master-pubkey' can only be used together with 'encryption-key'\n"
	    if !defined($scfg->{'encryption-key'});

	my $decoded = decode_base64($master_key);
	pbs_set_master_pubkey($scfg, $storeid, $decoded);
	$scfg->{'master-pubkey'} = 1;
    } else {
	pbs_delete_master_pubkey($scfg, $storeid);
    }

    return $res;
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    my $res = {};

    if (exists($param{password})) {
	if (defined($param{password})) {
	    pbs_set_password($scfg, $storeid, $param{password});
	} else {
	    pbs_delete_password($scfg, $storeid);
	}
    }

    if (exists($param{'encryption-key'})) {
	if (defined(my $encryption_key = delete($param{'encryption-key'}))) {
	    my $decoded_key;
	    if ($encryption_key eq 'autogen') {
		$res->{'encryption-key'} = $autogen_encryption_key->($scfg, $storeid);
		$decoded_key = decode_json($res->{'encryption-key'});
	    } else {
		$decoded_key = eval { decode_json($encryption_key) };
		if ($@ || !exists($decoded_key->{data})) {
		    die "Value does not seems like a valid, JSON formatted encryption key!\n";
		}
		pbs_set_encryption_key($scfg, $storeid, $encryption_key);
		$res->{'encryption-key'} = $encryption_key;
	    }
	    $scfg->{'encryption-key'} = $decoded_key->{fingerprint} || 1;
	} else {
	    pbs_delete_encryption_key($scfg, $storeid);
	    delete $scfg->{'encryption-key'};
	}
    }

    if (exists($param{'master-pubkey'})) {
	if (defined(my $master_key = delete($param{'master-pubkey'}))) {
	    my $decoded = decode_base64($master_key);

	    pbs_set_master_pubkey($scfg, $storeid, $decoded);
	    $scfg->{'master-pubkey'} = 1;
	} else {
	    pbs_delete_master_pubkey($scfg, $storeid);
	}
    }

    return $res;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    pbs_delete_password($scfg, $storeid);
    pbs_delete_encryption_key($scfg, $storeid);
    pbs_delete_master_pubkey($scfg, $storeid);

    return;
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

    my $repo = PVE::PBSClient::get_repository($scfg);

    # artifical url - we currently do not use that anywhere
    my $path = "pbs://$repo/$name";

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

my sub snapshot_files_encrypted {
    my ($files) = @_;
    return 0 if !$files;

    my $any;
    my $all = 1;
    for my $file (@$files) {
	my $fn = $file->{filename};
	next if $fn eq 'client.log.blob' || $fn eq 'index.json.blob';

	my $crypt = $file->{'crypt-mode'};

	$all = 0 if !$crypt || $crypt ne 'encrypt';
	$any ||= defined($crypt) && $crypt eq 'encrypt';
    }
    return $any && $all;
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
	next if defined($vmid) && $bid ne $vmid;

	my $volid = print_volid($storeid, $btype, $bid, $epoch);

	my $info = {
	    volid => $volid,
	    format => "pbs-$btype",
	    size => $size,
	    content => 'backup',
	    vmid => int($bid),
	    ctime => $epoch,
	};

	$info->{verification} = $item->{verification} if defined($item->{verification});
	$info->{notes} = $item->{comment} if defined($item->{comment});
	if (defined($item->{fingerprint})) {
	    $info->{encrypted} = $item->{fingerprint};
	} elsif (snapshot_files_encrypted($item->{files})) {
	    $info->{encrypted} = '1';
	}

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

# TODO: use a client with native rust/proxmox-backup bindings to profit from
# API schema checks and types
my sub pbs_api_connect {
    my ($scfg, $password) = @_;

    my $params = {};

    my $user = $scfg->{username} // 'root@pam';

    if (my $tokenid = PVE::AccessControl::pve_verify_tokenid($user, 1)) {
	$params->{apitoken} = "PBSAPIToken=${tokenid}:${password}";
    } else {
	$params->{password} = $password;
	$params->{username} = $user;
    }

    if (my $fp = $scfg->{fingerprint}) {
	$params->{cached_fingerprints}->{uc($fp)} = 1;
    }

    my $conn = PVE::APIClient::LWP->new(
	%$params,
	host => $scfg->{server},
	port => $scfg->{port} // 8007,
	timeout => 7, # cope with a 401 (3s api delay) and high latency
	cookie_name => 'PBSAuthCookie',
    );

    return $conn;
}

# can also be used for not (yet) added storages, pass $scfg with
# {
#   server
#   user
#   port          (optional default to 8007)
#   fingerprint   (optional for trusted certs)
# }
sub scan_datastores {
    my ($scfg, $password) = @_;

    my $conn = pbs_api_connect($scfg, $password);

    my $response = eval { $conn->get('/api2/json/admin/datastore', {}) };
    die "error fetching datastores - $@" if $@;

    return $response;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $password = pbs_get_password($scfg, $storeid);

    my $datastores = eval { scan_datastores($scfg, $password) };
    die "$storeid: $@" if $@;

    my $datastore = $scfg->{datastore};

    for my $ds (@$datastores) {
	if ($ds->{store} eq $datastore) {
	    return 1;
	}
    }

    die "$storeid: Cannot find datastore '$datastore', check permissions and existance!\n";
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

sub get_volume_notes {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my (undef, $name,  undef, undef, undef, undef, $format) = $class->parse_volname($volname);

    my $data = run_client_cmd($scfg, $storeid, "snapshot", [ "notes", "show", $name ]);

    return $data->{notes};
}

sub update_volume_notes {
    my ($class, $scfg, $storeid, $volname, $notes, $timeout) = @_;

    my (undef, $name,  undef, undef, undef, undef, $format) = $class->parse_volname($volname);

    run_client_cmd($scfg, $storeid, "snapshot", [ "notes", "update", $name, $notes ], 1);

    return undef;
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
