package PVE::Storage::ESXiPlugin;

use strict;
use warnings;

use Fcntl qw(F_GETFD F_SETFD FD_CLOEXEC);
use JSON qw(from_json);
use POSIX ();
use File::Path qw(mkpath remove_tree);

use PVE::Network;
use PVE::Systemd;
use PVE::Tools qw(file_get_contents file_set_contents run_command);

use base qw(PVE::Storage::Plugin);

my $ESXI_LIST_VMS = '/usr/lib/pve-esxi-import-tools/listvms.py';
my $ESXI_MOUNT = '/usr/lib/x86_64-linux-gnu/pve-esxi-import-tools/esxi-folder-fuse';
my $ESXI_PRIV_DIR = '/etc/pve/priv/import/esxi';

#
# Configuration
#

sub type {
    return 'esxi';
}

sub plugindata {
    return {
	content => [ { import => 1, images => 1 }, { import => 1, images => 1 }],
	format => [ { raw => 1, qcow2 => 1, vmdk => 1 } , 'raw' ],
    };
}

sub properties {
    return {};
}

sub options {
    return {
	nodes => { optional => 1 },
	shared => { optional => 1 },
	disable => { optional => 1 },
	content => { optional => 1 },
	# FIXME: bwlimit => { optional => 1 },
	server => {},
	username => {},
	password => { optional => 1},
   };
}

sub esxi_cred_file_name {
    my ($storeid) = @_;
    return "/etc/pve/priv/storage/${storeid}.pw";
}

sub esxi_delete_credentials {
    my ($storeid) = @_;

    if (my $cred_file = get_cred_file($storeid)) {
	unlink($cred_file) or warn "removing esxi credientials '$cred_file' failed: $!\n";
    }
}

sub esxi_set_credentials {
    my ($password, $storeid) = @_;

    my $cred_file = esxi_cred_file_name($storeid);
    mkdir "/etc/pve/priv/storage";

    PVE::Tools::file_set_contents($cred_file, $password);

    return $cred_file;
}

sub get_cred_file {
    my ($storeid) = @_;

    my $cred_file = esxi_cred_file_name($storeid);

    if (-e $cred_file) {
	return $cred_file;
    }
    return undef;
}

#
# Dealing with the esxi API.
#

my sub run_path : prototype($) {
    my ($storeid) = @_;
    return "/run/pve/import/esxi/$storeid";
}

# "public" because it is needed by the VMX package
sub mount_dir : prototype($) {
    my ($storeid) = @_;
    return run_path($storeid) . "/mnt";
}

my sub check_esxi_import_package : prototype() {
    die "pve-esxi-import-tools package not installed, cannot proceed\n"
	if !-e $ESXI_LIST_VMS;
}

sub get_manifest : prototype($$$;$) {
    my ($class, $storeid, $scfg, $force_query) = @_;

    my $rundir = run_path($storeid);
    my $manifest_file = "$rundir/manifest.json";

    if (!$force_query && -e $manifest_file) {
	return PVE::Storage::ESXiPlugin::Manifest->new(
	    file_get_contents($manifest_file),
	);
    }

    check_esxi_import_package();

    my $host = $scfg->{server};
    my $user = $scfg->{username};
    my $pwfile = esxi_cred_file_name($storeid);
    my $json = '';
    run_command(
	[$ESXI_LIST_VMS, $host, $user, $pwfile],
	outfunc => sub { $json .= $_[0] . "\n" },
    );

    my $result = PVE::Storage::ESXiPlugin::Manifest->new($json);
    mkpath($rundir);
    file_set_contents($manifest_file, $json);

    return $result;
}

my sub scope_name_base : prototype($) {
    my ($storeid) = @_;
    return "pve-esxi-fuse-" . PVE::Systemd::escape_unit($storeid);
}

my sub is_mounted : prototype($) {
    my ($storeid) = @_;

    my $scope_name_base = scope_name_base($storeid);
    return PVE::Systemd::is_unit_active($scope_name_base . '.scope');
}

sub esxi_mount : prototype($$$;$) {
    my ($class, $storeid, $scfg, $force_requery) = @_;

    return if !$force_requery && is_mounted($storeid);

    $class->get_manifest($storeid, $scfg, $force_requery);

    my $rundir = run_path($storeid);
    my $manifest_file = "$rundir/manifest.json";
    my $mount_dir = mount_dir($storeid);
    if (!mkdir($mount_dir)) {
	die "mkdir failed on $mount_dir $!\n" if !$!{EEXIST};
    }

    my $scope_name_base = scope_name_base($storeid);
    my $user = $scfg->{username};
    my $host = $scfg->{server};
    my $pwfile = esxi_cred_file_name($storeid);

    pipe(my $rd, my $wr) or die "failed to create pipe: $!\n";

    my $pid = fork();
    die "fork failed: $!\n" if !defined($pid);
    if (!$pid) {
	eval {
	    undef $rd;
	    POSIX::setsid();
	    PVE::Systemd::enter_systemd_scope(
		$scope_name_base,
		"Proxmox VE FUSE mount for ESXi storage $storeid (server $host)",
	    );

	    my $flags = fcntl($wr, F_GETFD, 0)
		// die "failed to get file descriptor flags: $!\n";
	    fcntl($wr, F_SETFD, $flags & ~FD_CLOEXEC)
		// die "failed to remove CLOEXEC flag from fd: $!\n";
	    # FIXME: use the user/group options!
	    exec {$ESXI_MOUNT}
		$ESXI_MOUNT,
		'-o', 'allow_other',
		'--ready-fd', fileno($wr),
		'--user', $user,
		'--password-file', $pwfile,
		$host,
		$manifest_file,
		$mount_dir;
	    die "exec failed: $!\n";
	};
	if (my $err = $@) {
	    print {$wr} "ERROR: $err";
	}
	POSIX::_exit(1);
    };
    undef $wr;

    my $result = do { local $/ = undef; <$rd> };
    if ($result =~ /^ERROR: (.*)$/) {
	die "$1\n";
    }

    if (waitpid($pid, POSIX::WNOHANG) == $pid) {
	die "failed to spawn fuse mount, process exited with status $?\n";
    }
}

sub esxi_unmount : prototype($$$) {
    my ($class, $storeid, $scfg) = @_;

    my $scope_name_base = scope_name_base($storeid);
    my $scope = "${scope_name_base}.scope";
    my $mount_dir = mount_dir($storeid);

    my %silence_std_outs = (outfunc => sub {}, errfunc => sub {});
    eval { run_command(['/bin/systemctl', 'reset-failed', $scope], %silence_std_outs) };
    eval { run_command(['/bin/systemctl', 'stop', $scope], %silence_std_outs) };
    run_command(['/bin/umount', $mount_dir]);
}

my sub get_raw_vmx : prototype($$$$%) {
    my ($class, $storeid, $scfg, $vm, %opts) = @_;

    my ($datacenter, $mount, $force_requery) = @opts{qw(datacenter mount force-requery)};
    my $mntdir = mount_dir($storeid);
    my $manifest = $class->get_manifest($storeid, $scfg, $force_requery);

    $datacenter //= $manifest->datacenter_for_vm($vm);
    die "no such VM\n" if !defined($datacenter);

    my $dc = $manifest->{$datacenter}
	or die "no such datacenter\n";
    my $info = $dc->{vms}->{$vm}
	or die "no such vm\n";
    my ($datastore, $path) = $info->{config}->@{qw(datastore path)};

    if ($mount && !is_mounted($storeid)) {
	$class->esxi_mount($storeid, $scfg, $force_requery);
    }

    my $contents = file_get_contents("$mntdir/$datacenter/$datastore/$path");
    return wantarray ? ($datacenter, $contents) : $contents;
}

# Split a path into (datacenter, datastore, path)
sub split_path : prototype($) {
    my ($path) = @_;
    if ($path =~ m!^([^/]+)/([^/]+)/(.+)$!) {
	return ($1, $2, $3);
    }
    return;
}

sub get_import_metadata : prototype($$$$) {
    my ($class, $scfg, $volname, $storeid) = @_;

    if ($volname !~ m!^([^/]+)/.*\.vmx$!) {
	die "volume '$volname' does not look like an importable vm config\n";
    }

    my $vmx_path = $class->path($scfg, $volname, $storeid, undef);
    if (!is_mounted($storeid)) {
	die "storage '$storeid' is not activated\n";
    }

    my $manifest = $class->get_manifest($storeid, $scfg, 0);
    my $contents = file_get_contents($vmx_path);
    return PVE::Storage::ESXiPlugin::VMX->parse(
	$storeid,
	$scfg,
	$volname,
	$contents,
	$manifest,
    );
}

# Returns a size in bytes, this is a helper for already-mounted files.
sub query_vmdk_size : prototype($;$) {
    my ($filename, $timeout) = @_;

    my $json = eval {
	my $json = '';
	run_command(['/usr/bin/qemu-img', 'info', '--output=json', $filename],
	    timeout => $timeout,
	    outfunc => sub { $json .= $_[0]; },
	    errfunc => sub { warn "$_[0]\n"; }
	);
	from_json($json)
    };
    warn $@ if $@;

    return int($json->{'virtual-size'});
}

#
# Storage API implementation
#

sub on_add_hook {
    my ($class, $storeid, $scfg, %sensitive) = @_;

    my $password = $sensitive{password};
    die "missing password\n" if !defined($password);
    esxi_set_credentials($password, $storeid);

    return;
}

sub on_update_hook {
    my ($class, $storeid, $scfg, %sensitive) = @_;

    return if !exists($sensitive{password});

    if (defined($sensitive{password})) {
	esxi_set_credentials($sensitive{password}, $storeid);
    } else {
	esxi_delete_credentials($storeid);
    }

    return;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    esxi_delete_credentials($storeid);

    return;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $class->esxi_mount($storeid, $scfg, 0);
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $class->esxi_unmount($storeid, $scfg);
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    # FIXME: maybe check if it exists?
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    return 1;
}

sub check_connection {
    my ($class, $storeid, $scfg) = @_;

    return PVE::Network::tcp_ping($scfg->{server}, 443, 2);
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    return (0, 0, 0, 0);
}

sub parse_volname {
    my ($class, $volname) = @_;

    # it doesn't really make sense tbh, we can't return an owner, the format
    # may be a 'vmx' (config), the paths are arbitrary...

    die "failed to parse volname '$volname'\n"
	if $volname !~ m!^([^/]+)/([^/]+)/(.+)$!;

    return ('import', $volname) if $volname =~ /\.vmx$/;

    my $format = 'raw';
    $format = 'vmdk'  if $volname =~ /\.vmdk/;
    return ('images', $volname, 0, undef, undef, undef, $format);
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    return [];
}

sub list_volumes {
    my ($class, $storeid, $scfg, $vmid, $content_types) = @_;

    return if !grep { $_ eq 'import' } @$content_types;

    my $data = $class->get_manifest($storeid, $scfg, 0);

    my $res = [];
    for my $dc_name (keys $data->%*) {
	my $dc = $data->{$dc_name};
	my $vms = $dc->{vms};
	for my $vm_name (keys $vms->%*) {
	    my $vm = $vms->{$vm_name};
	    my $ds_name = $vm->{config}->{datastore};
	    my $path = $vm->{config}->{path};
	    push @$res, {
		content => 'import',
		format => 'vmx',
		name => $vm_name,
		volid => "$storeid:$dc_name/$ds_name/$path",
		size => 0,
	    };
	}
    }

    return $res;
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "cloning images is not supported for $class\n";
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "creating base images is not supported for $class\n";
}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    die "storage '$class' does not support snapshots\n" if defined $snapname;

    # FIXME: activate/mount:
    return mount_dir($storeid) . '/' . $volname;
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "creating images is not supported for $class\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    die "deleting images is not supported for $class\n";
}

sub rename_volume {
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;

    die "renaming volumes is not supported for $class\n";
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    # FIXME: maybe we can support raw+size via `qemu-img dd`?

    die "exporting not supported for $class\n";
}

sub volume_export {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots) = @_;

    # FIXME: maybe we can support raw+size via `qemu-img dd`?

    die "exporting not supported for $class\n";
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    die "importing not supported for $class\n";
}

sub volume_import {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots, $allow_rename) = @_;

    die "importing not supported for $class\n";
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    die "resizing volumes is not supported for $class\n";
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    return 0 if $volname =~ /\.vmx$/;

    my $filename = $class->path($scfg, $volname, $storeid, undef);
    return PVE::Storage::Plugin::file_size_info($filename, $timeout);
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    die "creating snapshots is not supported for $class\n";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    die "deleting snapshots is not supported for $class\n";
}
sub volume_snapshot_info {

    my ($class, $scfg, $storeid, $volname) = @_;

    die "getting snapshot information is not supported for $class";
}

sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap, $blockers) = @_;

    return 0;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running, $opts) = @_;

    return undef if defined($snapname) || $volname =~ /\.vmx$/;
    return 1 if $feature eq 'copy';
    return undef;
}

sub get_subdir {
    my ($class, $scfg, $vtype) = @_;

    die "no subdirectories available for storage $class\n";
}

package PVE::Storage::ESXiPlugin::Manifest;

use strict;
use warnings;

use JSON qw(from_json);

sub new : prototype($$) {
    my ($class, $data) = @_;

    my $json = from_json($data);

    return bless $json, $class;
}

sub datacenter_for_vm {
    my ($self, $vm) = @_;

    for my $dc_name (sort keys %$self) {
	my $dc = $self->{$dc_name};
	return $dc_name if exists($dc->{vms}->{$vm});
    }

    return;
}

sub datastore_for_vm {
    my ($self, $vm, $datacenter) = @_;

    my @dc_names = defined($datacenter) ? ($datacenter) : keys %$self;
    for my $dc_name (@dc_names) {
	my $dc = $self->{$dc_name}
	    or die "no such datacenter '$datacenter'\n";
	if (defined(my $vm = $dc->{vms}->{$vm})) {
	    return $vm->{config}->{datastore};
	}
    }

    return;
}

sub resolve_path {
    my ($self, $path) = @_;

    if ($path !~ m|^/|) {
	return wantarray ? (undef, undef, $path) : $path;
    }

    for my $dc_name (sort keys %$self) {
	my $dc = $self->{$dc_name};

	my $datastores = $dc->{datastores};

	for my $ds_name (keys %$datastores) {
	    my $ds_path = $datastores->{$ds_name};
	    if (substr($path, 0, length($ds_path)) eq $ds_path) {
		my $relpath = substr($path, length($ds_path));
		return wantarray ? ($dc_name, $ds_name, $relpath) : $relpath;
	    }
	}
    }

    return;
}

sub config_path_for_vm {
    my ($self, $vm, $datacenter) = @_;

    my @dc_names = defined($datacenter) ? ($datacenter) : keys %$self;
    for my $dc_name (@dc_names) {
	my $dc = $self->{$dc_name}
	    or die "no such datacenter '$datacenter'\n";

	my $vm = $dc->{vms}->{$vm}
	    or next;

	my $cfg = $vm->{config};
	if (my (undef, $ds_name, $path) = $self->resolve_path($cfg->{path})) {
	    $ds_name //= $cfg->{datastore};
	    return ($dc_name, $ds_name, $path);
	}

	die "failed to resolve path for vm '$vm' "
	  ."($dc_name, $cfg->{datastore}, $cfg->{path})\n";
    }

    die "no such vm '$vm'\n";
}

# Since paths in the vmx file are relative to the vmx file itself, this helper
# provides a way to resolve paths which are relative based on the config file
# path, while also resolving absolute paths without the vm config.
sub resolve_path_for_vm {
    my ($self, $vm, $path, $datacenter) = @_;

    if ($path =~ m|^/|) {
	if (my ($disk_dc, $disk_ds, $disk_path) = $self->resolve_path($path)) {
	    return "$disk_dc/$disk_ds/$disk_path";
	}
	die "failed to resolve path '$path' for vm '$vm'\n";
    }

    my ($cfg_dc, $cfg_ds, $cfg_path) = $self->config_path_for_vm($vm, $datacenter)
	or die "failed to resolve vm config path for '$vm'\n";
    $cfg_path =~ s|/[^/]+$||;

    return "$cfg_dc/$cfg_ds/$cfg_path/$path";
}

sub resolve_path_relative_to {
    my ($self, $vmx_path, $path) = @_;

    if ($path =~ m|^/|) {
	if (my ($disk_dc, $disk_ds, $disk_path) = $self->resolve_path($path)) {
	    return "$disk_dc/$disk_ds/$disk_path";
	}
	die "failed to resolve path '$path'\n";
    }

    my ($rel_dc, $rel_ds, $rel_path) = PVE::Storage::ESXiPlugin::split_path($vmx_path)
	or die "bad path '$vmx_path'\n";
    $rel_path =~ s|/[^/]+$||;

    return "$rel_dc/$rel_ds/$rel_path/$path";
}

package PVE::Storage::ESXiPlugin::VMX;

use strict;
use warnings;
use feature 'fc';

# FIXME: see if vmx files can actually have escape sequences in their quoted values?
my sub unquote : prototype($) {
    my ($value) = @_;
    $value =~ s/^\"(.*)\"$/$1/s
	or $value =~ s/^\'(.*)\'$/$1/s;
    return $value;
}

sub parse : prototype($$$$$$) {
    my ($class, $storeid, $scfg, $vmx_path, $vmxdata, $manifest) = @_;

    my $conf = {};

    for my $line (split(/\n/, $vmxdata)) {
	$line =~ s/^\s+//;
	$line =~ s/\s+$//;
	next if $line !~ /^(\S+)\s*=\s*(.+)$/;
	my ($key, $value) = ($1, $2);

	$value = unquote($value);

	$conf->{$key} = $value;
    }

    $conf->{'pve.storeid'} = $storeid;
    $conf->{'pve.storage.config'} = $scfg;
    $conf->{'pve.vmx.path'} = $vmx_path;
    $conf->{'pve.manifest'} = $manifest;

    return bless $conf, $class;
}

sub storeid { $_[0]->{'pve.storeid'} }
sub scfg { $_[0]->{'pve.storage.config'} }
sub vmx_path { $_[0]->{'pve.vmx.path'} }
sub manifest { $_[0]->{'pve.manifest'} }

# (Also used for the fileName config key...)
sub is_disk_entry : prototype($) {
    my ($id) = @_;
    if ($id =~ /^(scsi|ide|sata|nvme)(\d+:\d+)(:?\.fileName)?$/) {
	return ($1, $2);
    }
    return;
}

sub is_cdrom {
    my ($self, $bus, $slot) = @_;
    if (my $type = $self->{"${bus}${slot}.deviceType"}) {
	return $type =~ /cdrom/;
    }
    return;
}

sub for_each_disk {
    my ($self, $code) = @_;

    for my $key (sort keys %$self) {
	my ($bus, $slot) = is_disk_entry($key)
	    or next;
	my $kind = $self->is_cdrom($bus, $slot) ? 'cdrom' : 'disk';

	my $file = $self->{$key};

	my ($maj, $min) = split(/:/, $slot, 2);
	my $vdev = $self->{"${bus}${maj}.virtualDev"}; # may of course be undef...

	$code->($bus, $slot, $file, $vdev, $kind);
    }

    return;
}

sub for_each_netdev {
    my ($self, $code) = @_;

    my $found_devs = {};
    for my $key (keys %$self) {
	next if $key !~ /^ethernet(\d+)\.(.+)$/;
	my ($slot, $opt) = ($1, $2);

	my $dev = ($found_devs->{$slot} //= {});
	$dev->{$opt} = $self->{$key};
    }

    for my $id (sort keys %$found_devs) {
	my $dev = $found_devs->{$id};

	next if ($dev->{present} // '') ne 'TRUE';

	my $ty = $dev->{addressType};
	my $mac = $dev->{address};
	if ($ty && fc($ty) eq fc('generated')) {
	    $mac = $dev->{generatedAddress} // $mac;
	}

	$code->($id, $dev, $mac);
    }

    return;
}

sub firmware {
    my ($self) = @_;
    my $fw = $self->{firmware};
    return 'efi' if $fw && fc($fw) eq fc('efi');
    return 'bios';
}

# This is in MB
sub memory {
    my ($self) = @_;

    return $self->{memSize};
}

# CPU info is stored as a maximum ('numvcpus') and a core-per-socket count.
# We return a (cores, sockets) tuple the way want it for PVE.
sub cpu_info {
    my ($self) = @_;

    my $cps = int($self->{'cpuid.coresPerSocket'} // 1);
    my $max = int($self->{numvcpus} // $cps);

    return ($cps, ($max / $cps));
}

# FIXME: Test all possible values esxi creates?
sub is_windows {
    my ($self) = @_;

    my $guest = $self->{guestOS} // return;
    return 1 if $guest =~ /^win/i;
    return;
}

my %guest_types = (
    dos                     => 'other',
    winNetBusiness          => 'w2k3',
    windows9                => 'win10',
    'windows9-64'           => 'win10',
    'windows11-64'          => 'win11',
    'windows12-64'          => 'win11', # FIXME / win12?
    win2000AdvServ          => 'w2k',
    win2000Pro              => 'w2k',
    win2000Serv             => 'w2k',
    win31                   => 'other',
    windows7                => 'win7',
    'windows7-64'           => 'win7',
    windows8                => 'win8',
    'windows8-64'           => 'win8',
    win95                   => 'other',
    win98                   => 'other',
    winNT                   => 'wxp', # ?
    winNetEnterprise        => 'w2k3',
    'winNetEnterprise-64'   => 'w2k3',
    winNetDatacenter        => 'w2k3',
    'winNetDatacenter-64'   => 'w2k3',
    winNetStandard          => 'w2k3',
    'winNetStandard-64'     => 'w2k3',
    winNetWeb               => 'w2k3',
    winLonghorn             => 'w2k8',
    'winLonghorn-64'        => 'w2k8',
    'windows7Server-64'     => 'w2k8',
    'windows8Server-64'     => 'win8',
    'windows9Server-64'     => 'win10',
    'windows2019srv-64'     => 'win10',
    'windows2019srvNext-64' => 'win11',
    'windows2022srvNext-64' => 'win11', # FIXME / win12?
    winVista                => 'wvista',
    'winVista-64'           => 'wvista',
    winXPPro                => 'wxp',
    'winXPPro-64'           => 'wxp',
);

# Best effort translation from vmware guest os type to pve.
# Returns a tuple: `(pve-type, is_windows)`
sub guest_type {
    my ($self) = @_;

    if (defined(my $guest = $self->{guestOS})) {
	if (defined(my $known = $guest_types{$guest})) {
	    return ($known, 1);
	}
	# This covers all the 'Mac OS' types AFAICT
	return ('other', 0) if $guest =~ /^darwin/;
    }

    # otherwise we'll just go with l26 defaults because why not...
    return ('l26', 0);
}

sub smbios1_uuid {
    my ($self) = @_;

    my $uuid = $self->{'uuid.bios'};

    return if !defined($uuid);

    # vmware stores space separated bytes and has 1 dash in the middle...
    $uuid =~ s/[^0-9a-fA-f]//g;

    if ($uuid =~ /^
	([0-9a-fA-F]{8})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{12})
	$/x)
    {
	return "$1-$2-$3-$4-$5";
    }
    return;
}

# This builds arguments for the `create` api call for this config.
sub get_create_args {
    my ($self, $default_storage) = @_;

    $default_storage //= 'local';

    my $storeid = $self->storeid;
    my $manifest = $self->manifest;

    my $create_args = {};

    my ($cores, $sockets) = $self->cpu_info();
    $create_args->{cores} = $cores if $cores != 1;
    $create_args->{sockets} = $sockets if $sockets != 1;

    my $firmware = $self->firmware;
    if ($firmware eq 'efi') {
	$create_args->{bios} = 'ovmf';
	$create_args->{efidisk0} = "$default_storage:1";
    } else {
	$create_args->{bios} = 'seabios';
    }

    my $memory = $self->memory;
    $create_args->{memory} = $memory;

    my $default_scsihw;
    my $scsihw;
    my $set_scsihw = sub {
	if (defined($scsihw) && $scsihw ne $_[0]) {
	    warn "multiple different SCSI hardware types are not supported\n";
	    return;
	}
	$scsihw = $_[0];
    };

    my ($ostype, $is_windows) = $self->guest_type();
    $create_args->{ostype} //= $ostype if defined($ostype);
    if ($ostype eq 'l26') {
	$default_scsihw = 'virtio-scsi-single';
    }

    $self->for_each_netdev(sub {
	my ($id, $dev, $mac) = @_;
	$mac //= '';
	my $model = $dev->{virtualDev} // 'vmxnet3';

	my $param = "model=$model";
	$param .= ",macaddr=$mac" if length($mac);
	$param .= ",firewall=1";
	$create_args->{"net$id"} = $param;
    });

    my %counts = ( scsi => 0, sata => 0, ide => 0 );

    my $mntdir = PVE::Storage::ESXiPlugin::mount_dir($storeid);

    my $boot_order = '';

    # we deal with nvme disks in a 2nd go-around since we currently don't
    # support nvme disks and instead just add them as additional scsi
    # disks.
    my @nvmes;
    my $add_disk = sub {
	my ($bus, $slot, $file, $devtype, $kind, $do_nvmes) = @_;

	my $vmbus = $bus;
	if ($do_nvmes) {
	    $bus = 'scsi';
	} elsif ($bus eq 'nvme') {
	    push @nvmes, [$slot, $file, $devtype, $kind];
	    return;
	}

	my $path = eval { $manifest->resolve_path_relative_to($self->vmx_path, $file) };
	return if !defined($path);

	# my $fullpath = "$mntdir/$path";
	# return if !-e $fullpath;

	if ($devtype && $devtype =~ /^lsi/i) {
	    $set_scsihw->('lsi');
	}

	my $count = $counts{$bus}++;
	$create_args->{"${bus}${count}"} = "$default_storage:0,import-from=$storeid:$path";

	$boot_order .= ';' if length($boot_order);
	$boot_order .= $bus.$count;
    };
    $self->for_each_disk($add_disk);
    for my $nvme (@nvmes) {
	my ($slot, $file, $devtype, $kind) = @$nvme;
	$add_disk->('nvme', $slot, $file, $devtype, $kind, 1);
    }

    $scsihw //= $default_scsihw;
    if ($firmware eq 'efi' && !defined($scsihw) || $scsihw =~ /^lsi/) {
	if ($is_windows) {
	    $scsihw = 'pvscsi';
	} else {
	    $scsihw = 'virtio-scsi-single';
	}
    }
    $create_args->{scsihw} = $scsihw;

    $create_args->{boot} = "order=$boot_order";

    if (defined(my $smbios1_uuid = $self->smbios1_uuid())) {
	$create_args->{smbios1} = "uuid=$smbios1_uuid";
    }

    if (defined(my $name = $self->{displayName})) {
	# name in pve is a 'dns-name', so... clean it
	$name =~ s/\s/-/g;
	$name =~ s/[^a-zA-Z0-9\-.]//g;
	$name =~ s/^[.-]+//;
	$name =~ s/[.-]+$//;
	$create_args->{name} = $name if length($name);
    }

    return $create_args;
}

1;
