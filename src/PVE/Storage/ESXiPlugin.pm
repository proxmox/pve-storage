package PVE::Storage::ESXiPlugin;

use strict;
use warnings;

use Fcntl qw(F_GETFD F_SETFD FD_CLOEXEC);
use File::Path qw(mkpath remove_tree);
use JSON qw(from_json);
use Net::IP;
use POSIX ();

use PVE::Network;
use PVE::Systemd;
use PVE::Tools qw(file_get_contents file_set_contents run_command);

use base qw(PVE::Storage::Plugin);

my $ESXI_LIST_VMS = '/usr/libexec/pve-esxi-import-tools/listvms.py';
my $ESXI_FUSE_TOOL = '/usr/libexec/pve-esxi-import-tools/esxi-folder-fuse';
my $ESXI_PRIV_DIR = '/etc/pve/priv/import/esxi';

#
# Configuration
#

sub type {
    return 'esxi';
}

sub plugindata {
    return {
        content => [{ import => 1 }, { import => 1 }],
        format => [{ raw => 1, qcow2 => 1, vmdk => 1 }, 'raw'],
        'sensitive-properties' => { password => 1 },
    };
}

sub properties {
    return {
        'skip-cert-verification' => {
            description =>
                'Disable TLS certificate verification, only enable on fully trusted networks!',
            type => 'boolean',
            default => 'false',
        },
    };
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
        password => { optional => 1 },
        'skip-cert-verification' => { optional => 1 },
        port => { optional => 1 },
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

my sub is_old : prototype($) {
    my ($file) = @_;
    my $mtime = (CORE::stat($file))[9];
    return !defined($mtime) || ($mtime + 30) < CORE::time();
}

sub get_manifest : prototype($$$;$) {
    my ($class, $storeid, $scfg, $force_query) = @_;

    my $rundir = run_path($storeid);
    my $manifest_file = "$rundir/manifest.json";

    $force_query ||= is_old($manifest_file);

    if (!$force_query && -e $manifest_file) {
        return PVE::Storage::ESXiPlugin::Manifest->new(
            file_get_contents($manifest_file),
        );
    }

    check_esxi_import_package();

    my @extra_params;
    push @extra_params, '--skip-cert-verification' if $scfg->{'skip-cert-verification'};
    if (my $port = $scfg->{port}) {
        push @extra_params, '--port', $port;
    }
    my $host = $scfg->{server};
    my $user = $scfg->{username};
    my $pwfile = esxi_cred_file_name($storeid);
    my $json = '';
    my $errmsg = '';
    eval {
        run_command(
            [$ESXI_LIST_VMS, @extra_params, $host, $user, $pwfile],
            outfunc => sub { $json .= $_[0] . "\n" },
            errfunc => sub { $errmsg .= $_[0] . "\n" },
        );
    };
    if ($@) {
        # propagate listvms error output if any, otherwise use the error from run_command
        die $errmsg || $@;
    }

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

    my $hostport = $host;
    $hostport = "[$hostport]" if Net::IP::ip_is_ipv6($host);
    if (my $port = $scfg->{port}) {
        $hostport .= ":$port";
    }

    pipe(my $rd, my $wr) or die "failed to create pipe: $!\n";

    my $pid = fork();
    die "fork failed: $!\n" if !defined($pid);
    if (!$pid) {
        eval {
            undef $rd;

            # Double fork to properly daemonize
            POSIX::setsid() or die "failed to create new session: $!\n";
            my $pid2 = fork();
            die "second fork failed: $!\n" if !defined($pid2);

            if ($pid2) {
                # First child exits immediately
                POSIX::_exit(0);
            }
            # Second child (grandchild) enters systemd scope
            PVE::Systemd::enter_systemd_scope(
                $scope_name_base,
                "Proxmox VE FUSE mount for ESXi storage $storeid (server $host)",
            );

            my @extra_params;
            push @extra_params, '--skip-cert-verification' if $scfg->{'skip-cert-verification'};

            my $flags = fcntl($wr, F_GETFD, 0)
                // die "failed to get file descriptor flags: $!\n";
            fcntl($wr, F_SETFD, $flags & ~FD_CLOEXEC)
                // die "failed to remove CLOEXEC flag from fd: $!\n";
            exec {$ESXI_FUSE_TOOL}
                $ESXI_FUSE_TOOL,
                @extra_params,
                '--change-user', 'nobody',
                '--change-group', 'nogroup',
                '-o', 'allow_other',
                '--ready-fd', fileno($wr),
                '--user', $user,
                '--password-file', $pwfile,
                $hostport,
                $manifest_file,
                $mount_dir;
            die "exec failed: $!\n";
        };
        if (my $err = $@) {
            print {$wr} "ERROR: $err";
        }
        POSIX::_exit(1);
    }
    # Parent wait for first child to exit
    waitpid($pid, 0);
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

    my %silence_std_outs = (outfunc => sub { }, errfunc => sub { });
    eval { run_command(['/bin/systemctl', 'reset-failed', $scope], %silence_std_outs) };
    eval { run_command(['/bin/systemctl', 'stop', $scope], %silence_std_outs) };
    run_command(['/bin/umount', $mount_dir]);
}

# Split a path into (datacenter, datastore, path)
sub split_path : prototype($) {
    my ($path) = @_;
    if ($path =~ m!^([^/]+)/([^/]+)/(.+)$!) {
        return ($1, $2, $3);
    }
    return;
}

sub get_import_metadata : prototype($$$$$) {
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
    my $vmx = PVE::Storage::ESXiPlugin::VMX->parse(
        $storeid, $scfg, $volname, $contents, $manifest,
    );
    return $vmx->get_create_args();
}

# Returns a size in bytes, this is a helper for already-mounted files.
sub query_vmdk_size : prototype($;$) {
    my ($filename, $timeout) = @_;

    my $json = eval {
        my $json = '';
        run_command(
            ['/usr/bin/qemu-img', 'info', '--output=json', $filename],
            timeout => $timeout,
            outfunc => sub { $json .= $_[0]; },
            errfunc => sub { warn "$_[0]\n"; },
        );
        from_json($json);
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

    # FIXME: allow to actually determine this, e.g., through new $changed hash passed to the hook
    my $connection_detail_changed = 1;

    if (exists($sensitive{password})) {
        $connection_detail_changed = 1;
        if (defined($sensitive{password})) {
            esxi_set_credentials($sensitive{password}, $storeid);
        } else {
            esxi_delete_credentials($storeid);
        }
    }

    if ($connection_detail_changed) {
        # best-effort deactivate storage so that it can get re-mounted with updated params
        eval { $class->deactivate_storage($storeid, $scfg) };
        warn $@ if $@;
    }

    return;
}

sub on_delete_hook {
    my ($class, $storeid, $scfg) = @_;

    eval { $class->deactivate_storage($storeid, $scfg) };
    warn $@ if $@;

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

    my $rundir = run_path($storeid);
    remove_tree($rundir); # best-effort, ignore errors for now

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

    my $port = $scfg->{port} || 443;
    return PVE::Network::tcp_ping($scfg->{server}, $port, 2);
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $active = is_mounted($storeid) ? 1 : 0;

    return (0, 0, 0, $active);
}

sub parse_volname {
    my ($class, $volname) = @_;

    # it doesn't really make sense tbh, we can't return an owner, the format
    # may be a 'vmx' (config), the paths are arbitrary...

    die "failed to parse volname '$volname'\n"
        if $volname !~ m!^([^/]+)/([^/]+)/(.+)$!;

    return ('import', $volname, 0, undef, undef, undef, 'vmx') if $volname =~ /\.vmx$/;

    my $format = 'raw';
    $format = 'vmdk' if $volname =~ /\.vmdk$/;
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
            push @$res,
                {
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
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots)
        = @_;

    # FIXME: maybe we can support raw+size via `qemu-img dd`?

    die "exporting not supported for $class\n";
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    die "importing not supported for $class\n";
}

sub volume_import {
    my (
        $class,
        $scfg,
        $storeid,
        $fh,
        $volname,
        $format,
        $snapshot,
        $base_snapshot,
        $with_snapshots,
        $allow_rename,
    ) = @_;

    die "importing not supported for $class\n";
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;

    die "resizing volumes is not supported for $class\n";
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    if ($volname =~ /\.vmx$/) {
        return wantarray ? (0, 'vmx') : 0;
    }

    my $filename = $class->path($scfg, $volname, $storeid, undef);
    return PVE::Storage::Plugin::file_size_info($filename, $timeout, 'auto-detect');
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
            . "($dc_name, $cfg->{datastore}, $cfg->{path})\n";
    }

    die "no such vm '$vm'\n";
}

# Since paths in the vmx file are relative to the vmx file itself, this helper
# provides a way to resolve paths which are relative based on the config file
# path, while also resolving absolute paths without the vm config.
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

# Imports happen by the volume id which is a path to a VMX file.
# In order to find the vm's power state and disk capacity info, we need to find the
# VM the vmx file belongs to.
sub vm_for_vmx_path {
    my ($self, $vmx_path) = @_;

    my ($dc_name, $ds_name, $path) = PVE::Storage::ESXiPlugin::split_path($vmx_path);
    if (my $dc = $self->{$dc_name}) {
        my $vms = $dc->{vms};
        for my $vm_name (keys %$vms) {
            my $vm = $vms->{$vm_name};
            my $cfg_info = $vm->{config};
            if ($cfg_info->{datastore} eq $ds_name && $cfg_info->{path} eq $path) {
                return $vm;
            }
        }
    }
    return;
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
    if ($id =~ /^(scsi|ide|sata|nvme)(\d+:\d+)(:?\.file[nN]ame)?$/) {
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
        if ($ty && fc($ty) =~ /^(static|generated|vpx)$/) {
            $mac = $dev->{generatedAddress} // $mac;
        }

        $code->($id, $dev, $mac);
    }

    return;
}

sub for_each_serial {
    my ($self, $code) = @_;

    my $found_serials = {};
    for my $key (sort keys %$self) {
        next if $key !~ /^serial(\d+)\.(.+)$/;
        my ($slot, $opt) = ($1, $2);
        my $serial = ($found_serials->{$1} //= {});
        $serial->{$opt} = $self->{$key};
    }

    for my $id (sort { $a <=> $b } keys %$found_serials) {
        my $serial = $found_serials->{$id};

        next if ($serial->{present} // '') ne 'TRUE';

        $code->($id, $serial);
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

my %guest_types_windows = (
    'dos' => 'other',
    'longhorn' => 'w2k8',
    'winNetBusiness' => 'w2k3',
    'windows9' => 'win10',
    'windows9-64' => 'win10',
    'windows9srv' => 'win10',
    'windows9srv-64' => 'win10',
    'windows11-64' => 'win11',
    'windows12-64' => 'win11', # FIXME / win12?
    'win2000AdvServ' => 'w2k',
    'win2000Pro' => 'w2k',
    'win2000Serv' => 'w2k',
    'win31' => 'other',
    'windows7' => 'win7',
    'windows7-64' => 'win7',
    'windows8' => 'win8',
    'windows8-64' => 'win8',
    'win95' => 'other',
    'win98' => 'other',
    'winNT' => 'wxp', # ?
    'winNetEnterprise' => 'w2k3',
    'winNetEnterprise-64' => 'w2k3',
    'winNetDatacenter' => 'w2k3',
    'winNetDatacenter-64' => 'w2k3',
    'winNetStandard' => 'w2k3',
    'winNetStandard-64' => 'w2k3',
    'winNetWeb' => 'w2k3',
    'winLonghorn' => 'w2k8',
    'winLonghorn-64' => 'w2k8',
    'windows7Server-64' => 'w2k8',
    'windows8Server-64' => 'win8',
    'windows9Server-64' => 'win10',
    'windows2019srv-64' => 'win10',
    'windows2019srvNext-64' => 'win11',
    'windows2022srvNext-64' => 'win11', # FIXME / win12?
    'winVista' => 'wvista',
    'winVista-64' => 'wvista',
    'winXPPro' => 'wxp',
    'winXPPro-64' => 'wxp',
);

my %guest_types_other = (
    'freeBSD11' => 'other',
    'freeBSD11-64' => 'other',
    'freeBSD12' => 'other',
    'freeBSD12-64' => 'other',
    'freeBSD13' => 'other',
    'freeBSD13-64' => 'other',
    'freeBSD14' => 'other',
    'freeBSD14-64' => 'other',
    'freeBSD' => 'other',
    'freeBSD-64' => 'other',
    'os2' => 'other',
    'netware5' => 'other',
    'netware6' => 'other',
    'solaris10' => 'solaris',
    'solaris10-64' => 'solaris',
    'solaris11-64' => 'solaris',
    'other' => 'other',
    'other-64' => 'other',
    'openserver5' => 'other',
    'openserver6' => 'other',
    'unixware7' => 'other',
    'eComStation' => 'other',
    'eComStation2' => 'other',
    'solaris8' => 'solaris',
    'solaris9' => 'solaris',
    'vmkernel' => 'other',
    'vmkernel5' => 'other',
    'vmkernel6' => 'other',
    'vmkernel65' => 'other',
    'vmkernel7' => 'other',
    'vmkernel8' => 'other',
);

# Best effort translation from vmware guest os type to pve.
# Returns a tuple: `(pve-type, is_windows)`
sub guest_type {
    my ($self) = @_;
    if (defined(my $guest = $self->{guestOS})) {
        if (defined(my $known_windows = $guest_types_windows{$guest})) {
            return ($known_windows, 1);
        } elsif (defined(my $known_other = $guest_types_other{$guest})) {
            return ($known_other, 0);
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

    if (
        $uuid =~ /^
	([0-9a-fA-F]{8})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{4})
	([0-9a-fA-F]{12})
	$/x
    ) {
        return "$1-$2-$3-$4-$5";
    }
    return;
}

# This builds arguments for the `create` api call for this config.
sub get_create_args {
    my ($self) = @_;

    my $storeid = $self->storeid;
    my $manifest = $self->manifest;
    my $vminfo = $manifest->vm_for_vmx_path($self->vmx_path);

    my $create_args = {};
    my $create_disks = {};
    my $create_net = {};
    my $warnings = [];

    # NOTE: all types must be added to the return schema of the import-metadata API endpoint
    my $warn = sub {
        my ($type, %properties) = @_;
        push @$warnings, { type => $type, %properties };
    };

    my ($cores, $sockets) = $self->cpu_info();
    $create_args->{cores} = $cores if $cores != 1;
    $create_args->{sockets} = $sockets if $sockets != 1;

    my $firmware = $self->firmware;
    if ($firmware eq 'efi') {
        $create_args->{bios} = 'ovmf';
        $create_disks->{efidisk0} = 1;
        $warn->('efi-state-lost', key => "bios", value => "ovmf");
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

        my $param = { model => $model };
        $param->{macaddr} = $mac if length($mac);
        $create_net->{"net$id"} = $param;
    });

    my %counts = (scsi => 0, sata => 0, ide => 0);

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

        if ($devtype) {
            if ($devtype =~ /^lsi/i) {
                $set_scsihw->('lsi');
            } elsif ($devtype eq 'pvscsi') {
                $set_scsihw->('pvscsi'); # same name in pve
            }
        }

        my $disk_capacity;
        if (defined(my $diskinfo = $vminfo->{disks})) {
            my ($dc, $ds, $rel_path) = PVE::Storage::ESXiPlugin::split_path($path);
            for my $disk ($diskinfo->@*) {
                if ($disk->{datastore} eq $ds && $disk->{path} eq $rel_path) {
                    $disk_capacity = $disk->{capacity};
                    last;
                }
            }
        }

        my $count = $counts{$bus}++;
        if ($kind eq 'cdrom') {
            # We currently do not pass cdroms through via the esxi storage.
            # Users should adapt import these from the storages directly/manually.
            $create_args->{"${bus}${count}"} = "none,media=cdrom";
            # CD-ROM image will not get imported
            $warn->('cdrom-image-ignored', key => "${bus}${count}", value => "$storeid:$path");
        } else {
            $create_disks->{"${bus}${count}"} = {
                volid => "$storeid:$path",
                defined($disk_capacity) ? (size => $disk_capacity) : (),
            };
        }

        $boot_order .= ';' if length($boot_order);
        $boot_order .= $bus . $count;
    };
    $self->for_each_disk($add_disk);
    if (@nvmes) {
        for my $nvme (@nvmes) {
            my ($slot, $file, $devtype, $kind) = @$nvme;
            $warn->('nvme-unsupported', key => "nvme${slot}", value => "$file");
            $add_disk->('scsi', $slot, $file, $devtype, $kind, 1);
        }
    }

    $scsihw //= $default_scsihw;
    if ($firmware eq 'efi') {
        if (!defined($scsihw) || $scsihw =~ /^lsi/) {
            if ($is_windows) {
                $scsihw = 'pvscsi';
            } else {
                $scsihw = 'virtio-scsi-single';
            }
            # OVMF is built without LSI drivers, scsi hardware was set to $scsihw
            $warn->('ovmf-with-lsi-unsupported', key => 'scsihw', value => "$scsihw");
        }
    }
    $create_args->{scsihw} = $scsihw if defined($scsihw);

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

    my $serid = 0;
    $self->for_each_serial(sub {
        my ($id, $serial) = @_;
        # currently we only support 'socket' type serials anyway
        $warn->('serial-port-socket-only', key => "serial$serid");
        $create_args->{"serial$serid"} = 'socket';
        ++$serid;
    });

    $warn->('guest-is-running') if defined($vminfo) && ($vminfo->{power} // '') ne 'poweredOff';

    return {
        type => 'vm',
        source => 'esxi',
        'create-args' => $create_args,
        disks => $create_disks,
        net => $create_net,
        warnings => $warnings,
    };
}

1;
