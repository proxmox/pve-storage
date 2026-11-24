package PVE::API2::Disks;

use strict;
use warnings;

use File::Basename;
use HTTP::Status qw(:constants);

use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::SafeSyslog;
use PVE::Storage::LVMPlugin;
use PVE::Storage::SAN;
use PVE::Tools qw(run_command);

use PVE::API2::Disks::Directory;
use PVE::API2::Disks::LVM;
use PVE::API2::Disks::LVMThin;
use PVE::API2::Disks::ZFS;

use PVE::RESTHandler;
use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    subclass => "PVE::API2::Disks::LVM",
    path => 'lvm',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::Disks::LVMThin",
    path => 'lvmthin',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::Disks::Directory",
    path => 'directory',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::Disks::ZFS",
    path => 'zfs',
});

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    permissions => { user => 'all' },
    description => "Node index.",
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {},
        },
        links => [{ rel => 'child', href => "{name}" }],
    },
    code => sub {
        my ($param) = @_;

        my $result = [
            { name => 'list' },
            { name => 'initgpt' },
            { name => 'smart' },
            { name => 'lvm' },
            { name => 'lvmthin' },
            { name => 'directory' },
            { name => 'wipedisk' },
            { name => 'zfs' },
        ];

        return $result;
    },
});

__PACKAGE__->register_method({
    name => 'list',
    path => 'list',
    method => 'GET',
    description => "List local disks.",
    protected => 1,
    proxyto => 'node',
    permissions => {
        check => ['or', ['perm', '/', ['Sys.Audit']], ['perm', '/nodes/{node}', ['Sys.Audit']]],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            'include-partitions' => {
                description => "Also include partitions.",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            skipsmart => {
                description => "Skip smart checks.",
                type => 'boolean',
                optional => 1,
                default => 0,
            },
            type => {
                description => "Only list specific types of disks.",
                type => 'string',
                enum => ['unused', 'journal_disks'],
                optional => 1,
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            properties => {
                devpath => {
                    type => 'string',
                    description => 'The device path',
                },
                used => { type => 'string', optional => 1 },
                gpt => { type => 'boolean' },
                mounted => { type => 'boolean' },
                size => { type => 'integer' },
                osdid => { type => 'integer' }, # TODO: deprecate / remove in PVE 9?
                'osdid-list' => {
                    type => 'array',
                    items => { type => 'integer' },
                },
                vendor => { type => 'string', optional => 1 },
                model => { type => 'string', optional => 1 },
                serial => { type => 'string', optional => 1 },
                wwn => { type => 'string', optional => 1 },
                health => { type => 'string', optional => 1 },
                transport => {
                    type => 'string',
                    description =>
                        "The device transport, for example 'sata', 'sas', 'usb', 'fc',"
                        . " 'nvme', 'nvme-fc', or 'nvme-tcp'.",
                    optional => 1,
                },
                parent => {
                    type => 'string',
                    description => 'For partitions only. The device path of '
                        . 'the disk the partition resides on.',
                    optional => 1,
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $skipsmart = $param->{skipsmart} // 0;
        my $include_partitions = $param->{'include-partitions'} // 0;

        my $disks = PVE::Diskmanage::get_disks(
            undef, $skipsmart, $include_partitions,
        );

        my $type = $param->{type} // '';
        my $result = [];

        foreach my $disk (sort keys %$disks) {
            my $entry = $disks->{$disk};
            if ($type eq 'journal_disks') {
                next if $entry->{osdid} >= 0;
                if (my $usage = $entry->{used}) {
                    next
                        if !($usage eq 'partitions' && $entry->{gpt} || $usage eq 'LVM');
                }
            } elsif ($type eq 'unused') {
                next if $entry->{used};
            } elsif ($type ne '') {
                die "internal error"; # should not happen
            }
            push @$result, $entry;
        }
        return $result;
    },
});

__PACKAGE__->register_method({
    name => 'smart',
    path => 'smart',
    method => 'GET',
    description => "Get SMART Health of a disk.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            disk => {
                type => 'string',
                pattern => '^/dev/[a-zA-Z0-9\/]+$',
                description => "Block device name",
            },
            healthonly => {
                type => 'boolean',
                description => "If true returns only the health status",
                optional => 1,
            },
        },
    },
    returns => {
        type => 'object',
        properties => {
            health => { type => 'string' },
            type => { type => 'string', optional => 1 },
            attributes => { type => 'array', optional => 1 },
            text => { type => 'string', optional => 1 },
        },
    },
    code => sub {
        my ($param) = @_;

        my $disk = PVE::Diskmanage::verify_blockdev_path($param->{disk});

        my $result = eval { PVE::Diskmanage::get_smart_data($disk, $param->{healthonly}) };

        $result->{health} = 'UNKNOWN' if !defined $result->{health};
        $result = { health => $result->{health} } if $param->{healthonly};

        return $result;
    },
});

__PACKAGE__->register_method({
    name => 'initgpt',
    path => 'initgpt',
    method => 'POST',
    description => "Initialize Disk with GPT",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            disk => {
                type => 'string',
                description => "Block device name",
                pattern => '^/dev/[a-zA-Z0-9\/]+$',
            },
            uuid => {
                type => 'string',
                description => 'UUID for the GPT table',
                pattern => '[a-fA-F0-9\-]+',
                maxLength => 36,
                optional => 1,
            },
        },
    },
    returns => { type => 'string' },
    code => sub {
        my ($param) = @_;

        my $disk = PVE::Diskmanage::verify_blockdev_path($param->{disk});

        my $rpcenv = PVE::RPCEnvironment::get();

        my $authuser = $rpcenv->get_user();

        die "$disk is a partition\n" if PVE::Diskmanage::is_partition($disk);
        die "disk $disk already in use\n" if PVE::Diskmanage::disk_is_used($disk);
        my $worker = sub {
            PVE::Diskmanage::init_disk($disk, $param->{uuid});
        };

        my $diskid = $disk;
        $diskid =~ s|^.*/||; # remove all up to the last slash
        return $rpcenv->fork_worker('diskinit', $diskid, $authuser, $worker);
    },
});

__PACKAGE__->register_method({
    name => 'wipe_disk',
    path => 'wipedisk',
    method => 'PUT',
    description => "Wipe a disk or partition.",
    proxyto => 'node',
    protected => 1,
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            disk => {
                type => 'string',
                description =>
                    "Block device name. A SAN LUN can also be given by a by-id or mapper name.",
                pattern => '^/dev/[a-zA-Z0-9._:+/-]+$',
            },
        },
    },
    returns => { type => 'string' },
    code => sub {
        my ($param) = @_;

        my $disk = PVE::Diskmanage::verify_blockdev_path($param->{disk});

        my $lun = PVE::Storage::SAN::lun_for_device($param->{disk});
        if (!$lun) {
            # A by-id, mapper or dm-N name could also select a logical volume or a partition of a
            # multipath map, which the checks for mounts and holders do not protect.
            if ($param->{disk} !~ m{^/dev/[a-zA-Z0-9/]+$}) {
                die "cannot wipe '$param->{disk}' - a name of this form is only accepted for a"
                    . " SAN LUN\n";
            }

            # Refuse local wipes on network/pathless NVMe namespaces (including partitions),
            # since local mount/holder checks miss other nodes; wipe whole namespaces via SAN.
            if (basename($disk) =~ m/^(nvme[0-9]+n[0-9]+)(p[0-9]+)?$/) {
                my ($namespace, $partition) = ($1, $2);
                my $transport = PVE::Diskmanage::get_nvme_transport("/sys/block/$namespace");
                die
                    "cannot wipe '$disk' - the transport of namespace '/dev/$namespace' cannot be"
                    . " read, as when it has no path\n"
                    if !defined($transport);
                if (PVE::Diskmanage::is_network_nvme_transport($transport)) {
                    die
                        "cannot wipe '$disk' - a partition of namespace '/dev/$namespace', which"
                        . " is attached over a network, cannot be wiped on its own; Node > SAN"
                        . " wipes the whole namespace\n"
                        if defined($partition);
                    # it got a path since the SAN check above, which found none
                    die "cannot wipe '$disk' - its transport changed, try again\n";
                }
            }

            my $mounted = PVE::Diskmanage::is_mounted($disk);
            die "disk/partition '${mounted}' is mounted\n" if $mounted;

            my $held = PVE::Diskmanage::has_holder($disk);
            die "disk/partition '${held}' has a holder\n" if $held;
        }

        my $rpcenv = PVE::RPCEnvironment::get();
        my $authuser = $rpcenv->get_user();

        my $worker = sub {
            if ($lun) {
                PVE::Storage::SAN::with_lun_lock(
                    $lun,
                    sub { PVE::Storage::SAN::wipe_lun($lun) },
                );
                PVE::Diskmanage::udevadm_trigger($disk, PVE::Storage::SAN::lun_paths($lun)->@*);
                eval { run_command(['udevadm', 'settle', '--timeout=30'], timeout => 35) };
                warn $@ if $@;
                PVE::Storage::LVMPlugin::refresh_cached_pv_volume_groups();
            } else {
                PVE::Diskmanage::wipe_blockdev($disk);
                PVE::Diskmanage::udevadm_trigger($disk);
            }
        };

        my $basename = basename($disk); # avoid '/' in the ID

        return $rpcenv->fork_worker('wipedisk', $basename, $authuser, $worker);
    },
});

1;
