package PVE::API2::Storage::Scan;

use strict;
use warnings;

# NOTE: This API endpoints are mounted by pve-manager's API2::Node module and pvesm CLI

use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::SafeSyslog;

use PVE::Storage::LVMPlugin;
use PVE::Storage::LvmThinPlugin;
use PVE::Storage::PBSPlugin;
use PVE::Storage::SAN;
use PVE::Storage;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => "Index of available scan methods",
    permissions => {
        user => 'all',
    },
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
            properties => {
                method => { type => 'string' },
            },
        },
        links => [{ rel => 'child', href => "{method}" }],
    },
    code => sub {
        my ($param) = @_;

        my $res = [
            { method => 'cifs' },
            { method => 'iscsi' },
            { method => 'lvm' },
            { method => 'nfs' },
            { method => 'pbs' },
            { method => 'san-luns' },
            { method => 'zfs' },
        ];

        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'nfsscan',
    path => 'nfs',
    method => 'GET',
    description => "Scan remote NFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            server => {
                description => "The server address (name or IP).",
                type => 'string',
                format => 'pve-storage-server',
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                path => {
                    description => "The exported path.",
                    type => 'string',
                },
                options => {
                    description => "NFS export options.",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $server = $param->{server};
        my $res = PVE::Storage::scan_nfs($server);

        my $data = [];
        foreach my $k (sort keys %$res) {
            push @$data, { path => $k, options => $res->{$k} };
        }
        return $data;
    },
});

__PACKAGE__->register_method({
    name => 'cifsscan',
    path => 'cifs',
    method => 'GET',
    description => "Scan remote CIFS server.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            server => {
                description => "The server address (name or IP).",
                type => 'string',
                format => 'pve-storage-server',
            },
            username => {
                description => "User name.",
                type => 'string',
                optional => 1,
            },
            password => {
                description => "User password.",
                type => 'string',
                sensitive => 1,
                optional => 1,
            },
            domain => {
                description => "SMB domain (Workgroup).",
                type => 'string',
                optional => 1,
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                share => {
                    description => "The cifs share name.",
                    type => 'string',
                },
                description => {
                    description => "Descriptive text from server.",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $server = $param->{server};

        my $username = $param->{username};
        my $password = $param->{password};
        my $domain = $param->{domain};

        my $res = PVE::Storage::scan_cifs($server, $username, $password, $domain);

        my $data = [];
        foreach my $k (sort keys %$res) {
            next if $k =~ m/NT_STATUS_/;
            push @$data, { share => $k, description => $res->{$k} };
        }

        return $data;
    },
});

__PACKAGE__->register_method({
    name => 'pbsscan',
    path => 'pbs',
    method => 'GET',
    description => "Scan remote Proxmox Backup Server.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            server => {
                description => "The server address (name or IP).",
                type => 'string',
                format => 'pve-storage-server',
            },
            username => {
                description => "User-name or API token-ID.",
                type => 'string',
            },
            password => {
                description => "User password or API token secret.",
                type => 'string',
                sensitive => 1,
            },
            fingerprint => get_standard_option('fingerprint-sha256', {
                    optional => 1,
            }),
            port => {
                description => "Optional port.",
                type => 'integer',
                minimum => 1,
                maximum => 65535,
                default => 8007,
                optional => 1,
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                store => {
                    description => "The datastore name.",
                    type => 'string',
                },
                comment => {
                    description => "Comment from server.",
                    type => 'string',
                    optional => 1,
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $password = delete $param->{password};

        return PVE::Storage::PBSPlugin::scan_datastores($param, $password);
    },
});

__PACKAGE__->register_method({
    name => 'iscsiscan',
    path => 'iscsi',
    method => 'GET',
    description => "Scan remote iSCSI server.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            portal => {
                description => "The iSCSI portal (IP or DNS name with optional port).",
                type => 'string',
                format => 'pve-storage-portal-dns',
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                target => {
                    description => "The iSCSI target name.",
                    type => 'string',
                },
                portal => {
                    description => "The iSCSI portal name.",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $res = PVE::Storage::scan_iscsi($param->{portal});

        my $data = [];
        foreach my $k (sort keys %$res) {
            push @$data, { target => $k, portal => join(',', @{ $res->{$k} }) };
        }

        return $data;
    },
});

__PACKAGE__->register_method({
    name => 'lvmscan',
    path => 'lvm',
    method => 'GET',
    description => "List local LVM volume groups.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
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
            properties => {
                vg => {
                    description => "The LVM logical volume group name.",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        my $res = PVE::Storage::LVMPlugin::lvm_vgs();
        return PVE::RESTHandler::hash_to_array($res, 'vg');
    },
});

__PACKAGE__->register_method({
    name => 'lvmthinscan',
    path => 'lvmthin',
    method => 'GET',
    description => "List local LVM Thin Pools.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            vg => {
                type => 'string',
                pattern => '[a-zA-Z0-9\.\+\_][a-zA-Z0-9\.\+\_\-]+', # see lvm(8) manpage
                maxLength => 100,
            },
        },
    },
    returns => {
        type => 'array',
        items => {
            type => "object",
            properties => {
                lv => {
                    description => "The LVM Thin Pool name (LVM logical volume).",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        return PVE::Storage::LvmThinPlugin::list_thinpools($param->{vg});
    },
});

__PACKAGE__->register_method({
    name => 'sanlunscan',
    path => 'san-luns',
    method => 'GET',
    description => "List SAN LUNs with their paths and cached usage.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => [
            'or',
            ['perm', '/nodes/{node}', ['Sys.Audit']],
            ['perm', '/storage', ['Datastore.Allocate']],
        ],
    },
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
            properties => {
                devpath => {
                    description => "The device path.",
                    type => 'string',
                },
                size => {
                    description => "The device size in bytes.",
                    type => 'integer',
                    renderer => 'bytes',
                    optional => 1,
                },
                transport => {
                    description =>
                        "The device transport, for example 'sata', 'sas', 'usb', 'fc',"
                        . " 'iscsi', 'nvme', 'nvme-fc', or 'nvme-tcp'.",
                    type => 'string',
                    optional => 1,
                },
                vendor => { type => 'string', optional => 1 },
                model => { type => 'string', optional => 1 },
                serial => { type => 'string', optional => 1 },
                state => {
                    description => 'The health of the LUN from the state of its paths, when'
                        . ' that is known. Absent while a requested multipath map is missing.',
                    type => 'string',
                    enum => ['optimal', 'degraded', 'failed'],
                    optional => 1,
                },
                'paths-total' => {
                    description => "The number of paths of a multipath mapped device or of"
                        . " an NVMe namespace attached through multiple controllers, or the"
                        . " number of SCSI devices of a LUN without a map.",
                    type => 'integer',
                    optional => 1,
                },
                'paths-active' => {
                    description =>
                        "The number of active paths whose checker state is ready, ghost,"
                        . " or not reported. Native NVMe paths count when their controller is live"
                        . " and their ANA state is optimized, non-optimized, or not reported. A"
                        . " device of a LUN without a map counts while the kernel reports it as"
                        . " running.",
                    type => 'integer',
                    optional => 1,
                },
                'paths-expected' => {
                    description => "The number of paths the LUN should have on this node: the"
                        . " highest of the count of its multipath entry and, for each managed"
                        . " iSCSI storage of the LUN, the number of its portals. Absent if no"
                        . " count is known.",
                    type => 'integer',
                    optional => 1,
                },
                'path-list' => {
                    description => "The member paths of the device with their state.",
                    type => 'array',
                    optional => 1,
                    items => {
                        type => 'object',
                        properties => {
                            device => {
                                description => "The path block device, or the NVMe"
                                    . " controller device the path uses.",
                                type => 'string',
                            },
                            state => {
                                description => "The device mapper state of the path (for"
                                    . " example active or failed), the state of the NVMe"
                                    . " controller (for example live or connecting), or for a"
                                    . " LUN without a map the kernel state of the device (for"
                                    . " example running or transport-offline).",
                                type => 'string',
                            },
                            'checker-state' => {
                                description => "The state reported by the multipathd path"
                                    . " checker, for example ready, ghost or faulty.",
                                type => 'string',
                                optional => 1,
                            },
                            'ana-state' => {
                                description => "The Asymmetric Namespace Access state of"
                                    . " the namespace on this path, for example optimized"
                                    . " or inaccessible.",
                                type => 'string',
                                optional => 1,
                            },
                            'host-wwpn' => {
                                description => "The WWPN of the host (initiator) port of"
                                    . " Fibre Channel attached paths.",
                                type => 'string',
                                optional => 1,
                            },
                            'target-wwpn' => {
                                description => "The WWPN of the target port of Fibre"
                                    . " Channel attached paths.",
                                type => 'string',
                                optional => 1,
                            },
                            address => {
                                description => "The address of the NVMe controller the"
                                    . " path uses, or the portal of the iSCSI session of the"
                                    . " path.",
                                type => 'string',
                                optional => 1,
                            },
                        },
                    },
                },
                usage => {
                    description => "The usage indicated by cached device metadata.",
                    type => 'string',
                    enum => ['unused', 'lvm', 'other'],
                },
                vgname => {
                    description => "The name of the LVM volume group the device belongs to.",
                    type => 'string',
                    optional => 1,
                },
                'by-id' => {
                    description =>
                        "A path under /dev/disk/by-id that encodes the device's identity"
                        . " and has been verified to resolve to this device. Absent when no such"
                        . " link could be verified.",
                    type => 'string',
                    optional => 1,
                },
                'used-by-storage' => {
                    description => "The storage that uses the LUN: as its base volume or"
                        . " through the volume group on it, or as an iSCSI storage that serves the"
                        . " LUNs of its target as images.",
                    type => 'string',
                    optional => 1,
                },
                wwid => {
                    description => "The LUN WWID.",
                    type => 'string',
                },
                name => {
                    description => 'The map alias, when different from its WWID.',
                    type => 'string',
                    optional => 1,
                },
                multipath => {
                    description => "How the node manages multipath for the LUN: 'manual' for a"
                        . " map with an explicit entry, 'storage' for the map of an iSCSI storage"
                        . " with the 'managed-multipath' option, 'unmanaged' for any other map,"
                        . " 'pending' while a map that an entry, a storage or the allow-list of"
                        . " the node requests does not exist yet, 'none' without a map or"
                        . " request, 'native' for the kernel-native multipath of an NVMe"
                        . " namespace.",
                    type => 'string',
                    enum => ['none', 'pending', 'storage', 'manual', 'unmanaged', 'native'],
                },
                'multipath-storage' => {
                    description =>
                        "The multipath-enabled storage the LUN belongs to, whether or"
                        . " not it also has an explicit multipath entry.",
                    type => 'string',
                    optional => 1,
                },
                'nvme-subsystem' => {
                    description => 'The NVMe subsystem NQN.',
                    type => 'string',
                    optional => 1,
                },
                'nvme-nsid' => {
                    description => 'The namespace ID within the NVMe subsystem.',
                    type => 'integer',
                    optional => 1,
                },
                'iscsi-target' => {
                    description => "An iSCSI target through which this node reaches the LUN;"
                        . " one of them if there are several.",
                    type => 'string',
                    optional => 1,
                },
                'iscsi-lun' => {
                    description => "The number of the LUN on that target.",
                    type => 'integer',
                    optional => 1,
                },
                detail => {
                    description => "The cached signature or usage when usage is 'other'.",
                    type => 'string',
                    optional => 1,
                },
            },
        },
    },
    code => sub {
        return PVE::Storage::SAN::scan_luns(PVE::Storage::config());
    },
});

__PACKAGE__->register_method({
    name => 'zfsscan',
    path => 'zfs',
    method => 'GET',
    description => "Scan zfs pool list on local node.",
    protected => 1,
    proxyto => "node",
    permissions => {
        check => ['perm', '/storage', ['Datastore.Allocate']],
    },
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
            properties => {
                pool => {
                    description => "ZFS pool name.",
                    type => 'string',
                },
            },
        },
    },
    code => sub {
        my ($param) = @_;

        return PVE::Storage::scan_zfs();
    },
});

1;
