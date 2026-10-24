package PVE::API2::SAN::Node;

use v5.36;

use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::RPCEnvironment;

use PVE::Storage;
use PVE::Storage::SAN;

use PVE::API2::ISCSI;
use PVE::API2::NVMeOF;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    subclass => "PVE::API2::ISCSI",
    path => 'iscsi',
});

__PACKAGE__->register_method({
    subclass => "PVE::API2::NVMeOF",
    path => 'nvmeof',
});

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => "Directory index.",
    proxyto => 'node',
    permissions => { user => 'all' },
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
        },
    },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            properties => {
                name => { type => 'string' },
            },
        },
        links => [{ rel => 'child', href => "{name}" }],
    },
    code => sub {
        return [map { +{ name => $_ } } qw(iscsi luns nvmeof)];
    },
});

__PACKAGE__->register_method({
    name => 'luns',
    path => 'luns',
    method => 'GET',
    description => "List the SAN LUNs of the node with their paths and cached usage.",
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
                        . " iSCSI storage of the LUN, the number of its portals. For a namespace"
                        . " of an NVMe over Fabrics storage, the number of its portals. Absent if"
                        . " no count is known.",
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
                        . " through the volume group on it, or as an iSCSI or NVMe over Fabrics"
                        . " storage that serves the LUNs of its target or subsystem as images.",
                    type => 'string',
                    optional => 1,
                },
                'used-by-process' => {
                    description => "A process of the node that has the LUN, one of its paths"
                        . " or a partition open for writing, as PID followed by the command name"
                        . " in parentheses. A guest shows up like this if it has the LUN as disk"
                        . " by its device path or from a third-party storage plugin.",
                    type => 'string',
                    optional => 1,
                },
                'used-by-guest' => {
                    description => "The ID of the virtual machine that this process belongs"
                        . " to. Only returned with the VM.Audit privilege on it.",
                    type => 'integer',
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
        my $rpcenv = PVE::RPCEnvironment::get();
        my $authuser = $rpcenv->get_user();

        my $rows = PVE::Storage::SAN::scan_luns(PVE::Storage::config());
        # without VM.Audit on the guest, its process still tells that the LUN is in use
        for my $row (@$rows) {
            my $vmid = $row->{'used-by-guest'} // next;
            delete $row->{'used-by-guest'}
                if !$rpcenv->check($authuser, "/vms/$vmid", ['VM.Audit'], 1);
        }
        return $rows;
    },
});

1;
