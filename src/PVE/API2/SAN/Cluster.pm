package PVE::API2::SAN::Cluster;

use v5.36;

use PVE::RESTHandler;

use PVE::Multipath::Status;

use base qw(PVE::RESTHandler);

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    description => "Directory index.",
    permissions => { user => 'all' },
    parameters => {
        additionalProperties => 0,
        properties => {},
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
        return [{ name => 'status' }];
    },
});

my $state_schema = {
    type => 'string',
    enum => ['optimal', 'degraded', 'failed', 'missing', 'unknown'],
};
my $lun_node_schema = {
    type => 'object',
    additionalProperties => 0,
    properties => {
        state => {
            $state_schema->%*,
            description => "The health of the map or NVMe namespace on the node from its paths,"
                . " 'missing' if the node should have it but does not list it, or 'unknown' if the"
                . " node has no current report that covers it.",
        },
        name => {
            type => 'string',
            optional => 1,
            description => 'The name of the map on the node, if it differs from the WWID.',
        },
        size => {
            type => 'integer',
            minimum => 0,
            optional => 1,
            description => 'The size of the map or namespace in bytes.',
        },
        'paths-active' => {
            type => 'integer',
            minimum => 0,
            optional => 1,
            description => 'The number of active paths of the map whose checker state is ready,'
                . ' ghost, or not reported. For an NVMe namespace, the number of its paths over a'
                . ' live controller whose ANA state is optimized, non-optimized, or not reported.',
        },
        'paths-total' => {
            type => 'integer',
            minimum => 0,
            optional => 1,
            description => 'The number of paths of the map or namespace.',
        },
        'paths-expected' => {
            type => 'integer',
            minimum => 0,
            optional => 1,
            description => 'The number of paths the LUN should have on the node: the highest of'
                . ' the count of its multipath entry and, for each managed iSCSI storage of the'
                . ' LUN, the number of its portals. For an NVMe namespace, the number of portals'
                . ' of its NVMe over Fabrics storage.',
        },
    },
};
my $node_schema = {
    type => 'object',
    additionalProperties => 0,
    properties => {
        state => {
            type => 'string',
            enum => ['ok', 'unavailable', 'no-report'],
            description => "'ok' if the node has a current report, 'unavailable' if its report"
                . " has an 'error', or 'no-report' if the node is offline or its last report is"
                . " missing or older than three minutes.",
        },
        time => {
            type => 'integer',
            optional => 1,
            description => 'The time of the last report of the node (seconds since the UNIX'
                . ' epoch).',
        },
        'other-maps' => {
            type => 'integer',
            minimum => 1,
            optional => 1,
            description => 'The number of multipath maps that the node leaves out of its LUN'
                . ' list. It lists the maps that Proxmox VE manages and the ones it knows to be the'
                . ' base volume of a storage or to hold its volume group.',
        },
        error => {
            type => 'string',
            optional => 1,
            description => 'Why the node cannot report its multipath maps, for example because'
                . ' multipathd is not running.',
        },
        'apply-error' => {
            type => 'string',
            optional => 1,
            description => 'The error of the last attempt of the node to apply the cluster-wide'
                . ' multipath configuration, for example a WWID that multipath refuses or a'
                . ' failed reload of multipathd.',
        },
        'iscsi-initiator-name' => {
            type => 'string',
            optional => 1,
            description => 'The iSCSI initiator name of the node.',
        },
        'nvme-host-nqn' => {
            type => 'string',
            optional => 1,
            description => 'The NVMe host NQN of the node.',
        },
    },
};

__PACKAGE__->register_method({
    name => 'status',
    path => 'status',
    method => 'GET',
    description => "Report the SAN status of the nodes, with their initiator names, and the"
        . " multipath health of each LUN on each node, with as summary the worst known state"
        . " among the nodes that should see or report the LUN. Nodes whose state is unknown are"
        . " not counted.",
    # Storage administrators need path health and initiator names to configure SAN target ACLs.
    permissions => {
        check => [
            'or', ['perm', '/', ['Sys.Audit']], ['perm', '/storage', ['Datastore.Allocate']],
        ],
    },
    parameters => {
        additionalProperties => 0,
        properties => {},
    },
    returns => {
        type => 'object',
        properties => {
            luns => {
                type => 'array',
                description =>
                    "One entry per LUN or NVMe namespace, identified by its WWID, with its"
                    . " path health on each node and the overall cluster state.",
                items => {
                    type => 'object',
                    additionalProperties => 0,
                    properties => {
                        wwid => { type => 'string', description => 'The LUN WWID.' },
                        explicit =>
                            { type => 'boolean', description => 'An explicit entry exists.' },
                        native => {
                            type => 'boolean',
                            optional => 1,
                            description => 'An NVMe namespace, which uses the native'
                                . ' multipath of the kernel.',
                        },
                        name => {
                            type => 'string',
                            optional => 1,
                            description =>
                                'The alias of the explicit entry, or else the name of'
                                . ' the map on a node if it differs from the WWID.',
                        },
                        'used-by-storage' => {
                            type => 'string',
                            optional => 1,
                            description =>
                                'The storage that uses the LUN: as its base volume or through'
                                . ' the volume group on it, or as an iSCSI or NVMe over Fabrics'
                                . ' storage that serves the LUNs of its target or subsystem as'
                                . ' images.',
                        },
                        'multipath-storage' => {
                            type => 'string',
                            optional => 1,
                            description => 'The storage that requests the map of the LUN.',
                        },
                        size => {
                            type => 'integer',
                            minimum => 0,
                            optional => 1,
                            description =>
                                'The size of the LUN in bytes, as a node reports it.',
                        },
                        state => {
                            $state_schema->%*,
                            description => "The worst state of the LUN among its nodes, or"
                                . " 'unknown' if no node's state is known.",
                        },
                        nodes => {
                            type => 'object',
                            description => "Path health keyed by node name.",
                            additionalProperties => $lun_node_schema,
                        },
                    },
                },
            },
            nodes => {
                type => 'object',
                description => "The SAN status of each cluster node, keyed by node name.",
                additionalProperties => $node_schema,
            },
        },
    },
    code => sub {
        return PVE::Multipath::Status::cluster_status();
    },
});

1;
