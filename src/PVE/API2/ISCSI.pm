package PVE::API2::ISCSI;

use v5.36;

use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;

use PVE::ISCSI::Topology;
use PVE::Storage;
use PVE::Storage::ISCSIPlugin;

use base qw(PVE::RESTHandler);

my $read_permissions = {
    check => [
        'or',
        ['perm', '/nodes/{node}', ['Sys.Audit']],
        ['perm', '/storage', ['Datastore.Allocate']],
    ],
};

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    permissions => { user => 'all' },
    description => 'Node iSCSI index.',
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'array',
        items => { type => 'object', properties => { name => { type => 'string' } } },
        links => [{ rel => 'child', href => '{name}' }],
    },
    code => sub { return [{ name => 'status' }, { name => 'target' }] },
});

__PACKAGE__->register_method({
    name => 'status',
    path => 'status',
    method => 'GET',
    protected => 1,
    proxyto => 'node',
    description => 'Report the iSCSI initiator identity and availability.',
    permissions => $read_permissions,
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'object',
        properties => {
            supported =>
                { type => 'boolean', description => 'Whether open-iscsi is installed.' },
            'initiator-name' =>
                { type => 'string', optional => 1, description => 'The initiator IQN.' },
            'initiator-alias' =>
                { type => 'string', optional => 1, description => 'The initiator alias.' },
        },
    },
    code => sub {
        my $res = { supported => PVE::Storage::ISCSIPlugin::iscsi_supported() };
        my $info = PVE::Storage::ISCSIPlugin::read_initiator_info() // {};
        $res->{'initiator-name'} = $info->{name} if defined($info->{name});
        $res->{'initiator-alias'} = $info->{alias} if defined($info->{alias});
        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'targets',
    path => 'target',
    method => 'GET',
    protected => 1,
    proxyto => 'node',
    description => 'List iSCSI targets and their sessions and devices.',
    permissions => $read_permissions,
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            properties => {
                target => { type => 'string', description => 'The target IQN.' },
                portal => { type => 'string', description => 'The portal address and port.' },
                'session-id' =>
                    { type => 'integer', optional => 1, description => 'The kernel session ID.' },
                state => {
                    type => 'string',
                    enum => ['logged-in', 'recovering', 'failed', 'not-connected', 'unknown'],
                    description => 'The connection state.',
                },
                'kernel-state' =>
                    { type => 'string', optional => 1, description => 'The kernel session state.' },
                'used-by-storage' => {
                    type => 'string',
                    optional => 1,
                    description => 'An iSCSI storage using the target.',
                },
                devices => {
                    type => 'array',
                    description => 'The block devices of the session.',
                    items => {
                        type => 'object',
                        properties => {
                            devpath =>
                                { type => 'string', description => 'The block device path.' },
                            lun => { type => 'integer', description => 'The LUN number.' },
                        },
                    },
                },
            },
        },
    },
    code => sub {
        my $sessions = PVE::ISCSI::Topology::sessions();
        my $cfg = PVE::Storage::config();
        my %owners;
        for my $sid (sort keys $cfg->{ids}->%*) {
            my $scfg = $cfg->{ids}->{$sid};
            $owners{ $scfg->{target} } //= $sid if $scfg->{type} eq 'iscsi' && $scfg->{target};
        }
        my (@rows, %seen);
        for my $sid (sort { $a <=> $b } keys %$sessions) {
            my $session = $sessions->{$sid};
            my $state = $session->{state};
            my $row = {
                target => $session->{target},
                portal => $session->{portal},
                'session-id' => int($sid),
                state => $state eq 'LOGGED_IN' ? 'logged-in'
                : $state eq 'FAILED' ? 'recovering'
                : $state eq 'FREE' ? 'failed'
                : 'unknown',
                'kernel-state' => $state,
                devices => [
                    sort { $a->{lun} <=> $b->{lun} || $a->{devpath} cmp $b->{devpath} }
                    map { { devpath => "/dev/$_", lun => $session->{devices}->{$_}->{lun} } }
                        keys $session->{devices}->%*
                ],
            };
            push @rows, $row;
            $seen{"$row->{target}\0$row->{portal}"} = 1;
        }
        if (PVE::Storage::ISCSIPlugin::iscsi_supported()) {
            for my $record (PVE::Storage::ISCSIPlugin::iscsi_node_list()->@*) {
                my $portal = PVE::ISCSI::Topology::normalize_portal($record->{portal});
                next if $seen{"$record->{target}\0$portal"}++;
                push @rows,
                    {
                        target => $record->{target},
                        portal => $portal,
                        state => 'not-connected',
                        devices => [],
                    };
            }
        }
        for my $row (@rows) {
            $row->{'used-by-storage'} = $owners{ $row->{target} }
                if defined($owners{ $row->{target} });
        }
        return [sort { $a->{target} cmp $b->{target} || $a->{portal} cmp $b->{portal} } @rows];
    },
});

1;
