package PVE::API2::NVMeOF;

use v5.36;

use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::RPCEnvironment;

use PVE::NVMeOF;
use PVE::Storage;

use base qw(PVE::RESTHandler);

my $read_permissions = {
    check => [
        'or',
        ['perm', '/nodes/{node}', ['Sys.Audit']],
        ['perm', '/storage', ['Datastore.Allocate']],
    ],
};
my $controller_schema = {
    description => 'The fabrics controller name, for example nvme3.',
    type => 'string',
    pattern => 'nvme[0-9]+',
};

__PACKAGE__->register_method({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    permissions => { user => 'all' },
    description => 'Node NVMe over Fabrics index.',
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'array',
        items => { type => 'object', properties => { name => { type => 'string' } } },
        links => [{ rel => 'child', href => '{name}' }],
    },
    code => sub { return [{ name => 'status' }, { name => 'controller' }] },
});

__PACKAGE__->register_method({
    name => 'status',
    path => 'status',
    method => 'GET',
    protected => 1,
    proxyto => 'node',
    permissions => $read_permissions,
    description => 'Report the NVMe host identity and nvme-cli availability.',
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'object',
        additionalProperties => 0,
        properties => {
            supported => { type => 'boolean', description => 'Whether nvme-cli is installed.' },
            'host-nqn' => {
                type => 'string',
                optional => 1,
                description => 'The host NQN for target access control.',
            },
            'host-id' =>
                { type => 'string', optional => 1, description => 'The persistent host UUID.' },
            error => {
                type => 'string',
                optional => 1,
                description => 'Why the host identity is unavailable.',
            },
        },
    },
    code => sub {
        my $res = { supported => PVE::NVMeOF::is_supported() };
        my $host = eval { PVE::NVMeOF::host_identity() };
        if (my $error = $@) {
            chomp $error;
            $res->{error} = $error;
        } else {
            $res->{'host-nqn'} = $host->{nqn};
            $res->{'host-id'} = $host->{id};
        }
        return $res;
    },
});

__PACKAGE__->register_method({
    name => 'controllers',
    path => 'controller',
    method => 'GET',
    protected => 1,
    proxyto => 'node',
    permissions => $read_permissions,
    description => 'List connected NVMe over Fabrics controllers and their states.',
    parameters =>
        { additionalProperties => 0, properties => { node => get_standard_option('pve-node') } },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            additionalProperties => 0,
            properties => {
                controller => $controller_schema,
                subsysnqn => { type => 'string', description => 'The subsystem NQN.' },
                transport => { type => 'string', description => 'The controller transport.' },
                portal => {
                    type => 'string',
                    description => 'The controller address and port, if applicable.',
                },
                state => {
                    type => 'string',
                    description => 'The controller state, such as live or connecting.',
                },
                namespaces => {
                    type => 'integer',
                    description => 'The number of namespaces of the subsystem, counted by the'
                        . ' devices that native NVMe multipath creates for them.',
                },
                'used-by-storage' => {
                    type => 'string',
                    optional => 1,
                    description => 'An NVMe over Fabrics storage using the subsystem.',
                },
            },
        },
    },
    code => sub {
        my $cfg = PVE::Storage::config();
        my %owners;
        for my $sid (sort keys $cfg->{ids}->%*) {
            my $scfg = $cfg->{ids}->{$sid};
            $owners{ $scfg->{subsysnqn} } //= $sid if $scfg->{type} eq 'nvmeof';
        }
        my $controllers = PVE::NVMeOF::controllers();
        my $counts = PVE::NVMeOF::namespace_counts();
        my @rows;
        for my $name (sort keys %$controllers) {
            my $ctrl = $controllers->{$name};
            my $nqn = $ctrl->{subsystem};
            push @rows,
                {
                    controller => $name,
                    subsysnqn => $nqn,
                    transport => $ctrl->{transport},
                    portal => PVE::NVMeOF::format_path($ctrl),
                    state => $ctrl->{state},
                    namespaces => $counts->{$nqn} // 0,
                    defined($owners{$nqn}) ? ('used-by-storage' => $owners{$nqn}) : (),
                };
        }
        return \@rows;
    },
});

__PACKAGE__->register_method({
    name => 'disconnect',
    path => 'controller/{controller}',
    method => 'DELETE',
    protected => 1,
    proxyto => 'node',
    permissions => { check => ['perm', '/', ['Sys.Modify']] },
    description => 'Disconnect one fabrics controller. Refuse when a namespace in use has no'
        . ' other usable path. A storage still listing the portal reconnects it on activation.',
    parameters => {
        additionalProperties => 0,
        properties => {
            node => get_standard_option('pve-node'),
            controller => $controller_schema,
            subsysnqn => {
                type => 'string',
                maxLength => 223,
                optional => 1,
                description => 'Refuse unless the controller still has this subsystem NQN.',
            },
            portal => {
                type => 'string',
                maxLength => 128,
                optional => 1,
                description => 'Refuse unless the controller still has this portal.',
            },
        },
    },
    returns => { type => 'string', description => 'The task identifier.' },
    code => sub($param) {
        my $rpcenv = PVE::RPCEnvironment::get();
        return $rpcenv->fork_worker(
            'nvmedisconnect',
            $param->{controller},
            $rpcenv->get_user(),
            sub {
                PVE::NVMeOF::disconnect_guarded(
                    $param->{controller},
                    { subsysnqn => $param->{subsysnqn}, portal => $param->{portal} },
                );
            },
        );
    },
});

1;
