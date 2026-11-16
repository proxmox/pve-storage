package PVE::Storage::TestPluginProperties;

use v5.36;

use lib qw(..);

use Test::More;

use PVE::Storage::Plugin;

# What a plugin that does not isolate its properties can use. Third-party plugins rely on these, so
# none can be removed, renamed, narrowed or left unexposed by an in-tree plugin that isolates.
my $available = {
    authsupported => 'string',
    base => 'string:pve-volume-id',
    blocksize => 'string:pve-storage-zfs-blocksize',
    bwlimit => 'string:{clone,default,migration,move,restore}',
    comstar_hg => 'string',
    comstar_tg => 'string',
    content => 'string:pve-storage-content-list',
    'content-dirs' => 'string:pve-dir-override-list',
    'create-base-path' => 'boolean',
    'create-subdirs' => 'boolean',
    'data-pool' => 'string',
    datastore => 'string',
    disable => 'boolean',
    domain => 'string maxLength=256',
    'encryption-key' => 'string',
    export => 'string:pve-storage-path',
    fingerprint => 'string pattern=([A-Fa-f0-9]{2}:){31}[A-Fa-f0-9]{2}',
    format => 'string enum=raw|qcow2|subvol|vmdk',
    'fs-name' => 'string:pve-configid',
    fuse => 'boolean',
    is_mountpoint => 'string',
    iscsiprovider => 'string',
    keyring => 'string',
    krbd => 'boolean',
    lio_tpg => 'string',
    'master-pubkey' => 'string',
    'max-protected-backups' => 'integer minimum=-1',
    mkdir => 'boolean',
    monhost => 'string:pve-storage-portal-dns-list',
    mountpoint => 'string:pve-storage-path',
    namespace => 'string',
    nocow => 'boolean',
    nodes => 'string:pve-node-list',
    nowritecache => 'boolean',
    'on-volume-remove' => 'string:on-volume-remove',
    options => 'string:pve-storage-options',
    password => 'string maxLength=256',
    path => 'string:pve-storage-path',
    pool => 'string',
    port => 'integer minimum=1 maximum=65535',
    portal => 'string:pve-storage-portal-dns',
    preallocation => 'string enum=off|metadata|falloc|full',
    'prune-backups' => 'string:prune-backups',
    saferemove => 'boolean',
    'saferemove-stepsize' => 'integer enum=1|2|4|8|16|32',
    saferemove_throughput => 'string pattern=-?\d+[kmgKMG]?',
    server => 'string:pve-storage-server',
    share => 'string',
    shared => 'boolean',
    'skip-cert-verification' => 'boolean',
    smbversion => 'string enum=default|2.0|2.1|3|3.0|3.11',
    'snapshot-as-volume-chain' => 'boolean',
    sparse => 'boolean',
    storage => 'string:pve-storage-id',
    subdir => 'string:pve-storage-path',
    tagged_only => 'boolean',
    target => 'string',
    thinpool => 'string:pve-storage-vgname',
    type => 'string',
    username => 'string',
    vgname => 'string:pve-storage-vgname',
    'zfs-base-path' => 'string:pve-storage-path',
};

# uses every property, so one that is no longer exposed fails the initialization
package PVE::Storage::Custom::LegacyFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'legacyfixture';
    }

    sub plugindata {
        return { content => [{ none => 1 }, { none => 1 }] };
    }

    sub options {
        return {
            map { $_ => { optional => 1 } }
            grep { !m/^(?:type|storage)$/ } keys $available->%*
        };
    }
}

package PVE::Storage::Custom::IsolatedFixturePlugin {
    use base qw(PVE::Storage::Plugin);

    sub type {
        return 'isolatedfixture';
    }

    sub plugindata {
        return { content => [{ none => 1 }, { none => 1 }], 'isolate-properties' => 1 };
    }
}

# has to happen before loading PVE::Storage, which initializes all registered plugins
PVE::Storage::Custom::LegacyFixturePlugin->register();
PVE::Storage::Custom::IsolatedFixturePlugin->register();

require PVE::Storage;

my $kind = sub($name, $schema) {
    my $format = $schema->{format};
    $format = '{' . join(',', sort keys $format->%*) . '}' if ref($format);

    my $kind = join(':', grep { defined } $schema->{type}, $format);
    # the type property lists the registered plugins
    $kind .= ' enum=' . join('|', $schema->{enum}->@*) if $schema->{enum} && $name ne 'type';
    for my $limit (qw(pattern minimum maximum minLength maxLength)) {
        $kind .= " $limit=$schema->{$limit}" if defined($schema->{$limit});
    }
    return $kind;
};

my $shared = PVE::Storage::Plugin->private()->{propertyList};

for my $name (sort keys $available->%*) {
    my $schema = $shared->{$name};
    is($schema && $kind->($name, $schema), $available->{$name}, "'$name' is available as before");
}

# An isolating plugin can only use these and must not declare one itself, so both removing and
# adding one can break third-party plugins. A property belongs here only if methods of the base
# plugin or code outside of the plugins read it from storages of any type.
my $base = [qw(
    base bwlimit content content-dirs create-base-path create-subdirs disable format
    max-protected-backups mkdir nodes path preallocation prune-backups shared
    snapshot-as-volume-chain storage type
)];
my $declared = PVE::Storage::Plugin->base_properties();
is(join(' ', sort keys $declared->%*), "$base->@*", "the base plugin declares the same properties");

# Third-party plugins that are installed on the host get loaded too and may add properties.
if (!grep { m|^PVE/Storage/Custom/| } keys %INC) {
    my @unlisted = grep { !$available->{$_} } sort keys $shared->%*;
    is("@unlisted", '', "all available properties are listed");
}

done_testing();
