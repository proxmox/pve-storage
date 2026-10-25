package PVE::API2::Multipath;

use v5.36;

use HTTP::Status qw(:constants);

use PVE::Exception qw(raise raise_param_exc);
use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTHandler;
use PVE::Tools qw(extract_param);

use PVE::Multipath::Config;

use base qw(PVE::RESTHandler);

my sub assert_map_name_free($cfg, $wwid, $name) {
    for my $other (sort keys $cfg->{ids}->%*) {
        next if $other eq $wwid;
        my $section = $cfg->{ids}->{$other};
        die "'$name' is already used by the entry of WWID '$other'\n"
            if $other eq $name || (defined($section->{alias}) && $section->{alias} eq $name);
    }
}

my sub apply_section_update($section, $param, $delete) {
    eval { PVE::Multipath::Config::apply_section_update($section, $param, $delete) };
    raise_param_exc({ delete => $@ }) if $@;
}

my sub edit_config_locked($digest, $errmsg, $code) {
    PVE::Multipath::Config::lock_config(
        sub {
            my $cfg = PVE::Multipath::Config::read_config();
            PVE::Tools::assert_if_modified($cfg->{digest}, $digest);
            $code->($cfg);
            PVE::Multipath::Config::write_cluster_config($cfg);
        },
        $errmsg,
    );
}

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
        return [map { +{ name => $_ } } qw(overrides wwid)];
    },
});

__PACKAGE__->register_method({
    name => 'read_overrides',
    path => 'overrides',
    method => 'GET',
    description => "Read the verbatim multipath.conf override sections.",
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {},
    },
    returns => {
        type => 'object',
        properties => {
            overrides => {
                type => 'string',
                description => 'Verbatim override sections.',
                optional => 1,
            },
            digest => get_standard_option('pve-config-digest'),
        },
    },
    code => sub {
        my $overrides = PVE::Multipath::Config::read_overrides();
        return {
            digest => PVE::Multipath::Config::overrides_digest($overrides),
            defined($overrides) && length($overrides) ? (overrides => $overrides) : (),
        };
    },
});

__PACKAGE__->register_method({
    name => 'set_overrides',
    path => 'overrides',
    method => 'PUT',
    protected => 1,
    description => "Set the verbatim multipath.conf sections for hardware-specific settings:"
        . " 'devices', 'overrides', 'defaults', 'blacklist' and 'blacklist_exceptions' blocks,"
        . " for example a 'devices' section with 'device' subsections.",
    permissions => {
        check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            overrides => {
                type => 'string',
                description => 'Verbatim override sections. Omit to clear them.',
                maxLength => 128 * 1024,
                optional => 1,
            },
            digest => get_standard_option('pve-config-digest'),
        },
    },
    returns => { type => 'null' },
    code => sub($param) {
        my $overrides = $param->{overrides};
        my $digest = extract_param($param, 'digest');
        eval { PVE::Multipath::Config::check_overrides($overrides) };
        raise_param_exc({ overrides => $@ }) if $@;

        PVE::Multipath::Config::lock_overrides(
            sub {
                my $current = PVE::Multipath::Config::read_overrides();
                PVE::Tools::assert_if_modified(
                    PVE::Multipath::Config::overrides_digest($current), $digest,
                );
                PVE::Multipath::Config::write_overrides($overrides // '');
            },
            "updating multipath overrides failed",
        );
        return undef;
    },
});

__PACKAGE__->register_method({
    name => 'wwid_index',
    path => 'wwid',
    method => 'GET',
    description => "List the explicit multipath entries with their alias and per-LUN settings.",
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {},
    },
    returns => {
        type => 'array',
        items => {
            type => 'object',
            properties => {
                wwid => { type => 'string', description => 'The LUN WWID.' },
                PVE::Multipath::Config::wwid_api_schema()->%*,
            },
        },
        links => [{ rel => 'child', href => '{wwid}' }],
    },
    code => sub {
        my $cfg = PVE::Multipath::Config::read_config();
        my $sections = PVE::Multipath::Config::wwid_sections($cfg);
        return [map { { wwid => $_, $sections->{$_}->%* } } sort keys %$sections];
    },
});

__PACKAGE__->register_method({
    name => 'read_wwid',
    path => 'wwid/{wwid}',
    method => 'GET',
    description => "Read the explicit multipath entry of a WWID, with the digest of the"
        . " configuration it was read from.",
    permissions => {
        check => ['perm', '/', ['Sys.Audit']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            wwid => {
                type => 'string',
                description => 'The WWID to read.',
                pattern => $PVE::Multipath::Config::WWID_PATTERN,
                maxLength => 128,
            },
        },
    },
    returns => {
        type => 'object',
        properties => {
            wwid => { type => 'string', description => 'The LUN WWID.' },
            PVE::Multipath::Config::wwid_api_schema()->%*,
            digest => get_standard_option('pve-config-digest'),
        },
    },
    code => sub($param) {
        my $wwid = $param->{wwid};

        my $cfg = PVE::Multipath::Config::read_config();
        my $entry = PVE::Multipath::Config::wwid_sections($cfg)->{$wwid}
            or raise("WWID '$wwid' has no explicit multipath entry\n", code => HTTP_NOT_FOUND);
        return { wwid => $wwid, $entry->%*, digest => $cfg->{digest} };
    },
});

__PACKAGE__->register_method({
    name => 'add_wwid',
    path => 'wwid',
    method => 'POST',
    protected => 1,
    description => "Add an explicit multipath entry for a LUN, with an optional alias, node"
        . " restriction, and per-LUN settings. For a LUN of an iSCSI storage with multipath"
        . " enabled, an entry is only needed to set an alias or per-LUN settings. The entry"
        . " keeps the LUN managed even if the storage's 'managed-multipath' option is disabled.",
    permissions => {
        check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            wwid => {
                type => 'string',
                description => 'The WWID of the LUN to manage via multipath.',
                pattern => $PVE::Multipath::Config::WWID_PATTERN,
                maxLength => 128,
            },
            PVE::Multipath::Config::wwid_api_schema()->%*,
            digest => get_standard_option('pve-config-digest'),
        },
    },
    returns => { type => 'null' },
    code => sub($param) {
        my $wwid = extract_param($param, 'wwid');
        my $digest = extract_param($param, 'digest');
        # The kernel joins the paths of a namespace with native NVMe multipath. No map would appear
        # for such an entry, while every node would add the WWID to the allow-list of multipath.
        if ($wwid =~ /^(?:eui|uuid|nvme)\./) {
            raise_param_exc({
                wwid => "WWID '$wwid' is an NVMe namespace, which uses native NVMe multipath",
            });
        }

        edit_config_locked(
            $digest,
            "adding WWID '$wwid' failed",
            sub($cfg) {
                if ($cfg->{ids}->{$wwid}) {
                    raise_param_exc({
                        wwid => "WWID '$wwid' already has an explicit multipath entry",
                    });
                }
                assert_map_name_free($cfg, $wwid, $wwid);
                assert_map_name_free($cfg, $wwid, $param->{alias}) if defined($param->{alias});
                my $section = $cfg->{ids}->{$wwid} = { type => 'wwid' };
                apply_section_update($section, $param, undef);
            },
        );
        return undef;
    },
});

__PACKAGE__->register_method({
    name => 'update_wwid',
    path => 'wwid/{wwid}',
    method => 'PUT',
    protected => 1,
    description => "Update the alias, node restriction, and per-LUN settings of an"
        . " explicit multipath entry.",
    permissions => {
        check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            wwid => {
                type => 'string',
                description => 'The WWID to update.',
                pattern => $PVE::Multipath::Config::WWID_PATTERN,
                maxLength => 128,
            },
            PVE::Multipath::Config::wwid_api_schema()->%*,
            delete => {
                type => 'string',
                format => 'pve-configid-list',
                description => 'Comma-separated list of settings to unset.',
                optional => 1,
            },
            digest => get_standard_option('pve-config-digest'),
        },
    },
    returns => { type => 'null' },
    code => sub($param) {
        my $wwid = extract_param($param, 'wwid');
        my $delete = extract_param($param, 'delete');
        my $digest = extract_param($param, 'digest');

        edit_config_locked(
            $digest,
            "updating WWID '$wwid' failed",
            sub($cfg) {
                my $section = $cfg->{ids}->{$wwid}
                    or die "WWID '$wwid' has no explicit multipath entry, add it first\n";
                assert_map_name_free($cfg, $wwid, $param->{alias}) if defined($param->{alias});
                apply_section_update($section, $param, $delete);
            },
        );
        return undef;
    },
});

__PACKAGE__->register_method({
    name => 'remove_wwid',
    path => 'wwid/{wwid}',
    method => 'DELETE',
    protected => 1,
    description => "Remove an explicit multipath entry without changing node allow-lists.",
    permissions => {
        check => ['perm', '/', ['Sys.Modify']],
    },
    parameters => {
        additionalProperties => 0,
        properties => {
            wwid => {
                type => 'string',
                description => 'The WWID to remove.',
                pattern => $PVE::Multipath::Config::WWID_PATTERN,
                maxLength => 128,
            },
            digest => get_standard_option('pve-config-digest'),
        },
    },
    returns => { type => 'null' },
    code => sub($param) {
        my $wwid = $param->{wwid};
        my $digest = extract_param($param, 'digest');

        edit_config_locked(
            $digest,
            "removing WWID '$wwid' failed",
            sub($cfg) {
                if (!$cfg->{ids}->{$wwid}) {
                    die "WWID '$wwid' has no explicit multipath entry - a LUN of a"
                        . " multipath-enabled storage is managed through that storage's"
                        . " 'managed-multipath' option\n";
                }
                delete $cfg->{ids}->{$wwid};
            },
        );
        return undef;
    },
});

1;
