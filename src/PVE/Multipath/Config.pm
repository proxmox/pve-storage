package PVE::Multipath::Config;

use v5.36;

use PVE::Cluster qw(cfs_register_file cfs_read_file cfs_write_file cfs_lock_file);
use PVE::JSONSchema qw(get_standard_option);
use PVE::SectionConfig;
use PVE::Tools;

use base qw(PVE::SectionConfig);

# multipath interprets a leading '-' as an option and major:minor as a device number.
our $WWID_PATTERN = '(?!-|[0-9]+:-?[0-9])[a-zA-Z0-9._:-]+';
our $WWID_RE = qr/^(?:$WWID_PATTERN)\z/;

my $properties = {
    alias => {
        type => 'string',
        pattern => '[a-zA-Z0-9][a-zA-Z0-9._-]*',
        maxLength => 64,
        description => 'The name of the multipath map.',
        optional => 1,
    },
    nodes => get_standard_option(
        'pve-node-list',
        {
            description => 'The nodes that should assemble the map, or all nodes when unset.',
            optional => 1,
        },
    ),
    'no-path-retry' => {
        type => 'string',
        pattern => '(?:queue|fail|[0-9]+)',
        description => 'The action or number of retries when all paths are down.',
        optional => 1,
    },
    'paths-expected' => {
        type => 'integer',
        minimum => 1,
        description => 'The number of paths each node should have to the LUN. A node with fewer'
            . ' paths reports the LUN as degraded.',
        optional => 1,
    },
};
my $defaultData = {
    propertyList => {
        type => { description => 'The section type.' },
        id => {
            type => 'string',
            pattern => $WWID_PATTERN,
            maxLength => 128,
            description => 'The LUN WWID.',
        },
        %$properties,
    },
};
sub private($class) { return $defaultData }
sub type($class) { return 'wwid' }

sub options($class) {
    return { map { $_ => { optional => 1 } } keys %$properties };
}

sub wwid_api_schema() {
    return { map { $_ => { $properties->{$_}->%* } } keys %$properties };
}

sub parse_section_header($class, $line) {
    my ($type, $id, $errmsg, $config) = $class->SUPER::parse_section_header($line);
    return undef if !defined($type);
    $errmsg //= "invalid WWID '$id'" if $id !~ $WWID_RE;
    return ($type, $id, $errmsg, $config);
}

sub decode_value($class, $type, $key, $value) {
    if ($key eq 'nodes') {
        my @nodes = PVE::Tools::split_list($value);
        return { map { $_ => 1 } grep { PVE::JSONSchema::pve_verify_node_name($_) } @nodes };
    }
    return $value;
}

sub encode_value($class, $type, $key, $value) {
    return join(',', sort keys %$value) if $key eq 'nodes';
    return $value;
}
__PACKAGE__->register();
__PACKAGE__->init();

sub check_overrides($text) {
    my $allowed = { map { $_ => 1 } qw(devices overrides defaults blacklist blacklist_exceptions) };
    my $depth = 0;
    for my $line (split(/\n/, $text // '')) {
        # multipath ends a word at a quoted string and at a character outside of ASCII
        $line =~ s/"[^"]*"/ /g;
        $line =~ s/[^\x00-\x7f]/ /g;
        $line =~ s/[#!].*//;
        # multipath takes the first word of a line as section name, also without a '{' after it
        die "multipath overrides: configure LUNs in multipath.cfg, not a multipaths block\n"
            if $line =~ /^\s*multipaths\b/;
        # multipath wants '{' to end a line and '}' alone on one, it ignores text after a '{'
        die "multipath overrides: '{' has to end its line - quote a value that contains braces\n"
            if $line =~ /\{\s*\S/;
        die "multipath overrides: '}' has to stand alone on its line\n"
            if $line =~ /\}/ && $line !~ /^\s*\}\s*$/;
        while ($line =~ /(\w+)\s*\{|([{}])/g) {
            if (defined($1)) {
                my $keyword = $1;
                # at any depth, as multipath can nest the sections differently than counted here
                die "multipath overrides: configure LUNs in multipath.cfg, not a multipaths block\n"
                    if $keyword eq 'multipaths';
                die "multipath overrides: unknown section '$keyword'\n"
                    if !$depth && !$allowed->{$keyword};
                $depth++;
            } elsif ($2 eq '{') {
                die "multipath overrides: '{' has to follow its section name on the same line\n";
            } else {
                die "multipath overrides: unbalanced braces\n" if !$depth;
                $depth--;
            }
        }
    }
    die "multipath overrides: unbalanced braces\n" if $depth;
}

sub format_overrides($text) {
    $text //= '';
    $text =~ s/\s+$//;
    return length($text) ? "$text\n" : '';
}

cfs_register_file(
    'multipath.cfg',
    sub($filename, $data) { __PACKAGE__->parse_config($filename, $data) },
    sub($filename, $data) { __PACKAGE__->write_config($filename, $data) },
);
cfs_register_file(
    'multipath-overrides.conf',
    sub($filename, $data) { $data // '' },
    sub($filename, $data) { format_overrides($data) },
);

sub read_config() { return cfs_read_file('multipath.cfg') }
sub write_cluster_config($cfg) { cfs_write_file('multipath.cfg', $cfg) }
sub read_overrides() { return cfs_read_file('multipath-overrides.conf') }
sub write_overrides($text) { cfs_write_file('multipath-overrides.conf', $text) }

# An exception object passes unchanged, so that the API keeps its status code and field errors.
sub lock_config($code, $errmsg = undef) {
    cfs_lock_file('multipath.cfg', undef, $code);
    die ref($@) || !defined($errmsg) ? $@ : "$errmsg - $@" if $@;
}

sub lock_overrides($code, $errmsg = undef) {
    cfs_lock_file('multipath-overrides.conf', undef, $code);
    die ref($@) || !defined($errmsg) ? $@ : "$errmsg - $@" if $@;
}

sub generate_managed_conf() {
    return "# Managed by Proxmox VE.\ndefaults {\n\tauto_resize grow_only\n}\n";
}

sub parse_wwids($text) {
    return [map { m{^/($WWID_PATTERN)/\s*$} ? $1 : () } split(/\n/, $text // '')];
}

sub wwid_list($cfg) {
    return [sort keys $cfg->{ids}->%*];
}

sub wwids_for_node($cfg, $node) {
    return [grep { my $nodes = $cfg->{ids}->{$_}->{nodes}; !$nodes || !%$nodes || $nodes->{$node} }
        wwid_list($cfg)->@*];
}

sub build_multipaths_block($cfg, $node) {
    my $out = "# Managed by Proxmox VE.\nmultipaths {\n";
    for my $wwid (wwids_for_node($cfg, $node)->@*) {
        my $entry = $cfg->{ids}->{$wwid};
        $out .= "\tmultipath {\n\t\twwid $wwid\n";
        $out .= "\t\talias $entry->{alias}\n" if defined($entry->{alias});
        $out .= "\t\tno_path_retry $entry->{'no-path-retry'}\n"
            if defined($entry->{'no-path-retry'});
        $out .= "\t}\n";
    }
    return "$out}\n";
}

1;
