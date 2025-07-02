package PVE::CephConfig;

use strict;
use warnings;
use Net::IP;

use PVE::RESTEnvironment qw(log_warn);
use PVE::Tools qw(run_command);
use PVE::Cluster qw(cfs_register_file);

cfs_register_file('ceph.conf', \&parse_ceph_config, \&write_ceph_config);

# For more information on how the Ceph parser works and how its grammar is
# defined, see:
# https://git.proxmox.com/?p=ceph.git;a=blob;f=ceph/src/common/ConfUtils.cc;h=2f78fd02bf9e27467275752e6f3bca0c5e3946ce;hb=e9fe820e7fffd1b7cde143a9f77653b73fcec748#l144
sub parse_ceph_config {
    my ($filename, $raw) = @_;

    my $cfg = {};
    return $cfg if !defined($raw);

    # Note: According to Ceph's config grammar, a single key-value pair in a file
    # (and nothing else!) is a valid config file and will be parsed by `ceph-conf`.
    # We choose not to handle this case here because it doesn't seem to be used
    # by Ceph at all (and otherwise doesn't really make sense anyway).

    # Regexes ending with '_class' consist of only an extended character class
    # each, which allows them to be interpolated into other ext. char classes.

    my $re_leading_ws = qr/^\s+/;
    my $re_trailing_ws = qr/\s+$/;

    my $re_continue_marker = qr/\\/;
    my $re_comment_class = qr/(?[ [ ; # ] ])/;
    my $re_not_allowed_in_section_header_class = qr/(?[ [ \] ] + $re_comment_class ])/;

    # Note: The Ceph config grammar defines keys the following way:
    #
    #     key %= raw[+(text_char - char_("=[ ")) % +blank];
    #
    # The ' - char_("=[ ")' expression might lure you into thinking that keys
    # may *not* contain spaces, but they can, due to the "% +blank" at the end!
    #
    # See: https://www.boost.org/doc/libs/1_42_0/libs/spirit/doc/html/spirit/qi/reference/operator/list.html
    #
    # Allowing spaces in this class and later squeezing whitespace as well as
    # removing any leading and trailing whitespace from keys is just so much
    # easier in our case.
    my $re_not_allowed_in_keys_class = qr/(?[ [  = \[  ] + $re_comment_class ])/;

    my $re_not_allowed_in_single_quoted_text_class = qr/(?[ [  '  ] + $re_comment_class ])/;
    my $re_not_allowed_in_double_quoted_text_class = qr/(?[ [  "  ] + $re_comment_class ])/;

    my $re_text_char = qr/\\.|(?[ ! $re_comment_class ])/;
    my $re_section_header_char = qr/\\.|(?[ ! $re_not_allowed_in_section_header_class ])/;

    my $re_key_char = qr/\\.|(?[ ! $re_not_allowed_in_keys_class ])/;

    my $re_single_quoted_text_char = qr/\\.|(?[ ! $re_not_allowed_in_single_quoted_text_class ])/;
    my $re_double_quoted_text_char = qr/\\.|(?[ ! $re_not_allowed_in_double_quoted_text_class ])/;

    my $re_single_quoted_value = qr/'(($re_single_quoted_text_char)*)'/;
    my $re_double_quoted_value = qr/"(($re_double_quoted_text_char)*)"/;

    my $re_key = qr/^(($re_key_char)+)/;
    my $re_quoted_value = qr/$re_single_quoted_value|$re_double_quoted_value/;
    my $re_unquoted_value = qr/(($re_text_char)*)/;
    my $re_value = qr/($re_quoted_value|$re_unquoted_value)/;

    my $re_kv_separator = qr/\s*(=)\s*/;

    my $re_section_start = qr/\[/;
    my $re_section_end = qr/\]/;
    my $re_section_header = qr/$re_section_start(($re_section_header_char)+)$re_section_end/;

    my $section;
    my @lines = split(/\n/, $raw);

    my $parse_section_header = sub {
        my ($section_line) = @_;

        # continued lines in section headers are allowed
        while ($section_line =~ s/$re_continue_marker$//) {
            $section_line .= shift(@lines);
        }

        my $remainder = $section_line;

        $remainder =~ s/$re_section_header//;
        my $parsed_header = $1;

        # Un-escape comment literals
        $parsed_header =~ s/\\($re_comment_class)/$1/g;

        if (!$parsed_header) {
            die "failed to parse section - skip: $section_line\n";
        }

        # preserve Ceph's behaviour and disallow anything after the section header
        # that's not whitespace or a comment
        $remainder =~ s/$re_leading_ws//;
        $remainder =~ s/^$re_comment_class.*$//;

        if ($remainder) {
            die "unexpected remainder after section - skip: $section_line\n";
        }

        return $parsed_header;
    };

    my $parse_key = sub {
        my ($line) = @_;

        my $remainder = $line;

        my $key = '';
        while ($remainder =~ s/$re_key//) {
            $key .= $1;

            while ($key =~ s/$re_continue_marker$//) {
                $remainder = shift(@lines);
            }
        }

        $key =~ s/$re_trailing_ws//;
        $key =~ s/$re_leading_ws//;

        $key =~ s/\s/ /;
        while ($key =~ s/\s\s/ /) { } # squeeze repeated whitespace

        # Ceph treats *single* spaces in keys the same as underscores,
        # but we'll just use underscores for readability
        $key =~ s/ /_/g;

        # Un-escape comment literals
        $key =~ s/\\($re_comment_class)/$1/g;

        if ($key eq '') {
            die "failed to parse key from line - skip: $line\n";
        }

        my $had_equals = $remainder =~ s/^$re_kv_separator//;

        if (!$had_equals) {
            die "expected '=' after key - skip: $line\n";
        }

        while ($remainder =~ s/^$re_continue_marker$//) {
            # Whitespace and continuations after equals sign can be arbitrary
            $remainder = shift(@lines);
            $remainder =~ s/$re_leading_ws//;
        }

        return ($key, $remainder);
    };

    my $parse_value = sub {
        my ($line, $remainder) = @_;

        my $starts_with_quote = $remainder =~ m/^['"]/;
        $remainder =~ s/$re_value//;
        my $value = $1 // '';

        if ($value eq '') {
            die "failed to parse value - skip: $line\n";
        }

        if ($starts_with_quote) {
            # If it started with a quote, the parsed value MUST end with a quote
            my $is_single_quoted = $value =~ m/$re_single_quoted_value/;
            $value = $1 if $is_single_quoted;
            my $is_double_quoted = !$is_single_quoted && $value =~ m/$re_double_quoted_value/;
            $value = $1 if $is_double_quoted;

            if (!($is_single_quoted || $is_double_quoted)) {
                die "failed to parse quoted value - skip: $line\n";
            }

            # Optionally, *only* line continuations may *only* follow right after
            while ($remainder =~ s/^$re_continue_marker$//) {
                $remainder .= shift(@lines);
            }

            # Nothing but whitespace or a comment may follow
            $remainder =~ s/$re_leading_ws//;
            $remainder =~ s/^$re_comment_class.*$//;

            if ($remainder) {
                die "unexpected remainder after value - skip: $line\n";
            }

        } else {
            while ($value =~ s/$re_continue_marker$//) {
                my $next_line = shift(@lines);

                $next_line =~ s/$re_unquoted_value//;
                my $value_part = $1 // '';
                $value .= $value_part;
            }

            $value =~ s/$re_trailing_ws//;
        }

        # Un-escape comment literals
        $value =~ s/\\($re_comment_class)/$1/g;

        return $value;
    };

    while (scalar(@lines)) {
        my $line = shift(@lines);

        $line =~ s/^\s*(?<!\\)$re_comment_class.*$//;
        $line =~ s/^\s*$//;
        next if !$line;
        next if $line =~ m/^$re_continue_marker$/;

        if ($line =~ m/$re_section_start/) {
            $section = undef;

            eval { $section = $parse_section_header->($line) };
            if ($@) {
                warn "$@\n";
            }

            if (defined($section)) {
                $cfg->{$section} = {} if !exists($cfg->{$section});
            }

            next;
        }

        if (!defined($section)) {
            warn "no section header - skip: $line\n";
            next;
        }

        my ($key, $remainder) = eval { $parse_key->($line) };
        if ($@) {
            warn "$@\n";
            next;
        }

        my $value = eval { $parse_value->($line, $remainder) };
        if ($@) {
            warn "$@\n";
            next;
        }

        $cfg->{$section}->{$key} = $value;
    }

    return $cfg;
}

my $parse_ceph_file = sub {
    my ($filename) = @_;

    my $cfg = {};

    return $cfg if !-f $filename;

    my $content = PVE::Tools::file_get_contents($filename);

    return parse_ceph_config($filename, $content);
};

sub write_ceph_config {
    my ($filename, $cfg) = @_;

    my $written_sections = {};
    my $out = '';

    my $cond_write_sec = sub {
        my $re = shift;

        for my $section (sort keys $cfg->%*) {
            next if $section !~ m/^$re$/;
            next if exists($written_sections->{$section});

            $out .= "[$section]\n";
            for my $key (sort keys $cfg->{$section}->%*) {
                $out .= "\t$key = $cfg->{$section}->{$key}\n";
            }
            $out .= "\n";

            $written_sections->{$section} = 1;
        }
    };

    my @rexprs = (
        qr/global/,

        qr/client/,
        qr/client\..*/,

        qr/mds/,
        qr/mds\..*/,

        qr/mon/,
        qr/mon\..*/,

        qr/osd/,
        qr/osd\..*/,

        qr/mgr/,
        qr/mgr\..*/,

        qr/.*/,
    );

    for my $re (@rexprs) {
        $cond_write_sec->($re);
    }

    # Escape comment literals that aren't escaped already
    $out =~ s/(?<!\\)([;#])/\\$1/gm;

    return $out;
}

my $ceph_get_key = sub {
    my ($keyfile, $username) = @_;

    my $key = $parse_ceph_file->($keyfile);
    my $secret = $key->{"client.$username"}->{key};

    return $secret;
};

my $get_host = sub {
    my ($hostport) = @_;
    my ($host, $port) = PVE::Tools::parse_host_and_port($hostport);
    if (!defined($host)) {
        return "";
    }
    $port = defined($port) ? ":$port" : '';
    $host = "[$host]" if Net::IP::ip_is_ipv6($host);
    return "${host}${port}";
};

sub get_monaddr_list {
    my ($configfile) = shift;

    if (!defined($configfile)) {
        warn "No ceph config specified\n";
        return;
    }

    my $config = $parse_ceph_file->($configfile);

    my $monhostlist = {};

    # get all ip addresses from mon_host
    my $monhosts = [split(/[ ,;]+/, $config->{global}->{mon_host} // "")];

    foreach my $monhost (@$monhosts) {
        $monhost =~ s/^\[?v\d\://; # remove beginning of vector
        $monhost =~ s|/\d+\]?||; # remove end of vector
        my $host = $get_host->($monhost);
        if ($host ne "") {
            $monhostlist->{$host} = 1;
        }
    }

    # then get all addrs from mon. sections
    for my $section (keys %$config) {
        next if $section !~ m/^mon\./;

        if (my $addr = $config->{$section}->{mon_addr}) {
            $monhostlist->{$addr} = 1;
        }
    }

    return join(',', sort keys %$monhostlist);
}

sub hostlist {
    my ($list_text, $separator) = @_;

    my @monhostlist = PVE::Tools::split_list($list_text);
    return join($separator, map { $get_host->($_) } @monhostlist);
}

my $ceph_check_keyfile = sub {
    my ($filename, $type) = @_;

    return if !-f $filename;

    my $content = PVE::Tools::file_get_contents($filename);
    eval {
        die if !$content;

        if ($type eq 'rbd') {
            die if $content !~ /\s*\[\S+\]\s*key\s*=\s*\S+==\s*$/m;
        } elsif ($type eq 'cephfs') {
            die if $content !~ /\S+==\s*$/;
        }
    };
    die "Not a proper $type authentication file: $filename\n" if $@;

    return undef;
};

sub ceph_connect_option {
    my ($scfg, $storeid, %options) = @_;

    my $cmd_option = {};
    my $keyfile = "/etc/pve/priv/ceph/${storeid}.keyring";
    $keyfile = "/etc/pve/priv/ceph/${storeid}.secret" if ($scfg->{type} eq 'cephfs');
    my $pveceph_managed = !defined($scfg->{monhost});

    $cmd_option->{ceph_conf} = '/etc/pve/ceph.conf' if $pveceph_managed;

    $ceph_check_keyfile->($keyfile, $scfg->{type});

    if (-e "/etc/pve/priv/ceph/${storeid}.conf") {
        # allow custom ceph configuration for external clusters
        if ($pveceph_managed) {
            warn
                "ignoring custom ceph config for storage '$storeid', 'monhost' is not set (assuming pveceph managed cluster)!\n";
        } else {
            $cmd_option->{ceph_conf} = "/etc/pve/priv/ceph/${storeid}.conf";
        }
    } elsif (!$pveceph_managed) {
        # No dedicated config for non-PVE-managed cluster, create new
        # TODO PVE 10 - remove. All such storages already got a configuration upon creation or here.
        ceph_create_configuration($scfg->{type}, $storeid);
    }

    $cmd_option->{keyring} = $keyfile if (-e $keyfile);
    $cmd_option->{auth_supported} = (defined $cmd_option->{keyring}) ? 'cephx' : 'none';
    $cmd_option->{userid} = $scfg->{username} ? $scfg->{username} : 'admin';
    $cmd_option->{mon_host} = hostlist($scfg->{monhost}, ',') if (defined($scfg->{monhost}));

    if (%options) {
        foreach my $k (keys %options) {
            $cmd_option->{$k} = $options{$k};
        }
    }

    return $cmd_option;

}

sub ceph_create_keyfile {
    my ($type, $storeid, $secret) = @_;

    my $extension = 'keyring';
    $extension = 'secret' if ($type eq 'cephfs');

    my $ceph_admin_keyring = '/etc/pve/priv/ceph.client.admin.keyring';
    my $ceph_storage_keyring = "/etc/pve/priv/ceph/${storeid}.$extension";

    die "ceph authx keyring file for storage '$storeid' already exists!\n"
        if -e $ceph_storage_keyring && !defined($secret);

    if (-e $ceph_admin_keyring || defined($secret)) {
        eval {
            if (defined($secret)) {
                mkdir '/etc/pve/priv/ceph';
                chomp $secret;
                PVE::Tools::file_set_contents($ceph_storage_keyring, "${secret}\n", 0400);
            } elsif ($type eq 'rbd') {
                mkdir '/etc/pve/priv/ceph';
                PVE::Tools::file_copy($ceph_admin_keyring, $ceph_storage_keyring);
            } elsif ($type eq 'cephfs') {
                my $cephfs_secret = $ceph_get_key->($ceph_admin_keyring, 'admin');
                mkdir '/etc/pve/priv/ceph';
                chomp $cephfs_secret;
                PVE::Tools::file_set_contents($ceph_storage_keyring, "${cephfs_secret}\n",
                    0400);
            }
        };
        if (my $err = $@) {
            unlink $ceph_storage_keyring;
            die "failed to copy ceph authx $extension for storage '$storeid': $err\n";
        }
    } else {
        warn "$ceph_admin_keyring not found, authentication is disabled.\n";
    }
}

sub ceph_remove_keyfile {
    my ($type, $storeid) = @_;

    my $extension = 'keyring';
    $extension = 'secret' if ($type eq 'cephfs');
    my $ceph_storage_keyring = "/etc/pve/priv/ceph/${storeid}.$extension";

    if (-f $ceph_storage_keyring) {
        unlink($ceph_storage_keyring) or warn "removing keyring of storage failed: $!\n";
    }
}

sub ceph_create_configuration {
    my ($type, $storeid) = @_;

    return if $type eq 'cephfs'; # no configuration file needed currently

    my $extension = 'keyring';
    $extension = 'secret' if $type eq 'cephfs';
    my $ceph_storage_keyring = "/etc/pve/priv/ceph/${storeid}.$extension";

    return if !-e $ceph_storage_keyring;

    my $ceph_storage_config = "/etc/pve/priv/ceph/${storeid}.conf";

    if (-e $ceph_storage_config) {
        log_warn(
            "file $ceph_storage_config already exists, check manually and ensure 'keyring'"
                . " option is set to '$ceph_storage_keyring'!\n",
        );
        return;
    }

    my $ceph_config = {
        global => {
            keyring => $ceph_storage_keyring,
        },
    };

    my $contents = PVE::CephConfig::write_ceph_config($ceph_storage_config, $ceph_config);
    PVE::Tools::file_set_contents($ceph_storage_config, $contents, 0600);

    return;
}

sub ceph_remove_configuration {
    my ($storeid) = @_;

    my $ceph_storage_config = "/etc/pve/priv/ceph/${storeid}.conf";
    if (-f $ceph_storage_config) {
        unlink $ceph_storage_config or log_warn("removing $ceph_storage_config failed - $!\n");
    }

    return;
}

my $ceph_version_parser = sub {
    my $ceph_version = shift;
    # FIXME this is the same as pve-manager PVE::Ceph::Tools get_local_version
    if ($ceph_version =~ /^ceph.*\sv?(\d+(?:\.\d+)+(?:-pve\d+)?)\s+(?:\(([a-zA-Z0-9]+)\))?/) {
        my ($version, $buildcommit) = ($1, $2);
        my $subversions = [split(/\.|-/, $version)];

        return ($subversions, $version, $buildcommit);
    }
    warn "Could not parse Ceph version: '$ceph_version'\n";
};

sub local_ceph_version {
    my ($cache) = @_;

    my $version_string = $cache;
    if (!defined($version_string)) {
        run_command(
            'ceph --version',
            outfunc => sub {
                $version_string = shift;
            },
        );
    }
    return undef if !defined($version_string);
    # subversion is an array ref. with the version parts from major to minor
    # version is the filtered version string
    my ($subversions, $version) = $ceph_version_parser->($version_string);

    return wantarray ? ($subversions, $version) : $version;
}

1;
