package PVE::Multipath;

use v5.36;

use Encode qw(decode encode);
use JSON qw(decode_json);

use PVE::Tools qw(run_command file_read_firstline dir_glob_foreach);

use PVE::Diskmanage;
use PVE::Multipath::Config;

my $MULTIPATH = '/sbin/multipath';
my $MULTIPATHD = '/sbin/multipathd';

sub parse_dm_uuid($uuid) {
    my ($part, $wwid) = ($uuid // '') =~ /^(?:part([1-9][0-9]*)-)?mpath-(\S+)$/;
    return undef if !defined($wwid);
    return { wwid => $wwid, defined($part) ? (partition => int($part)) : () };
}

sub map_info() {
    my $maps = {};
    dir_glob_foreach(
        '/sys/block',
        'dm-\\d+',
        sub($dev, @) {
            my $info = parse_dm_uuid(file_read_firstline("/sys/block/$dev/dm/uuid"));
            return if !$info || $info->{partition};
            my $name = file_read_firstline("/sys/block/$dev/dm/name") // return;
            my @slaves;
            dir_glob_foreach(
                "/sys/block/$dev/slaves",
                '[^.].*',
                sub($slave, @) { push @slaves, $slave },
            );
            $maps->{$dev} = { %$info, name => $name, slaves => [sort @slaves] };
        },
    );
    return $maps;
}

sub member_map($maps) {
    my $members = {};
    for my $dev (sort keys %$maps) {
        $members->{$_} = $dev for $maps->{$dev}->{slaves}->@*;
    }
    return $members;
}

sub is_supported() { return -x $MULTIPATH && -x $MULTIPATHD ? 1 : 0 }
sub assert_supported() { die "multipath-tools is not installed\n" if !is_supported() }

sub is_running() {
    return 0 if !is_supported();
    my $running = 0;
    eval {
        run_command(
            [$MULTIPATHD, 'show', 'daemon'],
            timeout => 10,
            outfunc => sub($line) { $running = 1 if $line =~ /^pid \d+ \S+/ },
            errfunc => sub($line) { },
        );
    };
    return $running;
}

my sub string_value($value) {
    return undef if !defined($value) || $value eq '' || $value eq '[undef]';
    die "invalid string in multipathd output\n" if ref($value);
    return $value;
}

sub path_is_usable($path) {
    return ($path->{'dm-state'} // '') eq 'active'
        && (!defined($path->{'check-state'}) || $path->{'check-state'} =~ /^(?:ready|ghost)$/);
}

sub derive_health($active, $total, $expected = undef) {
    return 'failed' if !$total || !$active;
    return 'degraded' if $active < $total || $total < ($expected // 0);
    return 'optimal';
}

sub parse_maps_json($json) {
    $json = decode('UTF-8', $json) if !utf8::is_utf8($json);
    my $data = decode_json(encode('UTF-8', $json));
    die "invalid multipathd maps output\n"
        if ref($data) ne 'HASH' || ref($data->{maps} // []) ne 'ARRAY';
    my @maps;
    for my $raw (($data->{maps} // [])->@*) {
        die "invalid multipathd map\n" if ref($raw) ne 'HASH';
        my $map = { paths => [], 'paths-total' => 0, 'paths-active' => 0 };
        for my $pair ([uuid => 'wwid'], [name => 'name'], [sysfs => 'sysfs']) {
            my $value = string_value($raw->{ $pair->[0] });
            $map->{ $pair->[1] } = $value if defined($value);
        }
        for my $group (($raw->{path_groups} // [])->@*) {
            for my $rawpath (($group->{paths} // [])->@*) {
                my $path = { dev => string_value($rawpath->{dev}) // '' };
                for my $pair (
                    [dm_st => 'dm-state'],
                    [chk_st => 'check-state'],
                    [host_wwpn => 'host-wwpn'],
                    [target_wwpn => 'target-wwpn'],
                ) {
                    my $value = string_value($rawpath->{ $pair->[0] });
                    next if !defined($value) || ($pair->[0] =~ /wwpn$/ && $value eq '[unknown]');
                    $path->{ $pair->[1] } = $value;
                }
                push $map->{paths}->@*, $path;
                $map->{'paths-total'}++;
                $map->{'paths-active'}++ if path_is_usable($path);
            }
        }
        push @maps, $map;
    }
    return \@maps;
}

sub parse_path_identities($text) {
    my @lines = grep { length } split(/\n/, $text // '');
    return {} if @lines == 1 && $lines[0] eq 'ok';
    my $res = {};
    for my $line (@lines) {
        my ($dev, $wwid) = split(/\|/, $line, 2);
        return undef if !defined($wwid);
        next if $dev !~ /^sd[a-z]+$/ || $wwid !~ $PVE::Multipath::Config::WWID_RE;
        $res->{$dev} = { wwid => $wwid };
    }
    return $res;
}

sub query_path_identities() {
    assert_supported();
    my $text = '';
    run_command(
        [$MULTIPATHD, 'show', 'paths', 'raw', 'format', '%d|%w'],
        timeout => 5,
        outfunc => sub($line) { $text .= "$line\n" },
        errfunc => sub($line) { },
    );
    return parse_path_identities($text) // die "multipathd returned an invalid path list\n";
}

sub get_maps() {
    assert_supported();
    my $json = '';
    run_command(
        [$MULTIPATHD, 'show', 'maps', 'json'],
        timeout => 10,
        outfunc => sub($line) { $json .= "$line\n" },
    );
    my $maps = parse_maps_json($json);
    for my $map (@$maps) {
        $map->{size} = PVE::Diskmanage::get_sysdir_size("/sys/block/$map->{sysfs}")
            if $map->{sysfs};
    }
    return $maps;
}

sub wait_for_map($wwid, $timeout = 10) {
    my $link = "/dev/disk/by-id/dm-uuid-mpath-$wwid";
    my $deadline = time() + ($timeout // 10);
    while (1) {
        return 1 if -b $link;
        return undef if time() >= $deadline;
        sleep(1);
    }
}

1;
