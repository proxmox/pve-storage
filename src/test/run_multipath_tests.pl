#!/usr/bin/perl

use v5.36;
use lib ('.', '..');
use Test::More;
use Test::MockModule;
use PVE::Multipath;
use PVE::Multipath::Config;
use PVE::Tools qw(file_get_contents);

subtest 'multipathd JSON fixtures' => sub {
    my $maps =
        PVE::Multipath::parse_maps_json(file_get_contents('multipath-three-states.json'));
    is_deeply(
        [map { [$_->{'paths-active'}, $_->{'paths-total'}] } @$maps],
        [[2, 2], [1, 2], [0, 1]],
        'fixture path counts',
    );
    is_deeply(
        [
            map { PVE::Multipath::derive_health($_->{'paths-active'}, $_->{'paths-total'}) }
                @$maps
        ],
        [qw(optimal degraded failed)],
        'health derived from the path counts of each map',
    );
    is_deeply(
        [sort keys $maps->[0]->%*],
        [qw(name paths paths-active paths-total sysfs wwid)],
        'only consumed map fields are parsed',
    );
    is_deeply(
        [sort keys $maps->[0]->{paths}->[0]->%*],
        [qw(check-state dev dm-state host-wwpn target-wwpn)],
        'only consumed path fields are parsed',
    );
    is($maps->[0]->{paths}->[0]->{'target-wwpn'}, '0x500a098000000001', 'FC target port');
    my $json = '{ "maps": [ { "uuid": "3600a", "path_groups": [
        { "group": 1, "pri": 50, "paths": [ { "dev": "sda", "dm_st": "active", "chk_st": "ready" } ] },
        { "group": 2, "pri": 10, "paths": [ { "dev": "sdb", "dm_st": "active", "chk_st": "ghost" } ] }
    ] } ] }';
    my $map = PVE::Multipath::parse_maps_json($json)->[0];
    is($map->{'paths-active'}, 2, 'ALUA ghost path is usable');
    is_deeply(
        [map { $_->{dev} } $map->{paths}->@*],
        ['sda', 'sdb'],
        'groups flatten into one path list',
    );
    $json =~ s/"ghost"/"faulty"/;
    is(
        PVE::Multipath::parse_maps_json($json)->[0]->{'paths-active'},
        1,
        'faulty path is unusable',
    );
    $json =~ s/"ready"/"faulty"/;
    is(PVE::Multipath::parse_maps_json($json)->[0]->{'paths-active'}, 0, 'no usable paths');
    $json = '{ "maps": [ { "uuid": "3600a", "path_faults": "7", "path_groups": [
        { "group": "1", "pri": "50", "paths": [ { "dev": "sda", "dm_st": "active", "pri": "50" } ] }
    ] } ] }';
    $map = PVE::Multipath::parse_maps_json($json)->[0];
    is_deeply(
        $map,
        {
            wwid => '3600a',
            'paths-active' => 1,
            'paths-total' => 1,
            paths => [{ dev => 'sda', 'dm-state' => 'active' }],
        },
        'numeric strings in unused fields are ignored',
    );
    $json = '{ "maps": [ { "uuid": "3600a", "path_groups": [ { "group": 1, "pri": 50, "paths": [
        { "dev": "sda", "dm_st": "active", "host_wwpn": "[unknown]", "target_wwpn": "[unknown]" }
    ] } ] } ] }';
    my $path = PVE::Multipath::parse_maps_json($json)->[0]->{paths}->[0];
    ok(!exists($path->{'host-wwpn'}), 'unknown WWPN is omitted');
    $json =~ s/\[unknown\]/[undef]/g;
    $path = PVE::Multipath::parse_maps_json($json)->[0]->{paths}->[0];
    ok(!exists($path->{'target-wwpn'}), 'undefined WWPN is omitted');
    is(
        PVE::Multipath::parse_maps_json("{\"maps\":[{\"name\":\"lun\xff\"}]}")->[0]->{name},
        "lun\x{fffd}",
        'invalid UTF-8 alias is replaced, not a query failure',
    );
    is_deeply(PVE::Multipath::parse_maps_json('{}'), [], 'empty response');
    is_deeply(PVE::Multipath::parse_maps_json('{"maps":[]}'), [], 'empty list');
    eval { PVE::Multipath::parse_maps_json('not json') };
    ok($@, 'invalid JSON fails');
    is(PVE::Multipath::derive_health(1, 1, 2), 'degraded', 'removed expected path is degraded');
    is(PVE::Multipath::derive_health(0, 0), 'failed', 'empty map is failed');
};

subtest 'WWID entries, generated configuration and overrides check' => sub {
    my $cfg = PVE::Multipath::Config->parse_config(
        'multipath.cfg',
        "wwid: 3600a\n\talias data\n\tnodes n1\n\tno-path-retry fail\n\tpaths-expected 2\n",
    );
    is_deeply(PVE::Multipath::Config::wwid_list($cfg), ['3600a'], 'WWID is listed');
    is_deeply(PVE::Multipath::Config::wwids_for_node($cfg, 'n2'), [], 'node restriction');
    is_deeply(
        [sort keys PVE::Multipath::Config::wwid_api_schema()->%*],
        [qw(alias no-path-retry nodes paths-expected)],
        'only four options',
    );
    like(
        PVE::Multipath::Config::generate_managed_conf(),
        qr/auto_resize grow_only/,
        'generated defaults set auto_resize',
    );
    like(
        PVE::Multipath::Config::build_multipaths_block($cfg, 'n1'),
        qr/no_path_retry fail/,
        'per-LUN option',
    );
    unlike(
        PVE::Multipath::Config::build_multipaths_block($cfg, 'n1'),
        qr/expected/,
        'the expected path count is not a multipath option',
    );
    is_deeply(
        PVE::Multipath::Config::parse_wwids("# header\n/3600a/\n/-bad/\n"),
        ['3600a'],
        'allow-list parser',
    );
    eval { PVE::Multipath::Config::check_overrides('multipaths { }') };
    like($@, qr/configure LUNs/, 'LUN configuration belongs in explicit entries');
    eval { PVE::Multipath::Config::check_overrides("multipaths\ndefaults {\n}\n") };
    like($@, qr/configure LUNs/, 'also as a bare section name, which multipath still enters');
    # multipath reads the byte as space and closes 'devices' with the '}' of an unknown section
    my $nested = "devices {\n\tbogus {\n\t}\n\x80multipaths {\n\t}\n}\n";
    eval { PVE::Multipath::Config::check_overrides($nested) };
    like($@, qr/configure LUNs/, 'and at any depth');
    eval { PVE::Multipath::Config::check_overrides("\x80multipaths\ndefaults {\n}\n") };
    like($@, qr/configure LUNs/, 'also behind a byte outside of ASCII');
    $nested =~ s/\x80/\x{e9}/;
    eval { PVE::Multipath::Config::check_overrides($nested) };
    like($@, qr/configure LUNs/, 'or a letter outside of ASCII');
    eval { PVE::Multipath::Config::check_overrides("multipaths\"x\"y\ndefaults {\n}\n") };
    like($@, qr/configure LUNs/, 'or with a quoted string glued to it');
    eval { PVE::Multipath::Config::check_overrides("devices\n{\n}\n") };
    like($@, qr/'\{' has to follow its section name/, 'nor a brace on a line of its own');
    eval {
        PVE::Multipath::Config::check_overrides("defaults {\n\tuser_friendly_names no\n}\n");
    };
    is($@, '', 'raw defaults override is allowed');
    eval { PVE::Multipath::Config::check_overrides('defaults { user_friendly_names no }') };
    like($@, qr/'\{' has to end its line/, 'but not on one line, multipath ignores the rest');
    eval { PVE::Multipath::Config::check_overrides("defaults {\n\tuser_friendly_names no }\n") };
    like($@, qr/'\}' has to stand alone on its line/, 'nor with a closing brace after a value');
    eval {
        PVE::Multipath::Config::check_overrides("blacklist {\n\tdevnode ^nvme[0-9]{1,3}\n}\n");
    };
    like($@, qr/quote a value that contains braces/, 'a value with braces needs quotes');
};

subtest 'map identity from the dm UUID and map members' => sub {
    is_deeply(PVE::Multipath::parse_dm_uuid('mpath-3600a'), { wwid => '3600a' }, 'whole map');
    is_deeply(
        PVE::Multipath::parse_dm_uuid('part2-mpath-3600a'),
        { wwid => '3600a', partition => 2 },
        'partition of map',
    );
    ok(!PVE::Multipath::parse_dm_uuid('LVM-abc'), 'LV is not a multipath map');
    ok(!PVE::Multipath::parse_dm_uuid(undef), 'no identity does not invent a map');
    is_deeply(
        PVE::Multipath::member_map({ 'dm-0' => { slaves => ['sdb', 'sdc'] } }),
        { sdb => 'dm-0', sdc => 'dm-0' },
        'each member device maps to its map',
    );
};

subtest 'command timeouts, and the wait for a map only checks its by-id link' => sub {
    my $mp = Test::MockModule->new('PVE::Multipath');
    my @calls;
    $mp->mock(run_command => sub($cmd, %opts) { push @calls, [$cmd, $opts{timeout}] });
    ok(
        !PVE::Multipath::wait_for_map('nonexistent-test-lun', 0),
        'missing by-id block link times out',
    );
    is_deeply(\@calls, [], 'waiting never queries the daemon');
};

done_testing();
