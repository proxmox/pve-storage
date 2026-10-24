#!/usr/bin/perl

use v5.36;
use lib ('.', '..');
use Test::More;
use PVE::Multipath;
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

done_testing();
