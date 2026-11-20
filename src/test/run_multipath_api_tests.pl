#!/usr/bin/perl

use v5.36;
our $NOW;

BEGIN {
    $NOW = 1000;
    *CORE::GLOBAL::time = sub { $NOW }
}
use lib ('.', '..');
use File::Temp qw(tempdir);
use Storable qw(dclone);
use Test::More;
use Test::MockModule;
use PVE::API2::Multipath;
use PVE::Multipath::Generator;
use PVE::Storage;

subtest 'add, update and remove an explicit entry' => sub {
    my $cfg = { ids => {}, digest => 'a' x 40 };
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    $config->mock(read_config => sub { dclone($cfg) });
    $config->mock(lock_config => sub { $_[0]->() });
    $config->mock(write_cluster_config => sub { $cfg = $_[0] });
    PVE::API2::Multipath->add_wwid({ wwid => '3600a', alias => 'data' });
    is(
        $cfg->{ids}->{'3600a'}->{alias},
        'data',
        'entry can be added without node reports or tools',
    );
    eval { PVE::API2::Multipath->add_wwid({ wwid => '3600b', alias => 'data' }) };
    like($@, qr/already|used|alias/, 'duplicate alias refused');

    my @namespaces = (
        'eui.0123456789abcdef',
        'uuid.6b5f0c22-1111-4e2a-9c3b-000000000001',
        'nvme.1b36-31323334-51454d55-00000001',
    );
    for my $wwid (@namespaces) {
        eval { PVE::API2::Multipath->add_wwid({ wwid => $wwid, 'paths-expected' => 2 }) };
        like($@, qr/native NVMe multipath/, "namespace WWID $wwid refused");
        ok(!$cfg->{ids}->{$wwid}, 'and not added');
    }
    PVE::API2::Multipath->update_wwid({ wwid => '3600a', 'no-path-retry' => 'fail' });
    is($cfg->{ids}->{'3600a'}->{'no-path-retry'}, 'fail', 'per-LUN setting updated');
    eval { PVE::API2::Multipath->update_wwid({ wwid => '3600a', digest => 'b' x 40 }) };
    like($@, qr/modified/, 'digest checked');
    PVE::API2::Multipath->remove_wwid({ wwid => '3600a' });
    is_deeply($cfg->{ids}, {}, 'entry removed without local device commands');

    for my $name (qw(read_wwid add_wwid update_wwid remove_wwid)) {
        my $schema = PVE::API2::Multipath->map_method_by_name($name)->{parameters};
        eval { PVE::JSONSchema::validate({ wwid => '8:16' }, $schema) };
        like($@, qr/wwid: value does not match/,
            "$name refuses an ambiguous WWID as parameter");
    }
    is_deeply(
        PVE::API2::Multipath->index({}),
        [map { { name => $_ } } qw(overrides status wwid)],
        'index',
    );
};

subtest 'a parameter error under the lock keeps its status and field' => sub {
    my $cfg = { ids => { '3600a' => { type => 'wwid' } }, digest => 'a' x 40 };
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    $config->mock(read_config => sub { dclone($cfg) });
    $config->mock(write_cluster_config => sub { $cfg = $_[0] });
    # as PVE::Cluster does it, the error of the code is left in $@
    $config->mock(
        cfs_lock_file => sub {
            eval { $_[2]->() };
            return undef;
        },
    );
    eval { PVE::API2::Multipath->add_wwid({ wwid => '3600a' }) };
    my $err = $@;
    is(ref($err) ? $err->{code} : $err, 400, 'a duplicate entry is a parameter error');
    like(
        ref($err) ? $err->{errors}->{wwid} : '',
        qr/already has an explicit multipath entry/,
        'of the WWID',
    );
    eval { PVE::API2::Multipath->update_wwid({ wwid => '3600a', digest => 'b' x 40 }) };
    like($@, qr/^updating WWID '3600a' failed - .*modified/, 'another error names the action');
};

subtest 'add-only generation and failed reload throttle' => sub {
    my $dir = tempdir('multipath-test-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my $mp = Test::MockModule->new('PVE::Multipath');
    my (%listed, @added, $reloads, $fail);
    $mp->mock(list_etc_multipath_wwids => sub { [sort keys %listed] });
    $mp->mock(add_wwid => sub { $listed{ $_[0] } = 1; push @added, $_[0] });
    $mp->mock(reconfigure => sub { $reloads++; die "reload failed\n" if $fail });
    my $cfg = { ids => { '3600a' => { type => 'wwid', alias => 'data' } } };
    my $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply(\@added, ['3600a'], 'missing WWID added');
    is_deeply($res->{wwids}->{'3600a'}, {}, 'no expected path count without one in the entry');
    is($reloads, 1, 'changed config reloaded');
    $res = PVE::Multipath::Generator::regenerate(
        { ids => { '3600a' => { $cfg->{ids}->{'3600a'}->%*, 'paths-expected' => 2 } } },
        '',
        {},
    );
    is($res->{wwids}->{'3600a'}->{'paths-expected'}, 2, 'expected path count of the entry');
    is($reloads, 1, 'unchanged config not reloaded');
    PVE::Multipath::Generator::regenerate({ ids => {} }, '', {});
    ok($listed{'3600a'}, 'removed entry remains allow-listed');
    $fail = 1;
    my $failed = PVE::Multipath::Generator::regenerate($cfg, '', {});
    like($failed->{error}, qr/reload failed/, 'reload error returned');
    ok(exists($failed->{wwids}->{'3600a'}), 'apply failure retains wanted LUNs');
    ok(-e "$dir/pending", 'failed reload remains pending');
    my $count = $reloads;
    $NOW += 59;
    $failed = PVE::Multipath::Generator::regenerate($cfg, '', {});
    like(
        $failed->{error},
        qr/^multipath reload pending after a failed reload - reload failed$/,
        'pending reload remains visible with its error',
    );
    is($reloads, $count, 'no retry before 60 seconds');
    $NOW -= 3600;
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is($reloads, ++$count, 'unless the clock went back');
    $NOW += 59;
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is($reloads, $count, 'which restarts the wait');
    $NOW++;
    $fail = 0;
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is($reloads, $count + 1, 'retry at 60 seconds');
    ok(!-e "$dir/pending", 'successful reload clears pending marker');
    $NOW += 3600;
};

subtest 'a WWID that multipath refuses does not hold up the others' => sub {
    my $dir = tempdir('multipath-refused-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my $mp = Test::MockModule->new('PVE::Multipath');
    my (%listed, %refuse, @tried, $reloads);
    $mp->mock(list_etc_multipath_wwids => sub { [sort keys %listed] });
    $mp->mock(
        add_wwid => sub($wwid) {
            push @tried, $wwid;
            die "command 'multipath -a -- $wwid' failed: exit code 1\n" if $refuse{$wwid};
            $listed{$wwid} = 1;
        },
    );
    $mp->mock(reconfigure => sub { $reloads++ });
    %refuse = ('3600b' => 1);
    my $cfg = { ids => { map { $_ => { type => 'wwid' } } qw(3600a 3600b 3600c) } };
    my $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply($res->{added}, [qw(3600a 3600c)], 'the other WWIDs are added');
    is($reloads, 1, 'and loaded with one reload');
    ok(!-e "$dir/pending", 'which clears the marker');
    like($res->{error}, qr/-- 3600b' failed/, 'the refused WWID is reported');
    @tried = ();
    $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    like($res->{error}, qr/-- 3600b' failed/, 'on every pass');
    is_deeply(\@tried, [], 'but not tried again at once');
    is($reloads, 1, 'nor reloaded again');
    $NOW += 300;
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply(\@tried, ['3600b'], 'only after five minutes');
    PVE::Multipath::Generator::regenerate($cfg, "defaults {\n}\n", {});
    is_deeply(\@tried, ['3600b', '3600b'], 'or after a drop-in changed');
    $NOW -= 3600;
    PVE::Multipath::Generator::regenerate($cfg, "defaults {\n}\n", {});
    is_deeply(\@tried, ['3600b', '3600b', '3600b'], 'or after the clock went back');
    $NOW += 3600;
    # derived WWIDs leave the drop-ins alone, whose change would also lift a refusal
    my $none = { ids => {} };
    my $with = { s1 => { wwids => { '3600a' => {}, '3600b' => {} } } };
    PVE::Multipath::Generator::regenerate($none, '', $with);
    PVE::Multipath::Generator::regenerate($none, '', { s1 => { wwids => { '3600a' => {} } } });
    @tried = ();
    PVE::Multipath::Generator::regenerate($none, '', $with);
    is_deeply(\@tried, ['3600b'], 'or after a pass that did not want it');

    %refuse = map { ("3601$_" => 1) } 1 .. 5;
    my $many = { ids => { map { ("3601$_" => { type => 'wwid' }) } 1 .. 5 } };
    $res = PVE::Multipath::Generator::regenerate($many, '', {});
    is(
        $res->{error},
        join('', map { "command 'multipath -a -- 3601$_' failed: exit code 1\n" } 1 .. 3)
            . "and 2 more WWIDs could not be added\n",
        'the error names the first three refused WWIDs and counts the others',
    );
    %refuse = map { ("3602$_" => 1) } 1 .. 4;
    my $four = { ids => { map { ("3602$_" => { type => 'wwid' }) } 1 .. 4 } };
    $res = PVE::Multipath::Generator::regenerate($four, '', {});
    is(
        $res->{error},
        join('', map { "command 'multipath -a -- 3602$_' failed: exit code 1\n" } 1 .. 4),
        'up to four refused WWIDs are all named',
    );

    $mp->mock(
        add_wwid => sub($wwid) {
            push @tried, $wwid;
            die "command 'multipath -a -- $wwid' failed: got timeout\n";
        },
    );
    %listed = ();
    @tried = ();
    $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply(\@tried, ['3600a'], 'a timeout ends the adds of the pass');
    like($res->{error}, qr/got timeout/, 'and is reported');
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply(\@tried, ['3600a', '3600a'], 'the WWID is tried again on the next pass');

    $mp->mock(
        add_wwid => sub($wwid) {
            die "command 'multipath -a -- $wwid' failed: got timeout\n" if $wwid eq '3709';
            die "command 'multipath -a -- $wwid' failed: exit code 1\n";
        },
    );
    my $mixed =
        { ids => { map { ($_ => { type => 'wwid' }) } qw(37001 37002 37003 37004 3709) } };
    $res = PVE::Multipath::Generator::regenerate($mixed, '', {});
    like($res->{error}, qr/\A[^\n]*-- 3709' failed: got timeout\n/, 'a timeout is named first');
    like($res->{error}, qr/and 2 more WWIDs/, 'before the summary of the refused WWIDs');
};

subtest 'the name of a map in place of a WWID counts as refused' => sub {
    my $dir = tempdir('multipath-name-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my $mp = Test::MockModule->new('PVE::Multipath');
    my (%listed, @tried, $reloads);
    my %maps = (data => '3600d');
    $mp->mock(list_etc_multipath_wwids => sub { [sort keys %listed] });
    # as multipath does, which adds the WWID of the map whose name it gets
    $mp->mock(
        add_wwid => sub($wwid) {
            push @tried, $wwid;
            $listed{ $maps{$wwid} // $wwid } = 1;
        },
    );
    $mp->mock(reconfigure => sub { $reloads++ });
    my $cfg = { ids => { map { $_ => { type => 'wwid' } } qw(3600a data) } };
    my $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply($res->{added}, ['3600a'], 'the name does not count as added');
    is(
        $res->{error},
        "multipath did not list 'data' after adding it - it is probably the name of a map, not a"
            . " WWID\n",
        'but as refused',
    );
    @tried = ();
    my $count = $reloads;
    PVE::Multipath::Generator::regenerate($cfg, '', {}) for 1 .. 2;
    $res = PVE::Multipath::Generator::regenerate($cfg, '', {});
    like($res->{error}, qr/did not list 'data'/, 'which the next passes report');
    is_deeply(\@tried, [], 'without adding it again');
    is($reloads, $count, 'or a reload');
    $NOW += 300;
    PVE::Multipath::Generator::regenerate($cfg, '', {});
    is_deeply(\@tried, ['data'], 'until five minutes passed');
};

subtest 'a wanted WWID expects the highest path count of its entry and storages' => sub {
    my $cfg =
        { ids => { map { $_ => { type => 'wwid', 'paths-expected' => 3 } } qw(3600a 3600b) } };
    $cfg->{ids}->{'3600c'} = { type => 'wwid', 'paths-expected' => 2, nodes => { n2 => 1 } };
    my $derived = {
        s1 => {
            wwids => {
                '3600a' => { 'paths-expected' => 2 },
                '3600b' => { 'paths-expected' => 4 },
                '3600c' => {},
            },
        },
        s2 => { wwids => { '3600b' => { 'paths-expected' => 1 } } },
    };
    is_deeply(
        PVE::Multipath::Generator::wanted_wwids($cfg, 'n1', $derived),
        {
            '3600a' => { storages => ['s1'], 'paths-expected' => 3 },
            '3600b' => { storages => [qw(s1 s2)], 'paths-expected' => 4 },
            '3600c' => { storages => ['s1'] },
        },
        'the entries for the node and the WWIDs of the storages',
    );
};

subtest 'drop-ins of the same name that an administrator wrote stay' => sub {
    my $dir = tempdir('multipath-foreign-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $mp = Test::MockModule->new('PVE::Multipath');
    my $reloads = 0;
    $mp->mock(list_etc_multipath_wwids => sub { [] });
    $mp->mock(add_wwid => sub { });
    $mp->mock(reconfigure => sub { $reloads++ });
    my $own = "defaults {\n\tfind_multipaths strict\n}\n";
    my $admin = "devices {\n\tdevice {\n\t\tvendor \"NETAPP\"\n\t}\n}\n";
    PVE::Tools::file_set_contents("$dir/overrides", $admin);

    my $near = "# Not managed by Proxmox VE\nmultipaths {\n}\n";
    PVE::Tools::file_set_contents("$dir/aliases", $near);
    my $res = PVE::Multipath::Generator::regenerate({ ids => {} }, '', {});
    is(PVE::Tools::file_get_contents("$dir/overrides"), $admin, 'kept on an unconfigured node');
    is(PVE::Tools::file_get_contents("$dir/aliases"), $near, 'also with a similar first line');
    ok(!$res->{error} && !$reloads, 'without error or reload');
    my $edit = "# Managed by Proxmox VE, edited by hand\nmultipaths {\n}\n";
    PVE::Tools::file_set_contents("$dir/aliases", $edit);
    PVE::Multipath::Generator::regenerate({ ids => {} }, '', {});
    is(PVE::Tools::file_get_contents("$dir/aliases"), $edit, 'also with an edited first line');
    # an outdated file of Proxmox VE that the next pass would remove
    my $stale = "# Managed by Proxmox VE.\nmultipaths {\n}\n";
    PVE::Tools::file_set_contents("$dir/aliases", $stale);
    $res = PVE::Multipath::Generator::regenerate({ ids => {} }, $own, {});
    is(PVE::Tools::file_get_contents("$dir/overrides"), $admin, 'kept when overrides are set');
    like($res->{error}, qr/not written by Proxmox VE, rename it/, 'with an error');
    is($reloads, 0, 'and no reload');
    ok(!-e "$dir/defaults", 'no other drop-in is written');
    is(PVE::Tools::file_get_contents("$dir/aliases"), $stale, 'or removed');
    ok(!-e "$dir/pending", 'or marked for a reload');

    unlink("$dir/overrides");
    $res = PVE::Multipath::Generator::regenerate({ ids => {} }, $own, {});
    like(
        PVE::Tools::file_get_contents("$dir/overrides"),
        qr/^# Managed by Proxmox VE\./,
        'written once the name is free',
    );
    $res = PVE::Multipath::Generator::regenerate({ ids => {} }, '', {});
    ok(!-e "$dir/overrides" && !$res->{error}, 'removed again when no longer needed');
};

subtest 'overrides that the API refuses are not applied' => sub {
    my $dir = tempdir('multipath-overrides-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my $mp = Test::MockModule->new('PVE::Multipath');
    my ($added, $reloads) = (0, 0);
    $mp->mock(list_etc_multipath_wwids => sub { [] });
    $mp->mock(add_wwid => sub { $added++ });
    $mp->mock(reconfigure => sub { $reloads++ });
    PVE::Multipath::Generator::regenerate({ ids => {} }, "defaults {\n}\n", {});
    my %before = map { $_ => PVE::Tools::file_get_contents("$dir/$_") } qw(defaults overrides);
    ($added, $reloads) = (0, 0);
    # as a hand edit of /etc/pve/multipath-overrides.conf can leave it
    my $text = "multipaths {\n\tmultipath {\n\t\twwid 3600b\n\t}\n}\n";
    my $res = PVE::Multipath::Generator::regenerate(
        { ids => { '3600a' => { type => 'wwid' } } },
        $text,
        {},
    );
    like($res->{error}, qr/not a multipaths block/, 'a multipaths block is an apply error');
    is_deeply(
        { map { $_ => PVE::Tools::file_get_contents("$dir/$_") } qw(defaults overrides) },
        \%before,
        'the drop-ins keep their last valid content',
    );
    ok(!-e "$dir/aliases" && !$added && !$reloads,
        'nothing else is written, added or reloaded');
};

subtest 'storage LUNs are the identified paths of the storage target' => sub {
    my $topology = Test::MockModule->new('PVE::ISCSI::Topology');
    $topology->mock(
        device_map => sub {
            return { map { $_ => { target => $_ eq 'sdc' ? 'iqn.other' : 'iqn.san' } }
                qw(sda sdb sdc) };
        },
    );
    my $identity = Test::MockModule->new('PVE::Multipath::Identity');
    $identity->mock(dir_glob_foreach => sub { $_[2]->($_) for qw(sda sdb sdc sdd) });
    my $mp = Test::MockModule->new('PVE::Multipath');
    my %wwid = (sda => '3600a', sdb => '3600a', sdc => '3600b', sdd => '3600c');
    $mp->mock(
        query_path_identities => sub {
            return { map { $_ => { wwid => $wwid{$_} } } keys %wwid };
        },
    );
    my $iscsi = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
    $iscsi->mock(
        iscsi_portals => sub($target, @) {
            die "error: no iscsi support - please install open-iscsi\n" if $target ne 'iqn.san';
            return ['192.0.2.1:3260', '192.0.2.2:3260'];
        },
    );
    my $cfg =
        { ids => { san => { type => 'iscsi', target => 'iqn.san', 'managed-multipath' => 1 } } };
    my $expected = { san => { wwids => { '3600a' => { 'paths-expected' => 2 } } } };
    is_deeply(
        PVE::Multipath::Generator::derived_wwids($cfg),
        $expected,
        'one WWID per LUN of the target, one expected path per portal',
    );
    $cfg->{ids}->{none} = { type => 'iscsi', target => 'iqn.none', 'managed-multipath' => 1 };
    is_deeply(
        scalar(eval { PVE::Multipath::Generator::derived_wwids($cfg) }),
        { %$expected, none => { wwids => {} } },
        'no portal lookup, which takes iscsiadm, for a target without paths on the node',
    );
    delete $cfg->{ids}->{none};
    my $all = PVE::Multipath::Identity::collect(all => 1);
    is_deeply(
        PVE::Multipath::Generator::derived_wwids($cfg, $all),
        $expected,
        'paths of other targets and transports in a full collection are left out',
    );
};

subtest 'an unconfigured node does not write drop-ins or reload' => sub {
    my $dir = tempdir('multipath-empty-XXXX', DIR => '.', CLEANUP => 1);
    local $PVE::Multipath::Generator::DEFAULTS_DROPIN = "$dir/defaults";
    local $PVE::Multipath::Generator::OVERRIDES_DROPIN = "$dir/overrides";
    local $PVE::Multipath::Generator::ALIASES_DROPIN = "$dir/aliases";
    local $PVE::Multipath::Generator::RECONFIGURE_PENDING = "$dir/pending";
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    $config->mock(read_config => sub { { ids => {} } });
    $config->mock(read_overrides => sub { '' });
    my $storage = Test::MockModule->new('PVE::Storage');
    my $identity = Test::MockModule->new('PVE::Multipath::Identity');
    $identity->mock(collect => sub { die "unexpected identity collection\n" });
    my $mp = Test::MockModule->new('PVE::Multipath');
    $mp->mock(is_supported => sub { 1 });
    $mp->mock(is_running => sub { 1 });
    $mp->mock(get_maps => sub { [] });
    $mp->mock(list_etc_multipath_wwids => sub { [] });
    my $generator = Test::MockModule->new('PVE::Multipath::Generator');
    my @changes;

    for my $function (qw(file_set_contents make_path)) {
        $generator->mock($function => sub { push @changes, $function });
    }
    for my $function (qw(add_wwid reconfigure)) {
        $mp->mock($function => sub { push @changes, $function });
    }
    my $iscsi = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
    $iscsi->mock(read_initiator_info => sub { {} });
    my $status = Test::MockModule->new('PVE::Multipath::Status');
    my $report;
    $status->mock(publish => sub { $report = $_[0] });
    for my $ids ({}, { plain => { type => 'iscsi', target => 'iqn.test' } }) {
        $storage->mock(config => sub { { ids => $ids } });
        PVE::Multipath::Status::update_node_status();
        ok(!exists($report->{error}), 'running daemon reports successfully');
        ok(!exists($report->{'apply-error'}), 'unconfigured application succeeds');
        is_deeply(\@changes, [], 'no write, mkdir, allow-list addition or reload');
        is_deeply([glob("$dir/*")], [], 'no generated files or pending marker');
    }
};

my sub encode_report($report) { return PVE::Multipath::Status::encode_report($report) }

# rows as a node reports them for many LUNs, by default with random SCSI WWIDs
my sub luns($count, $prefix = '3') {
    require Digest::MD5;
    return [
        map {
            my $hash = Digest::MD5::md5_hex("wwid$_");
            {
                wwid => $prefix . substr($hash . $hash, 0, 33 - length($prefix)),
                size => (16 + hex(substr(Digest::MD5::md5_hex("size$_"), 0, 3))) * 1024**3,
                vgname => sprintf('vg-lun-%04d', $_),
                storages => ['san-netapp01'],
                state => 'optimal',
                'paths-total' => 4,
                'paths-active' => 4,
                'paths-expected' => 4,
            }
        } 1 .. $count
    ];
}

subtest 'unchanged reports wait a minute, lost ones are resent, oversized ones are errors' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my (%raw, @sent);
    $cluster->mock(get_node_kv => sub { \%raw });
    $cluster->mock(
        broadcast_node_kv => sub($key, $data) {
            is($key, 'multipath', 'one node key');
            push @sent, PVE::Multipath::Status::decode_report($data);
            $raw{n1} = $data;
        },
    );
    my $report = {
        luns => [{
            wwid => '3600a',
            name => 'data',
            state => 'optimal',
            'paths-total' => 2,
            'paths-active' => 2,
        }],
    };
    PVE::Multipath::Status::publish($report);
    $NOW += 59;
    PVE::Multipath::Status::publish($report);
    is(scalar(@sent), 1, 'same report younger than 60 seconds is skipped');
    $NOW++;
    PVE::Multipath::Status::publish($report);
    is(scalar(@sent), 2, 'report refreshed at 60 seconds');
    %raw = ();
    PVE::Multipath::Status::publish($report);
    is(scalar(@sent), 3, 'lost pmxcfs value is republished without local send state');
    PVE::Multipath::Status::publish(
        { luns => luns(2000), 'iscsi-initiator-name' => 'iqn.2026-11.test:n1' });
    is($sent[-1]->{error}, 'too many LUNs to report', 'oversized report is an error');
    ok(!exists($sent[-1]->{luns}), 'not an empty or partial LUN report');
    is(
        $sent[-1]->{'iscsi-initiator-name'},
        'iqn.2026-11.test:n1',
        'which keeps the node identity',
    );
};

subtest 'a compressed report holds 1000 LUNs of one array' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $node = Test::MockModule->new('PVE::INotify');
    $node->mock(nodename => sub { 'n1' });
    my (%raw, @sent);
    $cluster->mock(get_node_kv => sub { \%raw });
    $cluster->mock(broadcast_node_kv => sub($key, $data) { push @sent, $raw{n1} = $data });
    my $report = { luns => luns(1000, '3600a098038314c4a') };
    PVE::Multipath::Status::publish($report);
    my $sent = PVE::Multipath::Status::decode_report($sent[0]);
    is_deeply($sent->{luns}, $report->{luns}, 'all rows are sent');
    like($sent[0], qr{\A[A-Za-z0-9+/]+=*\z}, 'in base64 without line breaks');
    is_deeply(
        JSON::decode_json(Compress::Zlib::uncompress(MIME::Base64::decode_base64($sent[0]))),
        $sent,
        'of the JSON compressed with zlib',
    );
    PVE::Multipath::Status::publish($report);
    is(scalar(@sent), 1, 'and read back');
};

subtest 'only managed maps and those under a storage get a row' => sub {
    my $gen = Test::MockModule->new('PVE::Multipath::Generator');
    my $status = Test::MockModule->new('PVE::Multipath::Status');
    my $mp = Test::MockModule->new('PVE::Multipath');
    my $iscsi = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
    my $storage = Test::MockModule->new('PVE::Storage');
    my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
    my $report;
    $status->mock(publish => sub { $report = $_[0] });
    $iscsi->mock(read_initiator_info => sub { undef });
    $mp->mock(is_supported => sub { 1 }, is_running => sub { 1 });
    $mp->mock(
        get_maps => sub {
            return [
                map { {
                    wwid => "3600$_",
                    sysfs => "dm-$_",
                    'paths-active' => 2,
                    'paths-total' => 2,
                } } 1 .. 5
            ];
        },
    );
    $gen->mock(sync => sub { { wwids => { '36001' => {} } } });
    $lvm->mock(
        cached_pv_volume_groups => sub { { '/dev/dm-2' => 'vgsan', '/dev/dm-3' => 'vgother' } },
    );
    my $ids = {
        san => { type => 'lvm', vgname => 'vgsan' },
        based =>
            { type => 'lvm', vgname => 'vgbase', base => 'conn:0.0.0.dm-uuid-mpath-36004' },
    };
    $storage->mock(config => sub { { ids => $ids } });
    my $returned = PVE::Multipath::Status::update_node_status();
    is_deeply(
        [map { $_->{wwid} } $report->{luns}->@*],
        ['36001', '36002', '36004'],
        'a wanted map, one with the volume group of a storage and a base volume',
    );
    is_deeply(
        [map { $_->{wwid} } $returned->{luns}->@*],
        ['36001', '36002', '36004'],
        'the update returns the report it publishes',
    );
    is($report->{'other-maps'}, 2, 'the other maps are counted');
    ok(!exists($report->{'vgs-unknown'}), 'volume groups known from the hints of LVM');

    for my $base (qw(conn scsi-conn)) {
        local $ids->{legacy} =
            { type => 'lvm', vgname => 'vglegacy', base => "$base:0.0.1.scsi-36005" };
        PVE::Multipath::Status::update_node_status();
        ok(
            grep({ $_->{wwid} eq '36005' } $report->{luns}->@*),
            "and a base volume by its SCSI name, on storage '$base'",
        );
    }
    $lvm->mock(cached_pv_volume_groups => sub { undef });
    PVE::Multipath::Status::update_node_status();
    is_deeply(
        [map { $_->{wwid} } $report->{luns}->@*],
        ['36001', '36004'],
        'without hints only the maps that the configuration names',
    );
    is($report->{'other-maps'}, 3, 'the map of the volume group counts as other');
    ok($report->{'vgs-unknown'}, 'and the report says that volume groups are unknown');
    $storage->mock(config => sub { die "no configuration\n" });
    PVE::Multipath::Status::update_node_status();
    is(scalar($report->{luns}->@*), 5, 'all maps without the storage configuration');
    ok(!exists($report->{'other-maps'}), 'and no count');
    $storage->mock(config => sub { { ids => $ids } });
    $mp->mock(get_maps => sub { [] });
    PVE::Multipath::Status::update_node_status();
    ok(!exists($report->{'vgs-unknown'}),
        'unknown volume groups only matter for left out maps');
};

subtest 'a LUN of a storage without a map is missing' => sub {
    my $gen = Test::MockModule->new('PVE::Multipath::Generator');
    my $status = Test::MockModule->new('PVE::Multipath::Status');
    my $mp = Test::MockModule->new('PVE::Multipath');
    my $iscsi = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
    my $storage = Test::MockModule->new('PVE::Storage');
    my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    my %raw;
    $status->mock(publish => sub { $raw{n1} = encode_report({ $_[0]->%*, time => $NOW }) });
    $iscsi->mock(read_initiator_info => sub { undef });
    $mp->mock(is_supported => sub { 1 }, is_running => sub { 1 }, get_maps => sub { [] });
    $lvm->mock(cached_pv_volume_groups => sub { {} });
    $gen->mock(
        sync => sub {
            return {
                wwids => {
                    '3600a' => { storages => ['san'], 'paths-expected' => 2 },
                    '3600b' => {},
                    '3600c' => { storages => ['san'] },
                },
                added => ['3600c'],
            };
        },
    );
    $storage->mock(
        config => sub { { ids => { san => { type => 'iscsi', 'managed-multipath' => 1 } } } });
    PVE::Multipath::Status::update_node_status();
    is_deeply(
        PVE::Multipath::Status::decode_report($raw{n1})->{luns},
        [{ wwid => '3600a', state => 'missing', storages => ['san'], 'paths-expected' => 2 }],
        'the LUN of the storage, not one of an entry, nor one added in this pass',
    );
    $cluster->mock(get_nodelist => sub { ['n1'] }, get_members => sub { {} });
    $cluster->mock(get_node_kv => sub { \%raw });
    $config->mock(read_config => sub { { ids => {} } });
    my ($row) = PVE::Multipath::Status::cluster_status()->{luns}->@*;
    is($row->{wwid}, '3600a', 'the cluster status lists it');
    is($row->{state}, 'missing', 'as missing');
    is($row->{'multipath-storage'}, 'san', 'with the storage that requests it');
};

subtest 'one entry point applies configuration and reports unavailable reasons' => sub {
    my $gen = Test::MockModule->new('PVE::Multipath::Generator');
    my $status = Test::MockModule->new('PVE::Multipath::Status');
    my $mp = Test::MockModule->new('PVE::Multipath');
    my $iscsi = Test::MockModule->new('PVE::Storage::ISCSIPlugin');
    my ($report, @order);
    $gen->mock(sync => sub { push @order, 'apply'; { error => 'reload failed' } });
    $status->mock(publish => sub { push @order, 'publish'; $report = $_[0] });
    $iscsi->mock(read_initiator_info => sub { { name => 'iqn.2025-01.example:node' } });
    $mp->mock(is_supported => sub { 0 });
    PVE::Multipath::Status::update_node_status();
    is($report->{error}, 'multipath-tools is not installed', 'missing tools reason only');
    is_deeply(
        \@order,
        [qw(apply publish)],
        'the configuration is applied before the status is published',
    );
    is($report->{'apply-error'}, 'reload failed', 'apply failure is separate');
    $mp->mock(is_supported => sub { 1 });
    $mp->mock(is_running => sub { 0 });
    PVE::Multipath::Status::update_node_status();
    is(
        $report->{error},
        'multipathd is not running or does not respond',
        'stopped daemon reason only',
    );
    $mp->mock(is_running => sub { 1 });
    $mp->mock(get_maps => sub { die "bad JSON\n" });
    PVE::Multipath::Status::update_node_status();
    is($report->{error}, 'bad JSON', 'query error without unavailable prefix');
    $mp->mock(get_maps =>
        sub { [{ wwid => '3600a', name => 'data', 'paths-active' => 1, 'paths-total' => 2 }] });
    PVE::Multipath::Status::update_node_status();
    is($report->{luns}->[0]->{state}, 'degraded', 'all maps while the wanted ones are unknown');
    ok(!exists($report->{luns}->[0]->{'paths-expected'}), 'no expected count without a source');
    $gen->mock(sync => sub { { wwids => { '3600a' => { 'paths-expected' => 3 } } } });
    $mp->mock(get_maps =>
        sub { [{ wwid => '3600a', name => 'data', 'paths-active' => 2, 'paths-total' => 2 }] });
    PVE::Multipath::Status::update_node_status();
    is($report->{luns}->[0]->{'paths-expected'}, 3, 'expected count reported');
    is($report->{luns}->[0]->{state}, 'degraded', 'fewer paths than expected');
    ok(!exists($report->{supported}), 'the report has no separate "supported" field');
};

subtest 'API returns final node and LUN states' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    my $storage = Test::MockModule->new('PVE::Storage');
    $cluster->mock(get_nodelist => sub { [qw(n1 n2 n3 n4 n5)] });
    $cluster->mock(get_members => sub { { n4 => { online => 0 } } });
    my %raw = (
        n1 => encode_report({
            time => $NOW,
            luns => [
                {
                    wwid => '3600a',
                    state => 'optimal',
                    'paths-total' => 2,
                    'paths-active' => 2,
                    'paths-expected' => 2,
                    storages => ['iscsi'],
                    vgname => 'vgfc',
                },
                {
                    wwid => '3600d',
                    state => 'optimal',
                    'paths-total' => 2,
                    'paths-active' => 2,
                    storages => ['off'],
                    vgname => 'vgshared',
                },
                {
                    wwid => '3600f',
                    state => 'optimal',
                    'paths-total' => 2,
                    'paths-active' => 2,
                    storages => ['scoped'],
                },
            ],
        }),
        n2 => encode_report({ time => $NOW, luns => [] }),
        n3 => encode_report({ time => $NOW - 181, luns => [] }),
        n4 => encode_report({ time => $NOW, luns => [] }),
        n5 => encode_report({ time => $NOW, error => 'multipath-tools is not installed' }),
    );
    $cluster->mock(get_node_kv => sub { \%raw });
    my $entries = { '3600a' => { type => 'wwid' } };
    $config->mock(read_config => sub { { ids => $entries } });
    $storage->mock(
        config => sub { {
            ids => {
                iscsi => { type => 'iscsi', 'managed-multipath' => 1 },
                lvm => { type => 'lvm', base => 'iscsi:0.0.0.dm-uuid-mpath-3600b' },
                fc => { type => 'lvm', vgname => 'vgfc' },
                off => { type => 'iscsi' },
                lvmoff => { type => 'lvm', base => 'off:0.0.0.dm-uuid-mpath-3600c' },
                shared => { type => 'lvm', vgname => 'vgshared', shared => 1 },
                gone =>
                    { type => 'iscsi', 'managed-multipath' => 1, nodes => { departed => 1 } },
                lvmgone => { type => 'lvm', base => 'gone:0.0.0.dm-uuid-mpath-3600e' },
                scoped => { type => 'iscsi', 'managed-multipath' => 1, nodes => { n2 => 1 } },
            },
        } },
    );
    my $res = PVE::API2::Multipath->status({});
    is_deeply(
        [map { $res->{nodes}->{$_}->{state} } qw(n1 n2 n3 n4 n5)],
        [qw(ok ok no-report no-report unavailable)],
        'final node states',
    );
    is($res->{nodes}->{n3}->{time}, $NOW - 181, 'old report time retained');
    my $lun = $res->{luns}->[0];
    is($lun->{state}, 'missing', 'worst known state');
    is($lun->{nodes}->{n1}->{state}, 'optimal', 'observed healthy map');
    is($lun->{nodes}->{n2}->{state}, 'missing', 'expected map absent from current report');
    is($lun->{nodes}->{n3}->{state}, 'unknown', 'stale observation');
    is($lun->{'used-by-storage'}, 'fc', 'storage attributed through the reported volume group');
    is($res->{luns}->[1]->{wwid}, '3600b', 'LVM base included without observation');
    is($res->{luns}->[1]->{'used-by-storage'}, 'lvm', 'LVM consumer attributed');
    is($lun->{'multipath-storage'}, 'iscsi', 'reported storage requests the map');
    is($res->{luns}->[1]->{'multipath-storage'}, 'iscsi', 'base storage requests the map');
    is($lun->{nodes}->{n1}->{'paths-expected'}, 2, 'expected path count of the node');
    my %lun = map { $_->{wwid} => $_ } $res->{luns}->@*;
    ok(
        !exists($lun{'3600c'}->{'multipath-storage'}),
        'a base without multipath requests nothing',
    );
    ok(!exists($lun{'3600d'}->{'multipath-storage'}), 'neither does a reported one');
    ok(
        !exists($lun{'3600e'}->{'multipath-storage'}),
        'a base for no current node requests nothing',
    );
    ok(!exists($lun{'3600f'}->{'multipath-storage'}), 'a storage requests on its nodes only');
    is(
        $lun{'3600d'}->{nodes}->{n2}->{state},
        'missing',
        'the nodes of a shared LVM expect the map',
    );
    is($lun{'3600d'}->{'used-by-storage'}, 'shared', 'shared LVM attributed by volume group');
    {
        local $raw{n2} = encode_report({ time => $NOW, luns => [], 'vgs-unknown' => 1 });
        my %unhinted = map { $_->{wwid} => $_ } PVE::API2::Multipath->status({})->{luns}->@*;
        is(
            $unhinted{'3600d'}->{nodes}->{n2}->{state},
            'unknown',
            'a node without LVM hints may have left out the map of a volume group',
        );
        is($unhinted{'3600a'}->{nodes}->{n2}->{state}, 'missing', 'but for one with an entry');
        is(
            $unhinted{'3600f'}->{nodes}->{n2}->{state},
            'missing',
            'and for one that an iSCSI storage requests there',
        );
        local $entries->{'3600d'} = { type => 'wwid', nodes => { n1 => 1 } };
        %unhinted = map { $_->{wwid} => $_ } PVE::API2::Multipath->status({})->{luns}->@*;
        is(
            $unhinted{'3600d'}->{nodes}->{n2}->{state},
            'unknown',
            'an entry for other nodes does not change that',
        );
    }
    eval {
        PVE::JSONSchema::validate(
            $res,
            PVE::API2::Multipath->map_method_by_name('status')->{returns},
        );
    };
    is($@, '', 'result satisfies the API schema');
    is(
        PVE::Multipath::Status::worst_state(qw(unknown optimal failed degraded missing)),
        'failed',
        'failed ranks above missing, degraded, optimal and unknown',
    );
};

subtest 'every storage that requests a map counts' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    my $storage = Test::MockModule->new('PVE::Storage');
    $cluster->mock(get_nodelist => sub { [qw(n1 n2 n3)] });
    $cluster->mock(get_members => sub { {} });
    my $lun = {
        wwid => '3600x',
        state => 'optimal',
        'paths-total' => 2,
        'paths-active' => 2,
        storages => [qw(s1 s2)],
    };
    my %raw = map { $_ => encode_report({ time => $NOW, luns => [$lun] }) } qw(n1 n2);
    $raw{n3} = encode_report({ time => $NOW, luns => [] });
    $cluster->mock(get_node_kv => sub { \%raw });
    $config->mock(read_config => sub { { ids => {} } });
    $storage->mock(
        config => sub { {
            ids => {
                s1 =>
                    { type => 'iscsi', 'managed-multipath' => 1, nodes => { n1 => 1, n2 => 1 } },
                s2 => {
                    type => 'iscsi',
                    'managed-multipath' => 1,
                    nodes => { n3 => 1 },
                    content => { images => 1 },
                },
            },
        } },
    );
    my ($row) = PVE::Multipath::Status::cluster_status()->{luns}->@*;
    is($row->{nodes}->{n3}->{state}, 'missing', 'a node of the second storage expects the map');
    is($row->{'used-by-storage'}, 's2', 'which serves its LUNs');
};

subtest 'a base volume by its SCSI name is a map if its storage requests maps' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    my $storage = Test::MockModule->new('PVE::Storage');
    $cluster->mock(get_nodelist => sub { [qw(n1 n2)] });
    $cluster->mock(get_members => sub { {} });
    # n1 sees the LUN, but has no map for it
    my $missing = { wwid => '3600a', state => 'missing', storages => ['managed'] };
    my $observed = { wwid => '3600b', state => 'optimal' };
    my %raw = (
        n1 => encode_report({ time => $NOW, luns => [$missing, $observed] }),
        n2 => encode_report({ time => $NOW, luns => [] }),
    );
    $cluster->mock(get_node_kv => sub { \%raw });
    $config->mock(read_config => sub { { ids => {} } });
    $storage->mock(
        config => sub { {
            ids => {
                managed => { type => 'iscsi', 'managed-multipath' => 1 },
                plain => { type => 'iscsi' },
                mapped => { type => 'lvm', base => 'managed:0.0.1.scsi-3600a', shared => 1 },
                single => { type => 'lvm', base => 'plain:0.0.1.scsi-3600b' },
                # the iSCSI storage lists a LUN by another of its by-id names in some setups
                alias => {
                    type => 'lvm',
                    base => 'managed:0.0.2.scsi-SLIO-ORG_block1_abc',
                    shared => 1,
                },
            },
        } },
    );
    my %lun = map { $_->{wwid} => $_ } PVE::Multipath::Status::cluster_status()->{luns}->@*;
    is_deeply(
        $lun{'3600a'},
        {
            wwid => '3600a',
            explicit => 0,
            'used-by-storage' => 'mapped',
            'multipath-storage' => 'managed',
            state => 'missing',
            nodes => { n1 => { state => 'missing' }, n2 => { state => 'missing' } },
        },
        'the map that the storage requests is expected on its nodes',
    );
    is_deeply(
        $lun{'3600b'},
        {
            wwid => '3600b',
            explicit => 0,
            state => 'optimal',
            nodes => { n1 => { state => 'optimal' } },
        },
        'the plain base gives an observed map neither an owner nor an expected second node',
    );
    ok(
        !$lun{'SLIO-ORG_block1_abc'},
        'a SCSI name in a base volume that is not a reported WWID is no LUN of its own',
    );
};

subtest 'a disabled storage expects no map' => sub {
    my $cluster = Test::MockModule->new('PVE::Cluster');
    my $config = Test::MockModule->new('PVE::Multipath::Config');
    my $storage = Test::MockModule->new('PVE::Storage');
    $cluster->mock(get_nodelist => sub { [qw(n1 n2)] });
    $cluster->mock(get_members => sub { {} });
    my %raw = map { $_ => encode_report({ time => $NOW, luns => [] }) } qw(n1 n2);
    $cluster->mock(get_node_kv => sub { \%raw });
    $config->mock(read_config => sub { { ids => {} } });
    $storage->mock(
        config => sub { {
            ids => {
                conn => { type => 'iscsi', 'managed-multipath' => 1, disable => 1 },
                current => { type => 'lvm', vgname => 'current', shared => 1 },
                san => {
                    type => 'lvm',
                    base => 'conn:0.0.1.dm-uuid-mpath-3600a',
                    shared => 1,
                    disable => 1,
                },
            },
        } },
    );
    is_deeply(PVE::Multipath::Status::cluster_status()->{luns}, [], 'its LUN is not missing');
    $raw{n1} =
        encode_report({ time => $NOW, luns => [{ wwid => '3600a', state => 'optimal' }] });
    is_deeply(
        PVE::Multipath::Status::cluster_status()->{luns},
        [{
            wwid => '3600a',
            explicit => 0,
            'used-by-storage' => 'san',
            state => 'optimal',
            nodes => { n1 => { state => 'optimal' } },
        }],
        'a map that a node reports is still used by it',
    );
    $raw{n2} = encode_report({
        time => $NOW,
        luns => [{ wwid => '3600a', state => 'optimal', vgname => 'current' }],
    });
    is(
        PVE::Multipath::Status::cluster_status()->{luns}->[0]->{'used-by-storage'},
        'current',
        'unless an enabled storage uses it',
    );
};

done_testing();
