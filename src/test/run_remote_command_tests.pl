#!/usr/bin/perl

use strict;
use warnings;

use lib ('.', '..');

use File::Temp;
use JSON;
use Test::MockModule;
use Test::More;

use PVE::API2::Storage::Status;
use PVE::CLI::pvesm;
use PVE::JSONSchema;
use PVE::Storage;
use PVE::Storage::ZFSPlugin;

my $remote_ip = '192.0.2.1';

# Has characters a shell would interpret, while still being valid in names on directory storages.
# Getting interpreted only runs echo, whose output then shows up in the reported arguments.
my $special_chars = q{$(echo${IFS}a);echo${IFS}b;`echo${IFS}c`'d"e};

# OpenSSH joins the arguments after the destination with spaces and the login shell of the remote
# user evaluates the result. Do the same with a local shell that reports the resulting arguments.
my sub remote_argv {
    my ($cmd) = @_;

    my ($terminator) = grep { $cmd->[$_] eq '--' } 0 .. $cmd->$#*;
    die "missing option terminator in '@$cmd'\n" if !defined($terminator);

    my @remote_args = $cmd->@[$terminator + 1 .. $cmd->$#*];
    shift @remote_args if $remote_args[0] =~ m/^(root\@)?\Q$remote_ip\E$/;

    my $script = q{report() { printf '%s\0' "$@"; }; report } . join(' ', @remote_args);
    open(my $fh, '-|', '/bin/sh', '-c', $script) or die "unable to run shell - $!\n";
    my $output = do { local $/; <$fh> };
    close($fh);

    $output =~ s/\0\z//;
    return [split(/\0/, $output, -1)];
}

my sub parse_pvesm_args {
    my ($subcommand, @args) = @_;

    my ($class, $method, $arg_param, $fixed_param) = $PVE::CLI::pvesm::cmddef->{$subcommand}->@*;
    my $schema = $class->map_method_by_name($method)->{parameters};

    return PVE::JSONSchema::get_options($schema, [@args], $arg_param, $fixed_param);
}

# Records the commands of a module instead of running them, $respond can produce their output.
my sub mock_run_command {
    my ($module, $commands, $respond) = @_;

    my $mock = Test::MockModule->new($module);
    $mock->mock(
        run_command => sub {
            my ($cmd, %param) = @_;
            push $commands->@*, $cmd;
            $respond->($cmd, \%param) if $respond;
            return 0;
        },
    );

    return $mock;
}

subtest 'storage migrate' => sub {
    my $volname = "100/vm-100-disk-$special_chars.raw";
    my $migration_snapshot = 0;
    my @commands;

    my $mock = mock_run_command(
        'PVE::Storage',
        \@commands,
        sub {
            my ($cmd, $param) = @_;
            my $log = $param->{logfunc} // $param->{errfunc};
            $log->("successfully imported 'sb:100/vm-100-disk-1.raw'");
        },
    );
    $mock->mock(
        activate_volumes => sub { },
        storage_migrate_snapshot => sub { return $migration_snapshot; },
        volume_snapshot => sub { },
        volume_snapshot_delete => sub { },
        volume_transfer_formats => sub { return ('raw+size'); },
    );

    my $cfg = {
        ids => {
            sa => { type => 'dir', path => '/mnt/sa', content => { images => 1 } },
            sb => { type => 'dir', path => '/mnt/sb', content => { images => 1 } },
        },
    };
    my $ssh_info = { ip => $remote_ip, name => 'othernode' };

    my $tests = [
        {
            name => 'with snapshots and renaming allowed',
            ssh_info => $ssh_info,
            opts => {
                with_snapshots => 1,
                snapshot => '__replicate_new__',
                base_snapshot => '__replicate_old__',
                allow_rename => 1,
            },
            export => {
                'with-snapshots' => 1,
                snapshot => '__replicate_new__',
                base => '__replicate_old__',
            },
            import => {
                'with-snapshots' => 1,
                snapshot => '__replicate_new__',
                base => '__replicate_old__',
                'allow-rename' => 1,
            },
        },
        {
            name => 'with migration snapshot',
            ssh_info => $ssh_info,
            migration_snapshot => 1,
            opts => {},
            export => { 'with-snapshots' => 0, snapshot => '__migration__' },
            import => {
                'with-snapshots' => 0,
                snapshot => '__migration__',
                'delete-snapshot' => '__migration__',
                'allow-rename' => 0,
            },
        },
        {
            name => 'to the local node',
            opts => {},
            export => { 'with-snapshots' => 0 },
            import => { 'with-snapshots' => 0, 'allow-rename' => 0 },
        },
        {
            name => 'insecure',
            ssh_info => $ssh_info,
            opts => { insecure => 1 },
            filename => "tcp://$remote_ip",
            import => { 'with-snapshots' => 0, 'allow-rename' => 0 },
        },
    ];

    for my $test ($tests->@*) {
        my $name = $test->{name};
        @commands = ();
        $migration_snapshot = $test->{migration_snapshot} // 0;

        my $new_volid = PVE::Storage::storage_migrate(
            $cfg, "sa:$volname", $test->{ssh_info}, 'sb', $test->{opts}, sub { },
        );
        is($new_volid, 'sb:100/vm-100-disk-1.raw', "$name: volume ID from the import output");

        # the insecure migration only starts sending after it got told where to connect to
        my $is_pipeline = ref($commands[0]->[0]) eq 'ARRAY';
        my $recv = $is_pipeline ? $commands[0]->[-1] : $commands[0];

        if ($test->{export}) {
            my ($program, $subcommand, @args) = $commands[0]->[0]->@*;
            is("$program $subcommand", 'pvesm export', "$name: export command");
            is($args[-4], '--', "$name: export arguments are separated from options");
            is_deeply(
                parse_pvesm_args('export', @args),
                {
                    volume => "sa:$volname",
                    format => 'raw+size',
                    filename => '-',
                    $test->{export}->%*,
                },
                "$name: export parameters",
            );
        }

        my $argv = $test->{ssh_info} ? remote_argv($recv) : $recv;
        my ($program, $subcommand, @args) = $argv->@*;
        is("$program $subcommand", 'pvesm import', "$name: import command");
        is($args[-4], '--', "$name: import arguments are separated from options");
        is_deeply(
            parse_pvesm_args('import', @args),
            {
                volume => "sb:$volname",
                format => 'raw+size',
                filename => $test->{filename} // '-',
                $test->{import}->%*,
            },
            "$name: import parameters",
        );
    }
};

subtest 'upload' => sub {
    my $path = "/mnt/with space$special_chars";
    my $dir = "$path/template/iso";
    my $dest = "$dir/test.iso";
    my $tmpdir = File::Temp->newdir();
    my $tmpfile = "$tmpdir/upload";

    my $cfg = {
        ids => { store => { type => 'dir', path => $path, content => { iso => 1 } } },
    };

    my $fail_copy = 0;
    my @commands;

    my $mock = mock_run_command(
        'PVE::API2::Storage::Status',
        \@commands,
        sub {
            my ($cmd) = @_;
            die "import failed: copy\n" if $fail_copy && $cmd->[0] eq '/usr/bin/scp';
        },
    );
    my $cluster_mock = Test::MockModule->new('PVE::Cluster');
    $cluster_mock->mock(remote_node_ip => sub { return $remote_ip; });
    my $inotify_mock = Test::MockModule->new('PVE::INotify');
    $inotify_mock->mock(nodename => sub { return 'localnode'; });
    my $storage_mock = Test::MockModule->new('PVE::Storage');
    $storage_mock->mock(
        assert_iso_content => sub { },
        config => sub { return $cfg; },
    );
    my $rpcenv_mock = Test::MockModule->new('PVE::RPCEnvironment');
    $rpcenv_mock->mock(
        get => sub { return bless({}, 'PVE::RPCEnvironment'); },
        get_user => sub { return 'root@pam'; },
        fork_worker => sub {
            my ($self, $type, $id, $user, $worker) = @_;
            # keep what the worker logs out of the test output
            local *STDOUT;
            open(STDOUT, '>', '/dev/null') or die "unable to open /dev/null - $!\n";
            $worker->('UPID');
            return 'UPID';
        },
    );

    # calling the implementation directly to not have to use the fixed temporary file location
    my $upload = PVE::API2::Storage::Status->map_method_by_name('upload')->{code};
    my $param = {
        node => 'othernode',
        storage => 'store',
        content => 'iso',
        filename => 'test.iso',
        tmpfilename => $tmpfile,
    };

    my sub run_upload {
        @commands = ();
        PVE::Tools::file_set_contents($tmpfile, 'content');
        return $upload->({ $param->%* });
    }

    run_upload();
    is(scalar(@commands), 3, 'expected number of commands');
    is_deeply(
        remote_argv($commands[0]),
        ['/usr/sbin/pvesm', 'status', '--storage', 'store'],
        'activates the storage',
    );
    is_deeply(
        remote_argv($commands[1]),
        ['/bin/mkdir', '-p', '--', $dir],
        'creates the target directory',
    );
    is_deeply(
        [$commands[2]->[0], $commands[2]->@[-4 .. -1]],
        ['/usr/bin/scp', '-p', '--', $tmpfile, "[$remote_ip]:$dest"],
        'scp gets the unmodified target path as a single argument',
    );

    $fail_copy = 1;
    eval { run_upload() };
    is($@, "import failed: copy\n", 'failure to copy is reported');
    is_deeply(remote_argv($commands[-1]), ['rm', '-f', '--', $dest], 'cleans up after failure');
};

subtest 'ZFS over iSCSI' => sub {
    my @commands;
    my $mock = mock_run_command('PVE::Storage::ZFSPlugin', \@commands);
    $mock->mock(
        zfs_add_lun_mapping_entry => sub { },
        zfs_create_lu => sub { return 'guid'; },
    );

    my $scfg = { pool => 'tank', portal => $remote_ip, iscsiprovider => 'comstar' };

    for my $name ('vm-100-a b', 'vm-100-a:b', 'vm-100-a;b', 'vm-100-$(echo a)', "vm-100-a\n") {
        (my $shown_name = $name) =~ s/\n/\\n/;
        @commands = ();
        eval { PVE::Storage::ZFSPlugin->alloc_image('san', $scfg, 100, 'raw', $name, 1024) };
        is($@, "unable to parse zfs volume name '$name'\n", "allocating '$shown_name' fails");
        is(scalar(@commands), 0, "no command for '$shown_name'");
    }

    @commands = ();
    my $volname =
        PVE::Storage::ZFSPlugin->alloc_image('san', $scfg, 100, 'raw', 'vm-100-disk-0', 1024);
    is($volname, 'vm-100-disk-0', 'allocated volume name');
    is_deeply(
        remote_argv($commands[0]),
        ['zfs', 'create', '-V', '1024k', 'tank/vm-100-disk-0'],
        'zfs create',
    );

    @commands = ();
    PVE::Storage::ZFSPlugin->zfs_request($scfg, undef, 'zpool_list', '-o', 'name', '-Hp');
    is_deeply(remote_argv($commands[0]), ['zpool', 'list', '-o', 'name', '-Hp'], 'zpool list');

    @commands = ();
    my $snapshot = "snap$special_chars";
    PVE::Storage::ZFSPlugin->volume_snapshot_delete($scfg, 'san', 'vm-100-disk-0', $snapshot);
    is_deeply(
        remote_argv($commands[0]),
        ['zfs', 'destroy', "tank/vm-100-disk-0\@$snapshot"],
        'snapshot name stays part of a single argument',
    );
};

subtest 'LUN provider: Comstar' => sub {
    my $object = '/dev/zvol/rdsk/tank/vm-100-disk-0';
    my @commands;
    my $mock = mock_run_command(
        'PVE::Storage::LunCmd::Comstar',
        \@commands,
        sub {
            my ($cmd, $param) = @_;
            $param->{outfunc}->("600144f0000 1024 $object");
        },
    );

    my $scfg = { portal => $remote_ip, comstar_tg => 'tg', comstar_hg => 'hg' };
    my sub run_lun_command {
        @commands = ();
        my $res = PVE::Storage::LunCmd::Comstar::run_lun_command($scfg, undef, @_);
        return ($res, remote_argv($commands[0]));
    }

    # the object is only used to filter the output, an empty argument would break the command
    my ($guid, $argv) = run_lun_command('list_lu', $object);
    is($guid, '600144f0000', 'list_lu: result');
    is_deeply($argv, ['/usr/sbin/sbdadm', 'list-lu'], 'list_lu: command');

    (undef, $argv) = run_lun_command('add_view', "guid$special_chars");
    is_deeply(
        $argv,
        [
            '/usr/sbin/stmfadm',
            'add-view',
            '--host-group',
            'hg',
            '--target-group',
            'tg',
            "guid$special_chars",
        ],
        'add_view: command',
    );

    (undef, $argv) = run_lun_command('modify_lu', '1G', "guid$special_chars");
    is_deeply(
        $argv,
        ['/usr/sbin/stmfadm', 'modify-lu', '-s', '1G', "guid$special_chars"],
        'modify_lu: command',
    );
};

subtest 'LUN provider: LIO' => sub {
    my $target = "iqn.test$special_chars";
    my $config = encode_json({
        targets => [{
            fabric => 'iscsi',
            wwn => $target,
            tpgs => [{ tag => 1, luns => [] }],
        }],
    });
    my @commands;
    my $mock = mock_run_command(
        'PVE::Storage::LunCmd::LIO',
        \@commands,
        sub {
            my ($cmd, $param) = @_;
            my $line = join(' ', $cmd->@*);
            $param->{outfunc}->($config) if $line =~ m/ cat /;
            $param->{outfunc}->('Created LUN 5.') if $line =~ m/ create /;
        },
    );

    my $scfg = {
        portal => $remote_ip,
        target => $target,
        pool => 'tank',
        lio_tpg => 'tpg1',
    };
    PVE::Storage::LunCmd::LIO::run_lun_command(
        $scfg, undef, 'create_lu', '/dev/tank/vm-100-disk-0',
    );

    my $targetcli = '/usr/bin/targetcli';
    my $backstore = '/backstores/block';
    is_deeply(
        [map { remote_argv($_) } @commands],
        [
            ['cat', '/etc/rtslib-fb-target/saveconfig.json'],
            [
                $targetcli,
                $backstore,
                'create',
                'name=tank-vm-100-disk-0',
                'dev=/dev/tank/vm-100-disk-0',
            ],
            [$targetcli, "$backstore/tank-vm-100-disk-0", 'set', 'attribute', 'emulate_tpu=1'],
            [
                $targetcli,
                "/iscsi/$target/tpg1/luns/",
                'create',
                "$backstore/tank-vm-100-disk-0",
            ],
            [$targetcli, 'saveconfig'],
        ],
        'create_lu: commands',
    );
};

subtest 'LUN provider: Iet' => sub {
    my $config = join(
        "\n",
        '# configuration of the target',
        'Target iqn.test',
        "\tLun 0 Path=/dev/tank/vm-100-disk-0,Type=blockio",
    );
    my @commands;
    my $mock = mock_run_command(
        'PVE::Storage::LunCmd::Iet',
        \@commands,
        sub {
            my ($cmd, $param) = @_;
            my $line = join(' ', $cmd->@*);
            $param->{outfunc}->($config) if $line =~ m/ cat \/etc\/iet\/ietd\.conf/;
            $param->{outfunc}->('tid:1 name:iqn.test')
                if $line =~ m/ cat \/proc\/net\/iet\/volume/;
        },
    );

    # the path also ends up in regular expressions, so it cannot have all of the special characters
    my $path = '/dev/tank/vm-100-disk-1;echo a';
    my $scfg = { portal => $remote_ip, target => 'iqn.test', pool => 'tank' };
    PVE::Storage::LunCmd::Iet::run_lun_command($scfg, undef, 'create_lu', $path);

    my @scp = grep { $_->[0] eq '/usr/bin/scp' } @commands;
    is(scalar(@scp), 1, 'create_lu: copies the configuration once');
    is($scp[0]->[-1], "root\@[$remote_ip]:/etc/iet/ietd.conf", 'create_lu: copy target');

    is_deeply(
        [map { remote_argv($_) } grep { $_->[0] eq '/usr/bin/ssh' } @commands],
        [
            ['cat', '/etc/iet/ietd.conf'],
            ['cat', '/proc/net/iet/volume'],
            [
                '/usr/sbin/ietadm',
                '--op',
                'new',
                '--tid=1',
                '--lun=1',
                '--params',
                "Path=$path,Type=blockio",
            ],
        ],
        'create_lu: commands',
    );
};

subtest 'LUN provider: Istgt' => sub {
    my $config = join(
        "\n",
        '[Global]',
        '  NodeBase "iqn.test"',
        '  PidFile /var/run/istgt.pid',
        '[LogicalUnit1]',
        '  TargetName disk',
        '  LUN0 Storage /dev/zvol/tank/vm-100-disk-0 1024MB',
        '  LUN0 Option WriteCache Disable',
    );
    my @commands;
    my $mock = mock_run_command(
        'PVE::Storage::LunCmd::Istgt',
        \@commands,
        sub {
            my ($cmd, $param) = @_;
            $param->{outfunc}->($config) if join(' ', $cmd->@*) =~ m/ cat /;
        },
    );

    my $scfg = { portal => $remote_ip, target => 'iqn.test:disk', pool => 'tank' };
    PVE::Storage::LunCmd::Istgt::run_lun_command($scfg, undef, 'add_view');
    is_deeply(
        remote_argv($commands[0]),
        ['cat', '/usr/local/etc/istgt/istgt.conf'],
        'reads the configuration',
    );

    # These are the only commands that rely on the remote shell, so do not evaluate them locally.
    my sub remote_line {
        my ($cmd) = @_;
        my ($terminator) = grep { $cmd->[$_] eq '--' } 0 .. $cmd->$#*;
        return join(' ', $cmd->@[$terminator + 2 .. $cmd->$#*]);
    }

    is(remote_line($commands[1]), 'kill -HUP `cat /var/run/istgt.pid`', 'add_view: reload');

    @commands = ();
    PVE::Storage::LunCmd::Istgt::run_lun_command($scfg, undef, 'modify_lu', '1G', 'object');
    is(
        remote_line($commands[0]),
        '/usr/local/etc/rc.d/istgt onerestart >& /dev/null',
        'modify_lu: restart',
    );
};

done_testing();
