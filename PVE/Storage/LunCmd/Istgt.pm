package PVE::Storage::LunCmd::Istgt;

# TODO
# Create initial target and LUN if target is missing ?
# Create and use list of free LUNs

use strict;
use warnings;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use Data::Dumper;

my @CONFIG_FILES = (
    '/usr/local/etc/istgt/istgt.conf',  # FreeBSD, FreeNAS
    '/var/etc/iscsi/istgt.conf'         # NAS4Free
);
my @DAEMONS = (
    '/usr/local/etc/rc.d/istgt',        # FreeBSD, FreeNAS
    '/var/etc/rc.d/istgt'               # NAS4Free
);

# A logical unit can max have 63 LUNs
# https://code.google.com/p/istgt/source/browse/src/istgt_lu.h#39
my $MAX_LUNS = 64;

my $CONFIG_FILE = undef;
my $DAEMON = undef;
my $SETTINGS = undef;
my $CONFIG = undef;
my $OLD_CONFIG = undef;

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my @scp_cmd = ('/usr/bin/scp', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';

#Current SIGHUP reload limitations (http://www.peach.ne.jp/archives/istgt/):
#
#    The parameters other than PG, IG, and LU are not reloaded by SIGHUP.
#    LU connected by the initiator can't be reloaded by SIGHUP.
#    PG and IG mapped to LU can't be deleted by SIGHUP.
#    If you delete an active LU, all connections of the LU are closed by SIGHUP.
#    Updating IG is not affected until the next login.
#
# FreeBSD
# 1. Alt-F2 to change to native shell (zfsguru)
# 2. pw mod user root -w yes (change password for root to root)
# 3. vi /etc/ssh/sshd_config
# 4. uncomment PermitRootLogin yes
# 5. change PasswordAuthentication no to PasswordAuthentication yes
# 5. /etc/rc.d/sshd restart
# 6. On one of the proxmox nodes login as root and run: ssh-copy-id ip_freebsd_host
# 7. vi /etc/ssh/sshd_config
# 8. comment PermitRootLogin yes
# 9. change PasswordAuthentication yes to PasswordAuthentication no
# 10. /etc/rc.d/sshd restart
# 11. Reset passwd -> pw mod user root -w no
# 12. Alt-Ctrl-F1 to return to zfsguru shell (zfsguru)

sub get_base;
sub run_lun_command;

my $read_config = sub {
    my ($scfg, $timeout, $method) = @_;

    my $msg = '';
    my $err = undef;
    my $luncmd = 'cat';
    my $target;
    $timeout = 10 if !$timeout;

    my $output = sub {
    my $line = shift;
    $msg .= "$line\n";
    };

    my $errfunc = sub {
    my $line = shift;
    $err .= "$line";
    };

    $target = 'root@' . $scfg->{portal};

    my $daemon = 0;
    foreach my $config (@CONFIG_FILES) {
        $err = undef;
        my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $luncmd, $config];
        eval {
            run_command($cmd, outfunc => $output, errfunc => $errfunc, timeout => $timeout);
        };
        do {
            $err = undef;
            $DAEMON = $DAEMONS[$daemon];
            $CONFIG_FILE = $config;
            last;
        } unless $@;
        $daemon++;
    }
    die $err if ($err && $err !~ /No such file or directory/);
    die "No configuration found. Install istgt on $scfg->{portal}" if $msg eq '';

    return $msg;
};

my $get_config = sub {
    my ($scfg) = @_;
    my @conf = undef;

    my $config = $read_config->($scfg, undef, 'get_config');
    die "Missing config file" unless $config;

    $OLD_CONFIG = $config;

    return $config;
};

my $parse_size = sub {
    my ($text) = @_;

    return 0 if !$text;

    if ($text =~ m/^(\d+(\.\d+)?)([TGMK]B)?$/) {
    my ($size, $reminder, $unit) = ($1, $2, $3);
    return $size if !$unit;
    if ($unit eq 'KB') {
        $size *= 1024;
    } elsif ($unit eq 'MB') {
        $size *= 1024*1024;
    } elsif ($unit eq 'GB') {
        $size *= 1024*1024*1024;
    } elsif ($unit eq 'TB') {
        $size *= 1024*1024*1024*1024;
    }
        if ($reminder) {
            $size = ceil($size);
        }
        return $size;
    } elsif ($text =~ /^auto$/i) {
        return 'AUTO';
    } else {
        return 0;
    }
};

my $size_with_unit = sub {
    my ($size, $n) = (shift, 0);

    return '0KB' if !$size;

    return $size if $size eq 'AUTO';

    if ($size =~ m/^\d+$/) {
        ++$n and $size /= 1024 until $size < 1024;
        if ($size =~ /\./) {
            return sprintf "%.2f%s", $size, ( qw[bytes KB MB GB TB] )[ $n ];
        } else {
            return sprintf "%d%s", $size, ( qw[bytes KB MB GB TB] )[ $n ];
        }
    }
    die "$size: Not a number";
};

my $lun_dumper = sub {
    my ($lun) = @_;
    my $config = '';

    $config .= "\n[$lun]\n";
    $config .=  'TargetName ' . $SETTINGS->{$lun}->{TargetName} . "\n";
    $config .=  'Mapping ' . $SETTINGS->{$lun}->{Mapping} . "\n";
    $config .=  'AuthGroup ' . $SETTINGS->{$lun}->{AuthGroup} . "\n";
    $config .=  'UnitType ' . $SETTINGS->{$lun}->{UnitType} . "\n";
    $config .=  'QueueDepth ' . $SETTINGS->{$lun}->{QueueDepth} . "\n";

    foreach my $conf (@{$SETTINGS->{$lun}->{luns}}) {
        $config .=  "$conf->{lun} Storage " . $conf->{Storage};
        $config .= ' ' . $size_with_unit->($conf->{Size}) . "\n";
        foreach ($conf->{options}) {
            if ($_) {
                $config .=  "$conf->{lun} Option " . $_ . "\n";
            }
        }
    }
    $config .= "\n";

    return $config;
};

my $get_lu_name = sub {
    my ($target) = @_;
    my $used = ();
    my $i;

    if (! exists $SETTINGS->{$target}->{used}) {
        for ($i = 0; $i < $MAX_LUNS; $i++) {
            $used->{$i} = 0;
        }
        foreach my $lun (@{$SETTINGS->{$target}->{luns}}) {
            $lun->{lun} =~ /^LUN(\d+)$/;
            $used->{$1} = 1;
        }
        $SETTINGS->{$target}->{used} = $used;
    }

    $used = $SETTINGS->{$target}->{used};
    for ($i = 0; $i < $MAX_LUNS; $i++) {
        last unless $used->{$i};
    }
    $SETTINGS->{$target}->{used}->{$i} = 1;

    return "LUN$i";
};

my $init_lu_name = sub {
    my ($target) = @_;
    my $used = ();

    if (! exists($SETTINGS->{$target}->{used})) {
        for (my $i = 0; $i < $MAX_LUNS; $i++) {
            $used->{$i} = 0;
        }
        $SETTINGS->{$target}->{used} = $used;
    }
    foreach my $lun (@{$SETTINGS->{$target}->{luns}}) {
        $lun->{lun} =~ /^LUN(\d+)$/;
        $SETTINGS->{$target}->{used}->{$1} = 1;
    }
};

my $free_lu_name = sub {
    my ($target, $lu_name) = @_;

    $lu_name =~ /^LUN(\d+)$/;
    $SETTINGS->{$target}->{used}->{$1} = 0;
};

my $make_lun = sub {
    my ($scfg, $path) = @_;

    my $target = $SETTINGS->{current};
    die 'Maximum number of LUNs per target is 63' if scalar @{$SETTINGS->{$target}->{luns}} >= $MAX_LUNS;

    my @options = ();
    my $lun = $get_lu_name->($target);
    if ($scfg->{nowritecache}) {
        push @options, "WriteCache Disable";
    }
    my $conf = {
        lun => $lun,
        Storage => $path,
        Size => 'AUTO',
        options => @options,
    };
    push @{$SETTINGS->{$target}->{luns}}, $conf;

    return $conf->{lun};
};

my $parser = sub {
    my ($scfg) = @_;

    my $lun = undef;
    my $line = 0;

    my $config = $get_config->($scfg);
    my @cfgfile = split "\n", $config;

    foreach (@cfgfile) {
        $line++;
        if ($_ =~ /^\s*\[(PortalGroup\d+)\]\s*/) {
            $lun = undef;
            $SETTINGS->{$1} = ();
        } elsif ($_ =~ /^\s*\[(InitiatorGroup\d+)\]\s*/) {
            $lun = undef;
            $SETTINGS->{$1} = ();
        } elsif ($_ =~ /^\s*PidFile\s+"?([\w\/\.]+)"?\s*/) {
            $lun = undef;
            $SETTINGS->{pidfile} = $1;
        } elsif ($_ =~ /^\s*NodeBase\s+"?([\w\-\.]+)"?\s*/) {
            $lun = undef;
            $SETTINGS->{nodebase} = $1;
        } elsif ($_ =~ /^\s*\[(LogicalUnit\d+)\]\s*/) {
            $lun = $1;
            $SETTINGS->{$lun} = ();
            $SETTINGS->{targets}++;
        } elsif ($lun) {
            next if (($_ =~ /^\s*#/) || ($_ =~ /^\s*$/));
            if ($_ =~ /^\s*(\w+)\s+(.+)\s*/) {
                my $arg1 = $1;
                my $arg2 = $2;
                $arg2 =~ s/^\s+|\s+$|"\s*//g;
                if ($arg2 =~ /^Storage\s*(.+)/i) {
                    $SETTINGS->{$lun}->{$arg1}->{storage} = $1;
                } elsif ($arg2 =~ /^Option\s*(.+)/i) {
                    push @{$SETTINGS->{$lun}->{$arg1}->{options}}, $1;
                } else {
                    $SETTINGS->{$lun}->{$arg1} = $arg2;
                }
            } else {
                die "$line: parse error [$_]";
            }
        }
        $CONFIG .= "$_\n" unless $lun;
    }

    $CONFIG =~ s/\n$//;
    die "$scfg->{target}: Target not found" unless $SETTINGS->{targets};
    my $max = $SETTINGS->{targets};
    my $base = get_base;

    for (my $i = 1; $i <= $max; $i++) {
        my $target = $SETTINGS->{nodebase}.':'.$SETTINGS->{"LogicalUnit$i"}->{TargetName};
        if ($target eq $scfg->{target}) {
            my $lu = ();
            while ((my $key, my $val) = each(%{$SETTINGS->{"LogicalUnit$i"}})) {
                if ($key =~ /^LUN\d+/) {
                    $val->{storage} =~ /^([\w\/\-]+)\s+(\w+)/;
                    my $storage = $1;
                    my $size = $parse_size->($2);
                    my $conf = undef;
                    my @options = ();
                    if ($val->{options}) {
                        @options = @{$val->{options}};
                    }
                    if ($storage =~ /^$base\/$scfg->{pool}\/([\w\-]+)$/) {
                        $conf = {
                            lun => $key,
                            Storage => $storage,
                            Size => $size,
                            options => @options,
                        }
                    }
                    push @$lu, $conf if $conf;
                    delete $SETTINGS->{"LogicalUnit$i"}->{$key};
                }
            }
            $SETTINGS->{"LogicalUnit$i"}->{luns} = $lu;
            $SETTINGS->{current} = "LogicalUnit$i";
            $init_lu_name->("LogicalUnit$i");
        } else {
            $CONFIG .= $lun_dumper->("LogicalUnit$i");
            delete $SETTINGS->{"LogicalUnit$i"};
            $SETTINGS->{targets}--;
        }
    }
    die "$scfg->{target}: Target not found" unless $SETTINGS->{targets} > 0;
};

my $list_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $name = undef;

    my $object = $params[0];
    for my $key (keys %$SETTINGS)  {
        next unless $key =~ /^LogicalUnit\d+$/;
        foreach my $lun (@{$SETTINGS->{$key}->{luns}}) {
            if ($lun->{Storage} =~ /^$object$/) {
                return $lun->{Storage};
            }
        }
    }

    return $name;
};

my $create_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $res = ();
    my $file = "/tmp/config$$";

    if ($list_lun->($scfg, $timeout, $method, @params)) {
        die "$params[0]: LUN exists";
    }
    my $lun = $params[0];
    $lun = $make_lun->($scfg, $lun);
    my $config = $lun_dumper->($SETTINGS->{current});
    open(my $fh, '>', $file) or die "Could not open file '$file' $!";

    print $fh $CONFIG;
    print $fh $config;
    close $fh;
    @params = ($CONFIG_FILE);
    $res = {
        cmd => 'scp',
        method => $file,
        params => \@params,
        msg => $lun,
        post_exe => sub {
            unlink $file;
        },
    };

    return $res;
};

my $delete_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $res = ();
    my $file = "/tmp/config$$";

    my $target = $SETTINGS->{current};
    my $luns = ();

    foreach my $conf (@{$SETTINGS->{$target}->{luns}}) {
        if ($conf->{Storage} =~ /^$params[0]$/) {
            $free_lu_name->($target, $conf->{lun});
        } else {
            push @$luns, $conf;
        }
    }
    $SETTINGS->{$target}->{luns} = $luns;

    my $config = $lun_dumper->($SETTINGS->{current});
    open(my $fh, '>', $file) or die "Could not open file '$file' $!";

    print $fh $CONFIG;
    print $fh $config;
    close $fh;
    @params = ($CONFIG_FILE);
    $res = {
        cmd => 'scp',
        method => $file,
        params => \@params,
        post_exe => sub {
            unlink $file;
            run_lun_command($scfg, undef, 'add_view', 'restart');
        },
    };

    return $res;
};

my $import_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    my $res = $create_lun->($scfg, $timeout, $method, @params);

    return $res;
};

my $add_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $cmdmap;

    if (@params && $params[0] eq 'restart') {
        @params = ('onerestart', '>&', '/dev/null');
        $cmdmap = {
            cmd => 'ssh',
            method => $DAEMON,
            params => \@params,
        };
    } else {
        @params = ('-HUP', '`cat '. "$SETTINGS->{pidfile}`");
        $cmdmap = {
            cmd => 'ssh',
            method => 'kill',
            params => \@params,
        };
    }

    return $cmdmap;
};

my $modify_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    # Current SIGHUP reload limitations
    # LU connected by the initiator can't be reloaded by SIGHUP.
    # Until above limitation persists modifying a LUN will require
    # a restart of the daemon breaking all current connections
    #die 'Modify a connected LUN is not currently supported by istgt';
    @params = ('restart', @params);

    return $add_view->($scfg, $timeout, $method, @params);
};

my $list_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $lun = undef;

    my $object = $params[0];
    for my $key (keys %$SETTINGS)  {
        next unless $key =~ /^LogicalUnit\d+$/;
        foreach my $lun (@{$SETTINGS->{$key}->{luns}}) {
            if ($lun->{Storage} =~ /^$object$/) {
                if ($lun->{lun} =~ /^LUN(\d+)/) {
                    return $1;
                }
                die "$lun->{Storage}: Missing LUN";
            }
        }
    }

    return $lun;
};

my $get_lun_cmd_map = sub {
    my ($method) = @_;

    my $cmdmap = {
        create_lu   => { cmd => $create_lun },
        delete_lu   => { cmd => $delete_lun },
        import_lu   => { cmd => $import_lun },
        modify_lu   => { cmd => $modify_lun },
        add_view    => { cmd => $add_view },
        list_view   => { cmd => $list_view },
        list_lu     => { cmd => $list_lun },
    };

    die "unknown command '$method'" unless exists $cmdmap->{$method};

    return $cmdmap->{$method};
};

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    my $msg = '';
    my $luncmd;
    my $target;
    my $cmd;
    my $res;
    $timeout = 10 if !$timeout;
    my $is_add_view = 0;

    my $output = sub {
    my $line = shift;
    $msg .= "$line\n";
    };

    $target = 'root@' . $scfg->{portal};

    $parser->($scfg) unless $SETTINGS;
    my $cmdmap = $get_lun_cmd_map->($method);
    if ($method eq 'add_view') {
        $is_add_view = 1 ;
        $timeout = 15;
    }
    if (ref $cmdmap->{cmd} eq 'CODE') {
        $res = $cmdmap->{cmd}->($scfg, $timeout, $method, @params);
        if (ref $res) {
            $method = $res->{method};
            @params = @{$res->{params}};
            if ($res->{cmd} eq 'scp') {
                $cmd = [@scp_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $method, "$target:$params[0]"];
            } else {
                $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $method, @params];
            }
        } else {
            return $res;
        }
    } else {
        $luncmd = $cmdmap->{cmd};
        $method = $cmdmap->{method};
        $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $luncmd, $method, @params];
    }

    eval {
        run_command($cmd, outfunc => $output, timeout => $timeout);
    };
    if ($@ && $is_add_view) {
        my $err = $@;
        if ($OLD_CONFIG) {
            my $err1 = undef;
            my $file = "/tmp/config$$";
            open(my $fh, '>', $file) or die "Could not open file '$file' $!";
            print $fh $OLD_CONFIG;
            close $fh;
            $cmd = [@scp_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $file, $CONFIG_FILE];
            eval {
                run_command($cmd, outfunc => $output, timeout => $timeout);
            };
            $err1 = $@ if $@;
            unlink $file;
            die "$err\n$err1" if $err1;
            eval {
                run_lun_command($scfg, undef, 'add_view', 'restart');
            };
            die "$err\n$@" if ($@);
        }
        die $err;
    } elsif ($@) {
        die $@;
    } elsif ($is_add_view) {
        $OLD_CONFIG = undef;
    }

    if ($res->{post_exe} && ref $res->{post_exe} eq 'CODE') {
        $res->{post_exe}->();
    }

    if ($res->{msg}) {
        $msg = $res->{msg};
    }

    return $msg;
}

sub get_base {
    return '/dev/zvol';
}

1;
