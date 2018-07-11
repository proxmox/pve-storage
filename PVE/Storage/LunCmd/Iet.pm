package PVE::Storage::LunCmd::Iet;

# iscsi storage running Debian
# 1) apt-get install iscsitarget iscsitarget-dkms
# 2) Create target like (/etc/iet/ietd.conf):
# Target iqn.2001-04.com.example:tank
#   Alias           tank
# 3) Activate daemon (/etc/default/iscsitarget)
# ISCSITARGET_ENABLE=true
# 4) service iscsitarget start
#
# On one of the proxmox nodes:
# 1) Login as root
# 2) ssh-copy-id <ip_of_iscsi_storage>

use strict;
use warnings;

use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);

sub get_base;

# A logical unit can max have 16864 LUNs
# http://manpages.ubuntu.com/manpages/precise/man5/ietd.conf.5.html
my $MAX_LUNS = 16864;

my $CONFIG_FILE = '/etc/iet/ietd.conf';
my $DAEMON = '/usr/sbin/ietadm';
my $SETTINGS = undef;
my $CONFIG = undef;
my $OLD_CONFIG = undef;

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my @scp_cmd = ('/usr/bin/scp', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';
my $ietadm = '/usr/sbin/ietadm';

my $execute_command = sub {
    my ($scfg, $exec, $timeout, $method, @params) = @_;

    my $msg = '';
    my $err = undef;
    my $target;
    my $cmd;
    my $res = ();

    $timeout = 10 if !$timeout;

    my $output = sub {
    my $line = shift;
    $msg .= "$line\n";
    };

    my $errfunc = sub {
    my $line = shift;
    $err .= "$line";
    };

    if ($exec eq 'scp') {
        $target = 'root@[' . $scfg->{portal} . ']';
        $cmd = [@scp_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", '--', $method, "$target:$params[0]"];
    } else {
        $target = 'root@' . $scfg->{portal};
        $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, '--', $method, @params];
    }

    eval {
        run_command($cmd, outfunc => $output, errfunc => $errfunc, timeout => $timeout);
    };
    if ($@) {
        $res = {
            result => 0,
            msg => $err,
        }
    } else {
        $res = {
            result => 1,
            msg => $msg,
        }
    }

    return $res;
};

my $read_config = sub {
    my ($scfg, $timeout) = @_;

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

    my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $luncmd, $CONFIG_FILE];
    eval {
        run_command($cmd, outfunc => $output, errfunc => $errfunc, timeout => $timeout);
    };
    if ($@) {
        die $err if ($err !~ /No such file or directory/);
        die "No configuration found. Install iet on $scfg->{portal}" if $msg eq '';
    }

    return $msg;
};

my $get_config = sub {
    my ($scfg) = @_;
    my @conf = undef;

    my $config = $read_config->($scfg, undef);
    die "Missing config file" unless $config;

    $OLD_CONFIG = $config;

    return $config;
};

my $parser = sub {
    my ($scfg) = @_;

    my $line = 0;

    my $base = get_base;
    my $config = $get_config->($scfg);
    my @cfgfile = split "\n", $config;

    my $cfg_target = 0;
    foreach (@cfgfile) {
        $line++;
        if ($_ =~ /^\s*Target\s*([\w\-\:\.]+)\s*$/) {
            if ($1 eq $scfg->{target} && ! $cfg_target) {
                # start colect info
                die "$line: Parse error [$_]" if $SETTINGS;
                $SETTINGS->{target} = $1;
                $cfg_target = 1;
            } elsif ($1 eq $scfg->{target} && $cfg_target) {
                die "$line: Parse error [$_]";
            } elsif ($cfg_target) {
                $cfg_target = 0;
                $CONFIG .= "$_\n";
            } else {
                $CONFIG .= "$_\n";
            }
        } else {
            if ($cfg_target) {
                $SETTINGS->{text} .= "$_\n";
                next if ($_ =~ /^\s*#/ || ! $_);
                my $option = $_;
                if ($_ =~ /^(\w+)\s*#/) {
                    $option = $1;
                }
                if ($option =~ /^\s*(\w+)\s+(\w+)\s*$/) {
                    if ($1 eq 'Lun') {
                        die "$line: Parse error [$_]";
                    }
                    $SETTINGS->{$1} = $2;
                } elsif ($option =~ /^\s*(\w+)\s+(\d+)\s+([\w\-\/=,]+)\s*$/) {
                    die "$line: Parse error [$option]" unless ($1 eq 'Lun');
                    my $conf = undef;
                    my $num = $2;
                    my @lun = split ',', $3;
                    die "$line: Parse error [$option]" unless (scalar(@lun) > 1);
                    foreach (@lun) {
                        my @lun_opt = split '=', $_;
                        die "$line: Parse error [$option]" unless (scalar(@lun_opt) == 2);
                        $conf->{$lun_opt[0]} = $lun_opt[1];
                    }
                    if ($conf->{Path} && $conf->{Path} =~ /^$base\/$scfg->{pool}\/([\w\-]+)$/) {
                        $conf->{include} = 1;
                    } else {
                        $conf->{include} = 0;
                    }
                    $conf->{lun} = $num;
                    push @{$SETTINGS->{luns}}, $conf;
                } else {
                    die "$line: Parse error [$option]";
                }
            } else {
                $CONFIG .= "$_\n";
            }
        }
    }
    $CONFIG =~ s/^\s+|\s+$|"\s*//g;
};

my $update_config = sub {
    my ($scfg) = @_;
    my $file = "/tmp/config$$";
    my $config = '';

    while ((my $option, my $value) = each(%$SETTINGS)) {
        next if ($option eq 'include' || $option eq 'luns' || $option eq 'Path' || $option eq 'text' || $option eq 'used');
        if ($option eq 'target') {
            $config = "\n\nTarget " . $SETTINGS->{target} . "\n" . $config;
        } else {
            $config .= "\t$option\t\t\t$value\n";
        }
    }
    foreach my $lun (@{$SETTINGS->{luns}}) {
        my $lun_opt = '';
        while ((my $option, my $value) = each(%$lun)) {
            next if ($option eq 'include' || $option eq 'lun' || $option eq 'Path');
            if ($lun_opt eq '') {
            $lun_opt = $option . '=' . $value;
            } else {
                $lun_opt .= ',' . $option . '=' . $value;
            }
        }
        $config .= "\tLun $lun->{lun} Path=$lun->{Path},$lun_opt\n";
    }
    open(my $fh, '>', $file) or die "Could not open file '$file' $!";

    print $fh $CONFIG;
    print $fh $config;
    close $fh;

    my @params = ($CONFIG_FILE);
    my $res = $execute_command->($scfg, 'scp', undef, $file, @params);
    unlink $file;

    die $res->{msg} unless $res->{result};
};

my $get_target_tid = sub {
    my ($scfg) = @_;
    my $proc = '/proc/net/iet/volume';
    my $tid = undef;

    my @params = ($proc);
    my $res = $execute_command->($scfg, 'ssh', undef, 'cat', @params);
    die $res->{msg} unless $res->{result};
    my @cfg = split "\n", $res->{msg};

    foreach (@cfg) {
        if ($_ =~ /^\s*tid:(\d+)\s+name:([\w\-\:\.]+)\s*$/) {
            if ($2 && $2 eq $scfg->{target}) {
                $tid = $1;
                last;
            }
        }
    }

    return $tid;
};

my $get_lu_name = sub {
    my $used = ();
    my $i;

    if (! exists $SETTINGS->{used}) {
        for ($i = 0; $i < $MAX_LUNS; $i++) {
            $used->{$i} = 0;
        }
        foreach my $lun (@{$SETTINGS->{luns}}) {
            $used->{$lun->{lun}} = 1;
        }
        $SETTINGS->{used} = $used;
    }

    $used = $SETTINGS->{used};
    for ($i = 0; $i < $MAX_LUNS; $i++) {
        last unless $used->{$i};
    }
    $SETTINGS->{used}->{$i} = 1;

    return $i;
};

my $init_lu_name = sub {
    my $used = ();

    if (! exists($SETTINGS->{used})) {
        for (my $i = 0; $i < $MAX_LUNS; $i++) {
            $used->{$i} = 0;
        }
        $SETTINGS->{used} = $used;
    }
    foreach my $lun (@{$SETTINGS->{luns}}) {
        $SETTINGS->{used}->{$lun->{lun}} = 1;
    }
};

my $free_lu_name = sub {
    my ($lu_name) = @_;
    my $new;

    foreach my $lun (@{$SETTINGS->{luns}}) {
        if ($lun->{lun} != $lu_name) {
            push @$new, $lun;
        }
    }

    $SETTINGS->{luns} = $new;
    $SETTINGS->{used}->{$lu_name} = 0;
};

my $make_lun = sub {
    my ($scfg, $path) = @_;

    die 'Maximum number of LUNs per target is 16384' if scalar @{$SETTINGS->{luns}} >= $MAX_LUNS;

    my $lun = $get_lu_name->();
    my $conf = {
        lun => $lun,
        Path => $path,
        Type => 'blockio',
        include => 1,
    };
    push @{$SETTINGS->{luns}}, $conf;

    return $conf;
};

my $list_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $lun = undef;

    my $object = $params[0];
    foreach my $lun (@{$SETTINGS->{luns}}) {
        next unless $lun->{include} == 1;
        if ($lun->{Path} =~ /^$object$/) {
            return $lun->{lun} if (defined($lun->{lun}));
            die "$lun->{Path}: Missing LUN";
        }
    }

    return $lun;
};

my $list_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $name = undef;

    my $object = $params[0];
    foreach my $lun (@{$SETTINGS->{luns}}) {
        next unless $lun->{include} == 1;
        if ($lun->{Path} =~ /^$object$/) {
            return $lun->{Path};
        }
    }

    return $name;
};

my $create_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    if ($list_lun->($scfg, $timeout, $method, @params)) {
        die "$params[0]: LUN exists";
    }
    my $lun = $params[0];
    $lun = $make_lun->($scfg, $lun);
    my $tid = $get_target_tid->($scfg);
    $update_config->($scfg);

    my $path = "Path=$lun->{Path},Type=$lun->{Type}";

    @params = ('--op', 'new', "--tid=$tid", "--lun=$lun->{lun}", '--params', $path);
    my $res = $execute_command->($scfg, 'ssh', $timeout, $ietadm, @params);
    do {
        $free_lu_name->($lun->{lun});
        $update_config->($scfg);
        die $res->{msg};
    } unless $res->{result};

    return $res->{msg};
};

my $delete_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $res = {msg => undef};

    my $path = $params[0];
    my $tid = $get_target_tid->($scfg);

    foreach my $lun (@{$SETTINGS->{luns}}) {
        if ($lun->{Path} eq $path) {
            @params = ('--op', 'delete', "--tid=$tid", "--lun=$lun->{lun}");
            $res = $execute_command->($scfg, 'ssh', $timeout, $ietadm, @params);
            if ($res->{result}) {
                $free_lu_name->($lun->{lun});
                $update_config->($scfg);
                last;
            } else {
                die $res->{msg};
            }
        }
    }

    return $res->{msg};
};

my $import_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    return $create_lun->($scfg, $timeout, $method, @params);
};

my $modify_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $lun;
    my $res;

    my $path = $params[1];
    my $tid = $get_target_tid->($scfg);

    foreach my $cfg (@{$SETTINGS->{luns}}) {
        if ($cfg->{Path} eq $path) {
            $lun = $cfg;
            last;
        }
    }

    @params = ('--op', 'delete', "--tid=$tid", "--lun=$lun->{lun}");
    $res = $execute_command->($scfg, 'ssh', $timeout, $ietadm, @params);
    die $res->{msg} unless $res->{result};

    $path = "Path=$lun->{Path},Type=$lun->{Type}";
    @params = ('--op', 'new', "--tid=$tid", "--lun=$lun->{lun}", '--params', $path);
    $res = $execute_command->($scfg, 'ssh', $timeout, $ietadm, @params);
    die $res->{msg} unless $res->{result};

    return $res->{msg};
};

my $add_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    return '';
};

my $get_lun_cmd_map = sub {
    my ($method) = @_;

    my $cmdmap = {
        create_lu   =>  { cmd => $create_lun },
        delete_lu   =>  { cmd => $delete_lun },
        import_lu   =>  { cmd => $import_lun },
        modify_lu   =>  { cmd => $modify_lun },
        add_view    =>  { cmd => $add_view },
        list_view   =>  { cmd => $list_view },
        list_lu     =>  { cmd => $list_lun },
    };

    die "unknown command '$method'" unless exists $cmdmap->{$method};

    return $cmdmap->{$method};
};

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    $parser->($scfg) unless $SETTINGS;
    my $cmdmap = $get_lun_cmd_map->($method);
    my $msg = $cmdmap->{cmd}->($scfg, $timeout, $method, @params);

    return $msg;
}

sub get_base {
    return '/dev';
}

1;

