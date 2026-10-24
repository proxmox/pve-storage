package PVE::NVMeOF;

use v5.36;

use Cwd qw(abs_path);
use Digest::SHA qw(sha1_hex);
use Errno qw(EBUSY ENOENT);
use Fcntl qw(:flock F_GETFD F_SETFD FD_CLOEXEC O_EXCL O_RDONLY);
use File::Basename qw(basename);
use File::Path qw(make_path);
use JSON qw(decode_json);
use POSIX qw(ceil);
use Socket qw(AF_INET AF_INET6 inet_ntop inet_pton);
use Time::HiRes ();

use PVE::Cluster;
use PVE::JSONSchema;
use PVE::Network;
use PVE::Tools qw(dir_glob_foreach file_read_firstline file_set_contents run_command);

use PVE::Diskmanage;

our $NVME = '/usr/sbin/nvme';
our $SYSFS = '/sys';
our $HOSTNQN_FILE = '/etc/nvme/hostnqn';
our $HOSTID_FILE = '/etc/nvme/hostid';
our $RUN_DIR = '/run/pve/nvmeof';
our $LOCK_WAIT = 10;
our $SWAPS = '/proc/swaps';
# no control characters, which could reach logs and the arguments of nvme-cli
our $NQN_PATTERN = 'nqn\.[^\s\x00-\x1f\x7f,=]+';
our $DISCOVERY_NQN = 'nqn.2014-08.org.nvmexpress.discovery';
our $TIMED_OUT = "NVMe over Fabrics operation timed out - it may still be running\n";

sub canonical_address($addr) {
    if (defined(my $packed = inet_pton(AF_INET, $addr))) {
        return inet_ntop(AF_INET, $packed);
    }
    if (defined(my $packed = inet_pton(AF_INET6, $addr))) {
        return inet_ntop(AF_INET, substr($packed, 12))
            if substr($packed, 0, 12) eq ("\0" x 10) . "\xff\xff";
        return inet_ntop(AF_INET6, $packed);
    }
    return lc($addr);
}

# Link-local IPv6 needs a node-specific interface and cannot identify a shared configured portal.
sub parse_portal($portal) {
    my ($addr, $port);
    if ($portal =~ m/^\[([0-9a-fA-F:.]+)\](?::([0-9]+))?$/) {
        ($addr, $port) = ($1, $2);
    } elsif ($portal =~ m/^([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(?::([0-9]+))?$/) {
        ($addr, $port) = ($1, $2);
    } else {
        return undef;
    }
    return undef if defined($port) && ($port < 1 || $port > 65535);
    $addr = canonical_address($addr);
    if (defined(my $packed = inet_pton(AF_INET, $addr))) {
        my $multicast = (unpack('C', $packed) & 0xf0) == 0xe0;
        return undef if $addr eq '0.0.0.0' || $addr eq '255.255.255.255' || $multicast;
    } elsif (defined($packed = inet_pton(AF_INET6, $addr))) {
        my $prefix = unpack('n', $packed);
        return undef
            if $addr eq '::' || ($prefix & 0xffc0) == 0xfe80 || ($prefix & 0xff00) == 0xff00;
    } else {
        return undef;
    }
    return { traddr => $addr, trsvcid => int($port // 4420) };
}

PVE::JSONSchema::register_format(
    'pve-storage-nvme-portal',
    sub($portal, $noerr = undef) {
        return $portal if parse_portal($portal);
        return undef if $noerr;
        die "value is not an IPv4 address or a bracketed IPv6 address with optional port\n";
    },
);

sub format_path($path) {
    # an IPv6 address, not the address of a Fibre Channel port, which has a colon too
    my $ipv6 = $path->{traddr} =~ /:/ && $path->{traddr} =~ /^[0-9a-fA-F:.]+$/;
    my $addr = $ipv6 ? "[$path->{traddr}]" : $path->{traddr};
    return defined($path->{trsvcid}) ? "$addr:$path->{trsvcid}" : $addr;
}

sub storage_paths($scfg) {
    return [map { parse_portal($_) // die "invalid portal '$_'\n" }
        PVE::Tools::split_list($scfg->{portals})];
}

sub host_identity() {
    my ($nqn) = (file_read_firstline($HOSTNQN_FILE) // '') =~ /\A($NQN_PATTERN)\z/;
    my ($id) = (file_read_firstline($HOSTID_FILE) // '') =~
        /\A([0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12})\z/i;
    if (!defined($nqn) || length($nqn) > 223) {
        die "invalid or missing NVMe host NQN in '$HOSTNQN_FILE' - install nvme-cli and check the"
            . " host identity\n";
    }
    die "invalid or missing NVMe host ID in '$HOSTID_FILE' - expected a nonzero UUID\n"
        if !defined($id) || $id =~ /^[0-]+$/;
    return { nqn => $nqn, id => lc($id) };
}

sub is_supported() { return -x $NVME ? 1 : 0 }

sub assert_support() {
    die "nvme-cli is not installed\n" if !is_supported();
    run_command(['modprobe', 'nvme-tcp'], timeout => 10)
        if !-e "$SYSFS/module/nvme_tcp";
    my $multipath = file_read_firstline("$SYSFS/module/nvme_core/parameters/multipath") // '';
    die "native NVMe multipath is disabled - enable nvme_core.multipath\n" if $multipath ne 'Y';
}

sub controllers() {
    my $res = {};
    my $dir = "$SYSFS/class/nvme-fabrics/ctl";
    dir_glob_foreach(
        $dir,
        'nvme[0-9]+',
        sub($name, @) {
            my $address = file_read_firstline("$dir/$name/address") // return;
            my %address = map { split(/=/, $_, 2) } grep { /=/ } split(/,/, $address);
            return if !defined($address{traddr});
            $res->{$name} = {
                transport => file_read_firstline("$dir/$name/transport") // '',
                subsystem => PVE::Diskmanage::read_subsysnqn("$dir/$name/subsysnqn") // '',
                state => file_read_firstline("$dir/$name/state") // 'unknown',
                traddr => canonical_address($address{traddr}),
                trsvcid => $address{trsvcid},
            };
        },
    );
    return $res;
}

sub subsystem_controllers($subsystem) {
    my $controllers = controllers();
    return {
        map { $_ => $controllers->{$_} } grep {
            $controllers->{$_}->{subsystem} eq $subsystem
                && $controllers->{$_}->{transport} eq 'tcp'
        } keys %$controllers
    };
}

# The NQN of a subsystem, untainted, or undef if it is invalid or the one of the discovery service.
sub subsystem_nqn($nqn) {
    my ($res) = ($nqn // '') =~ /\A($NQN_PATTERN)\z/;
    return undef if !defined($res) || length($res) > 223 || $res eq $DISCOVERY_NQN;
    return $res;
}

sub namespace_wwid($dev) {
    return file_read_firstline("$SYSFS/block/$dev/wwid");
}

# Native NVMe multipath shows a namespace as a device of its subsystem. If the target does not
# declare that the subsystem may have several controllers, a namespace is a device of each
# controller instead, without the other paths.
sub is_namespace_head($dev) {
    my $parent = abs_path("$SYSFS/block/$dev/device") // return 0;
    return basename($parent) =~ /^nvme-subsys[0-9]+$/ ? 1 : 0;
}

sub has_live_controller($controllers) {
    return scalar(grep { $_->{state} eq 'live' } values %$controllers);
}

sub path_controller($controllers, $path) {
    for my $name (sort keys %$controllers) {
        my $ctrl = $controllers->{$name};
        if ($ctrl->{traddr} eq $path->{traddr} && ($ctrl->{trsvcid} // '') eq $path->{trsvcid}) {
            return $name;
        }
    }
    return undef;
}

# Calls $code with the subsystem NQN and the device name of each namespace of this node, or only
# of those of the subsystem $only.
my sub foreach_namespace($code, $only = undef) {
    my $dir = "$SYSFS/class/nvme-subsystem";
    # For one subsystem, the caller goes by which namespaces are missing, so a directory that cannot
    # be read must not look empty. One that is gone is.
    my $entries = sub($path, $pattern) {
        my $dh;
        if (!opendir($dh, $path)) {
            return () if $! == ENOENT || !defined($only);
            die "cannot read '$path' - $!\n";
        }
        my @res = sort grep { /\A$pattern\z/ } readdir($dh);
        closedir($dh);
        return @res;
    };
    for my $subsys ($entries->($dir, 'nvme-subsys[0-9]+')) {
        # one that cannot be read could be the subsystem asked for, whose namespaces must not
        # look gone
        my $nqn = PVE::Diskmanage::read_subsysnqn("$dir/$subsys/subsysnqn");
        if (!defined($nqn)) {
            die "cannot read the NQN of NVMe subsystem '$subsys'\n" if defined($only);
            next;
        }
        next if defined($only) && $nqn ne $only;
        $code->($nqn, $_) for $entries->("$dir/$subsys", 'nvme[0-9]+n[0-9]+');
    }
}

sub namespaces($subsystem) {
    my $res = {};
    foreach_namespace(
        sub($nqn, $dev) {
            # a failed read must not look like a namespace that is gone
            my $wwid = file_read_firstline("$SYSFS/block/$dev/wwid")
                // die "cannot read the WWID of namespace '$dev'\n";
            my $size = file_read_firstline("$SYSFS/block/$dev/size")
                // die "cannot read the size of namespace '$dev'\n";
            $res->{$dev} = { wwid => $wwid, size => int($size) * 512 };
        },
        $subsystem,
    );
    return $res;
}

# The number of namespaces of each subsystem of this node, from one pass over the subsystems.
sub namespace_counts() {
    my $res = {};
    foreach_namespace(sub($nqn, @) { $res->{$nqn}++ });
    return $res;
}

# $limit sets the time the operation may take in all, including the wait for the lock.
sub with_lock($code, $limit = undef) {
    my $total = $LOCK_WAIT + 35;
    # an alarm counts whole seconds and none at zero
    $total = ceil($limit) if defined($limit);
    $total = 1 if $total < 1;
    my ($res, $timeout) = PVE::Tools::run_fork_with_timeout(
        $total,
        sub {
            make_path($RUN_DIR);
            open(my $lock, '>>', "$RUN_DIR/fabrics.lock") or die "cannot open NVMe lock - $!\n";
            # A busy lock fails the operation, so wait a while for it, but give up before the time
            # limit, which counts as a hanging operation.
            my $wait = $LOCK_WAIT < $total ? $LOCK_WAIT : $total - 1;
            my $deadline = Time::HiRes::time() + $wait;
            while (!flock($lock, LOCK_EX | LOCK_NB)) {
                die "NVMe over Fabrics is busy - another operation is still running\n"
                    if Time::HiRes::time() >= $deadline;
                Time::HiRes::usleep(100_000);
            }
            # Commands inherit the lock, so that it stays held until they exit, even if the caller
            # timed out while one was stuck in the kernel.
            my $flags = fcntl($lock, F_GETFD, 0) // die "cannot read NVMe lock flags - $!\n";
            fcntl($lock, F_SETFD, $flags & ~FD_CLOEXEC)
                or die "cannot inherit NVMe lock - $!\n";
            return $code->();
        },
    );
    die $TIMED_OUT if $timeout;
    return $res;
}

my sub retry_file($scfg, $path) {
    my ($key) =
        sha1_hex(join(
            "\0", $scfg->{subsysnqn}, format_path($path), $scfg->{'fast-io-fail-tmo'} // 120)) =~
        /\A([0-9a-f]{40})\z/;
    return "$RUN_DIR/retry/$key";
}

sub in_cooldown($scfg, $path) {
    my $mtime = (stat(retry_file($scfg, $path)))[9] // return 0;
    my $age = time() - $mtime;
    return $age > -60 && $age < 60;
}

sub note_connect_failure($scfg, $path) {
    make_path("$RUN_DIR/retry");
    file_set_contents(retry_file($scfg, $path), '');
}

sub clear_connect_failures($scfg) {
    unlink(retry_file($scfg, $_)) for storage_paths($scfg)->@*;
}

sub connect_path($scfg, $path, $limit = undef) {
    my $nqn = subsystem_nqn($scfg->{subsysnqn}) // die "invalid subsystem NQN\n";
    my ($fail) = ($scfg->{'fast-io-fail-tmo'} // 120) =~ /\A(-?[0-9]+)\z/;
    die "invalid fast I/O fail timeout\n" if !defined($fail) || $fail < -1;
    # A connect that hangs until the time limit failed too, but the process that would note it is
    # gone by then.
    my $res = eval {
        with_lock(
            sub {
                return if path_controller(subsystem_controllers($scfg->{subsysnqn}), $path);
                assert_support();
                my $host = host_identity();
                # only a failed attempt is a failure of the portal, not a busy lock
                eval {
                    run_command(
                        [
                            $NVME,
                            'connect',
                            '--transport=tcp',
                            '--nqn',
                            $nqn,
                            '--traddr',
                            $path->{traddr},
                            '--trsvcid',
                            $path->{trsvcid},
                            '--hostnqn',
                            $host->{nqn},
                            '--hostid',
                            $host->{id},
                            '--ctrl-loss-tmo=-1',
                            '--fast_io_fail_tmo',
                            $fail,
                        ],
                        timeout => 30,
                        errmsg => 'nvme connect',
                    );
                    # nvme-cli can exit successfully without having connected
                    die "no controller appeared\n"
                        if !path_controller(subsystem_controllers($scfg->{subsysnqn}), $path);
                };
                if (my $err = $@) {
                    note_connect_failure($scfg, $path);
                    die $err;
                }
                unlink(retry_file($scfg, $path));
                return;
            },
            $limit,
        );
    };
    if (my $err = $@) {
        note_connect_failure($scfg, $path) if $err eq $TIMED_OUT;
        die $err;
    }
    return $res;
}

sub parse_discovery_log($raw) {
    my $log = decode_json($raw);
    die "invalid NVMe discovery log\n"
        if ref($log) ne 'HASH' || ref($log->{records}) ne 'ARRAY';
    my (%seen, %res);
    for my $record ($log->{records}->@*) {
        next
            if ($record->{subtype} // '') ne 'nvme subsystem' || ($record->{trtype} // '') ne 'tcp';
        # the log pads the address fields with spaces, which not every nvme-cli removes
        my ($nqn_raw, $addr, $port) =
            map { my $v = $_ // ''; $v =~ s/^\s+|\s+$//g; $v }
            $record->@{qw(subnqn traddr trsvcid)};
        my $nqn = subsystem_nqn($nqn_raw) // next;
        my $path = parse_portal(($addr =~ /:/ ? "[$addr]" : $addr) . ":$port") // next;
        next if $seen{$nqn}->{ format_path($path) }++;
        push $res{$nqn}->@*, $path;
    }
    return \%res;
}

sub discover($portal) {
    my $path = parse_portal($portal) // die "invalid portal '$portal'\n";
    my @ports = $portal =~ /:[0-9]+$/ ? ($path->{trsvcid}) : (8009, 4420);
    # a missing prerequisite fails on every port, so report it as it is
    assert_support();
    my $host = host_identity();
    my $error;
    for my $port (@ports) {
        # keep the error of a port that answered rather than "no answer" from the other port
        if (!PVE::Network::tcp_ping($path->{traddr}, $port, 2)) {
            $error //= "no answer from portal\n";
            next;
        }
        my $result = eval {
            with_lock(sub {
                my $out = '';
                run_command(
                    [
                        $NVME,
                        'discover',
                        '--transport=tcp',
                        '--traddr',
                        $path->{traddr},
                        '--trsvcid',
                        $port,
                        '--hostnqn',
                        $host->{nqn},
                        '--hostid',
                        $host->{id},
                        '--output-format=json',
                    ],
                    timeout => 30,
                    errmsg => 'nvme discover',
                    outfunc => sub($line) { $out .= "$line\n" },
                );
                return parse_discovery_log($out);
            });
        };
        return $result if !$@;
        $error = $@;
    }
    die "discovery at portal '$portal' failed - $error";
}

# A swap area on a namespace has no holder, mount or file descriptor of a process.
my sub is_swap($device) {
    my %rdevs = map {
        my $rdev = (stat($_))[6];
        defined($rdev) ? ($rdev => 1) : ()
    } ($device, glob("${device}p[0-9]*"));
    for my $line (split(/\n/, PVE::Tools::file_get_contents($SWAPS))) {
        my ($swap) = split(/\s+/, $line);
        next if !defined($swap) || $swap !~ m!^/!;
        return 1 if $swap =~ m!^\Q$device\E(?:p[0-9]+)?$!;
        my $rdev = (stat($swap))[6];
        return 1 if defined($rdev) && $rdevs{$rdev};
    }
    return 0;
}

# A mount in another mount namespace, like that of a container, is not in /proc/mounts here and has
# no holder, but it claims the device exclusively, as holders and swap do.
sub is_claimed($device) {
    sysopen(my $fh, $device, O_RDONLY | O_EXCL) and return 0;
    return $! == EBUSY;
}

# The expected subsystem and portal refuse a controller that replaced the one the caller listed.
sub disconnect_guarded($controller, $expected = {}) {
    my ($name) = $controller =~ /\A(nvme[0-9]+)\z/;
    die "invalid controller name '$controller'\n" if !defined($name);
    # A namespace that would lose its last usable path gets checked for use, which takes a while on
    # a subsystem with many. A task runs this, which holds no lock on the storage configuration.
    return with_lock(
        sub {
            require PVE::Storage;
            require PVE::Storage::SAN;
            my $ctrl = controllers()->{$name}
                // die "'$name' is not an NVMe over Fabrics controller\n";
            if (
                (
                    defined($expected->{subsysnqn})
                    && $expected->{subsysnqn} ne $ctrl->{subsystem}
                )
                || (defined($expected->{portal}) && $expected->{portal} ne format_path($ctrl))
            ) {
                die
                    "controller '$name' is no longer connected to the listed subsystem and portal\n";
            }
            # Without native multipath a namespace is a device of its controller and has no head
            # device, which the checks below go by. Its name can carry the number of the subsystem.
            my $own_devices = sub {
                my $found = 0;
                dir_glob_foreach(
                    "$SYSFS/class/nvme-fabrics/ctl/$name",
                    'nvme[0-9]+n[0-9]+',
                    sub { $found = 1 },
                );
                return $found;
            };
            die "cannot disconnect '$name' - its namespaces do not use native NVMe multipath\n"
                if $own_devices->();
            my $namespaces = namespaces($ctrl->{subsystem});
            my $status = PVE::Diskmanage::get_nvme_path_status();
            my %heads = map { $_ => 1 } keys %$namespaces;
            for my $head (keys %$status) {
                $heads{$head} = 1
                    if grep { $_->{device} eq "/dev/$name" } $status->{$head}->{paths}->@*;
            }
            PVE::Cluster::cfs_update(1);
            my $cfg = PVE::Storage::config();
            my (%relied, %referenced);
            for my $head (sort keys %heads) {
                my $paths = $status->{$head}->{paths} // [];
                my @others = grep { $_->{device} ne "/dev/$name" } @$paths;
                if (grep { PVE::Diskmanage::nvme_path_is_usable($_) } @others) {
                    $relied{$head} = 1;
                    next;
                }
                my $ns = $namespaces->{$head};
                if (!defined($ns)) {
                    die
                        "cannot disconnect '$name' - namespace '$head' has no device on this node"
                        . " to check for use\n";
                }
                my $device = "/dev/$head";
                if (
                    PVE::Diskmanage::is_mounted($device)
                    || PVE::Diskmanage::has_holder($device)
                    || is_swap($device)
                    || is_claimed($device)
                ) {
                    die
                        "cannot disconnect '$name' - namespace '$head' is in use and has no other"
                        . " usable path\n";
                }
                # an opener that is gone within a second, like an LVM scan of the status pass, is no
                # use of the namespace
                my $opener;
                for my $try (1 .. 4) {
                    Time::HiRes::usleep(300_000) if $try > 1;
                    $opener = PVE::Diskmanage::device_opener($device, glob("${device}p[0-9]*"));
                    last if !defined($opener);
                }
                if (defined($opener)) {
                    die
                        "cannot disconnect '$name' - process $opener has namespace '$head' open and"
                        . " it has no other usable path\n";
                }
                # The check of the storage references reads the namespace. Removing a path that is not
                # usable either costs no usable path, and a reconnecting one cannot be read.
                next if !grep {
                    $_->{device} eq "/dev/$name" && PVE::Diskmanage::nvme_path_is_usable($_)
                } @$paths;
                eval {
                    PVE::Storage::SAN::assert_lun_unreferenced(
                        $cfg,
                        { devpath => $device, wwid => $ns->{wwid} },
                    );
                };
                die "cannot disconnect '$name' - $@" if $@;
                $referenced{$head} = 1;
            }
            # The checks take a while, in which a path can fail or recover, a namespace can appear
            # and the controller number can pass to a new controller, so look again right before
            # the removal. That narrows what can change unseen, but cannot rule it out.
            my $changed =
                "cannot disconnect '$name' - the paths of the subsystem changed meanwhile\n";
            $status = PVE::Diskmanage::get_nvme_path_status();
            for my $head (sort keys %$status) {
                my @paths = ($status->{$head}->{paths} // [])->@*;
                my ($own) = grep { $_->{device} eq "/dev/$name" } @paths;
                next if !$own;
                next if grep {
                    $_->{device} ne "/dev/$name" && PVE::Diskmanage::nvme_path_is_usable($_)
                } @paths;
                # it loses its last usable path, which the checks above have to have covered
                die $changed
                    if $relied{$head}
                    || !$heads{$head}
                    || (PVE::Diskmanage::nvme_path_is_usable($own) && !$referenced{$head});
            }
            my $now_namespaces = namespaces($ctrl->{subsystem});
            my $wwids = sub($res) {
                join(',', map { "$_=$res->{$_}->{wwid}" } sort keys %$res);
            };
            die $changed if $wwids->($now_namespaces) ne $wwids->($namespaces);
            die $changed if $own_devices->();
            my $now = controllers()->{$name};
            die "cannot disconnect '$name' - the controller changed meanwhile\n"
                if !$now
                || grep { ($now->{$_} // '') ne ($ctrl->{$_} // '') }
                qw(subsystem transport traddr trsvcid);
            die "nvme-cli is not installed\n" if !is_supported();
            run_command([$NVME, 'disconnect', '--device', "/dev/$name"], timeout => 30);
            return;
        },
        300,
    );
}

1;
