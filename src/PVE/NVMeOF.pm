package PVE::NVMeOF;

use v5.36;

use Cwd qw(abs_path);
use Digest::SHA qw(sha1_hex);
use Fcntl qw(:flock F_GETFD F_SETFD FD_CLOEXEC);
use File::Basename qw(basename);
use File::Path qw(make_path);
use POSIX qw(ceil);
use Socket qw(AF_INET AF_INET6 inet_ntop inet_pton);
use Time::HiRes ();

use PVE::JSONSchema;
use PVE::Tools qw(dir_glob_foreach file_read_firstline file_set_contents run_command);

use PVE::Diskmanage;

our $NVME = '/usr/sbin/nvme';
our $SYSFS = '/sys';
our $HOSTNQN_FILE = '/etc/nvme/hostnqn';
our $HOSTID_FILE = '/etc/nvme/hostid';
our $RUN_DIR = '/run/pve/nvmeof';
our $LOCK_WAIT = 10;
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

sub namespaces($subsystem) {
    my $res = {};
    my $dir = "$SYSFS/class/nvme-subsystem";
    dir_glob_foreach(
        $dir,
        'nvme-subsys[0-9]+',
        sub($subsys, @) {
            # one that cannot be read could be this subsystem, whose namespaces must not look gone
            my $nqn = PVE::Diskmanage::read_subsysnqn("$dir/$subsys/subsysnqn")
                // die "cannot read the NQN of NVMe subsystem '$subsys'\n";
            return if $nqn ne $subsystem;
            dir_glob_foreach(
                "$dir/$subsys",
                'nvme[0-9]+n[0-9]+',
                sub($dev, @) {
                    # a failed read must not look like a namespace that is gone
                    my $wwid = file_read_firstline("$SYSFS/block/$dev/wwid")
                        // die "cannot read the WWID of namespace '$dev'\n";
                    my $size = file_read_firstline("$SYSFS/block/$dev/size")
                        // die "cannot read the size of namespace '$dev'\n";
                    $res->{$dev} = { wwid => $wwid, size => int($size) * 512 };
                },
            );
        },
    );
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

1;
