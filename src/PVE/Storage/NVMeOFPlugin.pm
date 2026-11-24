package PVE::Storage::NVMeOFPlugin;

use v5.36;

use Cwd ();
use Time::HiRes ();

use PVE::Network;

use PVE::Diskmanage;
use PVE::NVMeOF;
use PVE::Storage::Plugin;

use base qw(PVE::Storage::Plugin);

# Time budget (s) for connecting remaining portals once one is live (3x when none are live yet).
our $ACTIVATION_BUDGET = 15;

sub type { return 'nvmeof' }

sub plugindata {
    return {
        content => [{ images => 1, 'vm-vol' => 1, none => 1 }, { images => 1 }],
        select_existing => 1,
        'isolate-properties' => 1,
        'advanced-properties' => {
            'fast-io-fail-tmo' => 1,
        },
        'hidden-properties' => {
            bwlimit => 1,
        },
        'sensitive-properties' => {},
    };
}

sub properties {
    return {
        subsysnqn => {
            title => "Subsystem",
            description => 'The NQN of the NVMe subsystem.',
            type => 'string',
            pattern => $PVE::NVMeOF::NQN_PATTERN,
            maxLength => 223,
        },
        portals => {
            title => "Portals",
            description =>
                'TCP portals of the NVMe subsystem, as IPv4 or bracketed IPv6 addresses'
                . ' with an optional port (default 4420).',
            type => 'string',
            format => 'pve-storage-nvme-portal-list',
        },
        'fast-io-fail-tmo' => {
            title => "Fail I/O After (s)",
            description => 'Seconds before I/O through a reconnecting path fails. -1 disables'
                . ' this timeout; I/O can remain queued while paths reconnect.',
            type => 'integer',
            minimum => -1,
            default => 120,
        },
    };
}

sub options {
    return {
        subsysnqn => { fixed => 1 },
        portals => {},
        'fast-io-fail-tmo' => { optional => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        content => { optional => 1 },
        bwlimit => { optional => 1 },
    };
}

my sub assert_valid_config($scfg) {
    die "the discovery subsystem cannot be used as storage\n"
        if $scfg->{subsysnqn} eq $PVE::NVMeOF::DISCOVERY_NQN;
    my $paths = PVE::NVMeOF::storage_paths($scfg);
    # the list format also accepts a list of separators only
    die "no portal given\n" if !@$paths;
    my %seen;
    for my $path (@$paths) {
        my $portal = PVE::NVMeOF::format_path($path);
        die "portal '$portal' is listed more than once\n" if $seen{$portal}++;
    }
}

sub on_add_hook($class, $storeid, $scfg, %param) {
    assert_valid_config($scfg);
    # the LUN and controller lists and the node status attribute a subsystem to its one storage
    my $ids = PVE::Storage::config()->{ids};
    for my $sid (sort keys %$ids) {
        next if $sid eq $storeid || $ids->{$sid}->{type} ne 'nvmeof';
        die "storage '$sid' already uses this subsystem\n"
            if $ids->{$sid}->{subsysnqn} eq $scfg->{subsysnqn};
    }
    PVE::NVMeOF::clear_connect_failures($scfg);
    return;
}

sub on_update_hook_full($class, $storeid, $scfg, $update, $delete, $sensitive, @) {
    my $merged = { %$scfg, %$update };
    delete $merged->{$_} for ($delete // [])->@*;
    # without the property, the default of the plugin applies, which serves images
    $merged->{content} //= $class->plugindata()->{content}->[1];
    assert_valid_config($merged);
    if (
        PVE::Storage::san_storage_serves_images($merged)
        && !PVE::Storage::san_storage_serves_images($scfg)
    ) {
        my $ids = PVE::Storage::config()->{ids};
        for my $sid (sort keys %$ids) {
            die "storage '$sid' uses a namespace of this storage as base, so it cannot serve"
                . " images\n"
                if $ids->{$sid}->{type} eq 'lvm'
                && ($ids->{$sid}->{base} // '') =~ /^\Q$storeid\E:/;
        }
    }
    PVE::NVMeOF::clear_connect_failures($merged) if exists($update->{portals});
    return $class->SUPER::on_update_hook_full($storeid, $scfg, $update, $delete, $sensitive);
}

sub parse_volname($class, $volname) {
    if ($volname =~ m!^(nvme-[a-zA-Z0-9_.:+-]+)$! && $volname !~ /-part[0-9]+$/) {
        return ($class->guest_volume_vtype('raw'), $1, undef, undef, undef, undef, 'raw');
    }
    die "unable to parse NVMe over Fabrics volume name '$volname'\n";
}

sub assert_namespace($scfg, $volname, $real) {
    my ($dev) = $real =~ m!^/dev/(nvme[0-9]+n[0-9]+)$!;
    die "volume '$volname' is not an NVMe namespace\n" if !defined($dev);
    my $nqn = PVE::Diskmanage::read_subsysnqn("$PVE::NVMeOF::SYSFS/block/$dev/device/subsysnqn")
        // '';
    die "volume '$volname' is not part of subsystem '$scfg->{subsysnqn}'\n"
        if $nqn ne $scfg->{subsysnqn};
    die "volume '$volname' is the device of a single controller, not of native NVMe multipath\n"
        if !PVE::NVMeOF::is_namespace_head($dev);
    # another by-id name of the subsystem, like model and serial, can name another namespace
    my $wwid = PVE::NVMeOF::namespace_wwid($dev) // '';
    die "volume '$volname' is not named after the WWID of its namespace\n"
        if $volname ne "nvme-$wwid";
}

sub filesystem_path($class, $scfg, $volname, $snapname = undef, @) {
    die "snapshot is not possible on NVMe over Fabrics storage\n" if defined($snapname);
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my $path = "/dev/disk/by-id/$name";
    # not every user of a path activates the volume first, and the base volume of an LVM storage
    # has no device before its storage connects
    my $real = Cwd::realpath($path);
    assert_namespace($scfg, $volname, $real) if defined($real) && -e $real;
    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub create_base { die "can't create base images in NVMe over Fabrics storage\n" }
sub clone_image { die "can't clone images in NVMe over Fabrics storage\n" }
sub alloc_image { die "can't allocate space in NVMe over Fabrics storage\n" }
sub free_image { die "can't free space in NVMe over Fabrics storage\n" }
sub volume_resize { die "volume resize is not possible on NVMe over Fabrics storage\n" }

sub list_volumes($class, $storeid, $scfg, $vmid = undef, $content_types = undef, @) {
    my $res = $class->list_images($storeid, $scfg, $vmid);
    $_->{content} = 'images' for @$res;
    return $res;
}

sub list_images($class, $storeid, $scfg, $vmid = undef, $vollist = undef, $cache = undef, @) {
    return [] if defined($vmid) && !$vollist;
    my $namespaces = PVE::NVMeOF::namespaces($scfg->{subsysnqn});
    my $res = [];
    for my $dev (sort keys %$namespaces) {
        my $ns = $namespaces->{$dev};
        my $volid = "$storeid:nvme-$ns->{wwid}";
        next if $vollist && !grep { $_ eq $volid } @$vollist;
        push @$res,
            {
                volid => $volid,
                format => 'raw',
                size => $ns->{size},
                vmid => 0,
            };
    }
    return $res;
}

sub status($class, $storeid, $scfg, $cache = undef, @) {
    my $controllers = PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn});
    return (0, 0, 0, PVE::NVMeOF::has_live_controller($controllers) ? 1 : 0);
}

sub activate_storage($class, $storeid, $scfg, $cache = undef, @) {
    my $controllers = PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn});
    my @missing = grep { !defined(PVE::NVMeOF::path_controller($controllers, $_)) }
        PVE::NVMeOF::storage_paths($scfg)->@*;
    if (!@missing) {
        die "no controller of the subsystem is live\n"
            if !PVE::NVMeOF::has_live_controller($controllers);
        return;
    }
    # missing prerequisites are not portal failures; continue degraded if a controller is live
    eval {
        PVE::NVMeOF::assert_support();
        PVE::NVMeOF::host_identity();
    };
    if (my $err = $@) {
        die $err if !PVE::NVMeOF::has_live_controller($controllers);
        warn "storage '$storeid': cannot connect to the other portals - $err";
        return;
    }
    my @errors;
    my $live = PVE::NVMeOF::has_live_controller($controllers);
    my $start = Time::HiRes::time();
    my $left = sub { $start + ($live ? 1 : 3) * $ACTIVATION_BUDGET - Time::HiRes::time() };
    for my $path (@missing) {
        my $portal = PVE::NVMeOF::format_path($path);
        if ($left->() < 5) {
            push @errors, "$portal: not tried within the time limit";
            next;
        }
        if (PVE::NVMeOF::in_cooldown($scfg, $path)) {
            push @errors, "$portal: failed less than a minute ago";
            next;
        }
        eval {
            if (!PVE::Network::tcp_ping($path->{traddr}, $path->{trsvcid}, 2)) {
                PVE::NVMeOF::note_connect_failure($scfg, $path);
                die "no answer from portal\n";
            }
            PVE::NVMeOF::connect_path($scfg, $path, $left->());
        };
        if (my $err = $@) {
            warn "storage '$storeid': connecting to '$portal' failed - $err";
            chomp $err;
            push @errors, "$portal: $err";
        } else {
            # a new controller can still be connecting
            $live = PVE::NVMeOF::has_live_controller(
                PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn}));
        }
    }
    $controllers = PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn});
    if (!PVE::NVMeOF::has_live_controller($controllers)) {
        die "cannot connect to any portal - " . join('; ', @errors) . "\n" if @errors;
        die "no controller of the subsystem is live yet\n";
    }
    return;
}

sub deactivate_storage { return }

sub check_connection($class, $storeid, $scfg, @) {
    my $controllers = PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn});
    return 1 if PVE::NVMeOF::has_live_controller($controllers);
    my $paths = PVE::NVMeOF::storage_paths($scfg);
    # with a controller for each portal, there is nothing to connect, only to wait for the kernel
    die "no controller of the subsystem is live\n"
        if !grep { !defined(PVE::NVMeOF::path_controller($controllers, $_)) } @$paths;
    # bound check time; remaining portals are deferred to activate_storage
    my $deadline = Time::HiRes::time() + $ACTIVATION_BUDGET;
    for my $path (@$paths) {
        return 1 if Time::HiRes::time() >= $deadline;
        next if defined(PVE::NVMeOF::path_controller($controllers, $path));
        next if PVE::NVMeOF::in_cooldown($scfg, $path);
        return 1 if PVE::Network::tcp_ping($path->{traddr}, $path->{trsvcid}, 2);
        PVE::NVMeOF::note_connect_failure($scfg, $path);
    }
    return 0;
}

sub activate_volume($class, $storeid, $scfg, $volname, $snapname = undef, $cache = undef, @) {
    my $path = $class->filesystem_path($scfg, $volname, $snapname);
    # as for the multipath map of an iSCSI LUN, a status pass does not wait for the device
    my $deadline = Time::HiRes::time() + ($cache && $cache->{status_only} ? 0 : 10);
    while (!-b $path && Time::HiRes::time() < $deadline) {
        last if !keys PVE::NVMeOF::subsystem_controllers($scfg->{subsysnqn})->%*;
        Time::HiRes::usleep(250_000);
    }
    my $real = Cwd::realpath($path);
    die "volume '$volname' is not available on this node\n" if !$real || !-b $real;
    assert_namespace($scfg, $volname, $real);
}

sub volume_has_feature(
    $class, $scfg, $feature, $storeid, $volname,
    $snapname = undef,
    $running = undef, @,
) {
    return 1 if $feature eq 'copy' && !defined($snapname);
    return undef;
}

1;
