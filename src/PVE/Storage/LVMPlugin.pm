package PVE::Storage::LVMPlugin;

use strict;
use warnings;

use Cwd qw(abs_path);
use Fcntl qw(O_RDWR O_EXCL);
use File::Basename;
use IO::File;
use JSON;
use List::Util qw(max);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

use PVE::Exception qw(raise_param_exc);
use PVE::Format qw(render_bytes render_duration);
use PVE::INotify;
use PVE::JSONSchema qw(get_standard_option);
use PVE::RESTEnvironment qw(log_warn);
use PVE::Tools qw(run_command file_read_firstline trim);

use PVE::Storage::Common;
use PVE::Storage::Plugin;

use base qw(PVE::Storage::Plugin);

use constant FORMAT_EXTENSION => {
    raw => '',
    qcow2 => 'qcow2',
};

# lvm helper functions

use constant {
    BLKDISCARD => 0x1277,
    BLKZEROOUT => 0x127f,
};

my $ignore_no_medium_warnings = sub {
    my $line = shift;
    # ignore those, most of the time they're from (virtual) IPMI/iKVM devices
    # and just spam the log..
    if ($line !~ /open failed: No medium found/) {
        print STDERR "$line\n";
    }
};

my sub fork_cleanup_worker {
    my ($cleanup_worker) = @_;

    return if !$cleanup_worker;
    my $rpcenv = PVE::RPCEnvironment::get();
    my $authuser = $rpcenv->get_user();
    $rpcenv->fork_worker('imgdel', undef, $authuser, $cleanup_worker);
}

sub lvm_pv_info {
    my ($device) = @_;

    die "no device specified" if !$device;

    my $has_label = 0;

    my $cmd = ['/usr/bin/file', '-L', '-s', $device];
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;
            $has_label = 1 if $line =~ m/LVM2/;
        },
    );

    return undef if !$has_label;

    $cmd = [
        '/sbin/pvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'k',
        '--unbuffered',
        '--nosuffix',
        '--options',
        'pv_name,pv_size,vg_name,pv_uuid',
        $device,
    ];

    my $pvinfo;
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;

            $line = trim($line);

            my ($pvname, $size, $vgname, $uuid) = split(':', $line);

            die "found multiple pvs entries for device '$device'\n"
                if $pvinfo;

            $pvinfo = {
                pvname => $pvname,
                size => int($size),
                vgname => $vgname,
                uuid => $uuid,
            };
        },
    );

    return $pvinfo;
}

sub clear_first_sector {
    my ($dev) = shift;

    if (my $fh = IO::File->new($dev, "w")) {
        my $buf = 0 x 512;
        syswrite $fh, $buf;
        $fh->close();
    }
}

sub lvm_create_volume_group {
    my ($device, $vgname, $shared) = @_;

    my $res = lvm_pv_info($device);

    if ($res->{vgname}) {
        return if $res->{vgname} eq $vgname; # already created
        die "device '$device' is already used by volume group '$res->{vgname}'\n";
    }

    clear_first_sector($device); # else pvcreate fails

    # we use --metadatasize 250k, which reseults in "pe_start = 512"
    # so pe_start is aligned on a 128k boundary (advantage for SSDs)
    my $cmd = ['/sbin/pvcreate', '--metadatasize', '250k', $device];

    run_command($cmd, errmsg => "pvcreate '$device' error");

    $cmd = ['/sbin/vgcreate', $vgname, $device];
    # push @$cmd, '-c', 'y' if $shared; # we do not use this yet

    run_command(
        $cmd,
        errmsg => "vgcreate $vgname $device error",
        errfunc => $ignore_no_medium_warnings,
        outfunc => $ignore_no_medium_warnings,
    );
}

sub lvm_destroy_volume_group {
    my ($vgname) = @_;

    run_command(
        ['vgremove', '-y', $vgname],
        errmsg => "unable to remove volume group $vgname",
        errfunc => $ignore_no_medium_warnings,
        outfunc => $ignore_no_medium_warnings,
    );
}

sub lvm_vgs {
    my ($includepvs) = @_;

    my $cmd = [
        '/sbin/vgs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--options',
    ];

    my $cols = [qw(vg_name vg_size vg_free lv_count)];

    if ($includepvs) {
        push @$cols, qw(pv_name pv_size pv_free);
    }

    push @$cmd, join(',', @$cols);

    my $vgs = {};
    eval {
        run_command(
            $cmd,
            outfunc => sub {
                my $line = shift;
                $line = trim($line);

                my ($name, $size, $free, $lvcount, $pvname, $pvsize, $pvfree) =
                    split(':', $line);

                $vgs->{$name} //= {
                    size => int($size),
                    free => int($free),
                    lvcount => int($lvcount),
                };

                if (defined($pvname) && defined($pvsize) && defined($pvfree)) {
                    push @{ $vgs->{$name}->{pvs} },
                        {
                            name => $pvname,
                            size => int($pvsize),
                            free => int($pvfree),
                        };
                }
            },
            errfunc => $ignore_no_medium_warnings,
        );
    };
    my $err = $@;

    # just warn (vgs return error code 5 if clvmd does not run)
    # but output is still OK (list without clustered VGs)
    warn $err if $err;

    return $vgs;
}

sub lvm_list_volumes {
    my ($vgname) = @_;

    my $option_list =
        'vg_name,lv_name,lv_size,lv_attr,pool_lv,data_percent,metadata_percent,snap_percent,uuid,tags,metadata_size,time';

    my $cmd = [
        '/sbin/lvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--config',
        'report/time_format="%s"',
        '--options',
        $option_list,
    ];

    push @$cmd, $vgname if $vgname;

    my $lvs = {};
    run_command(
        $cmd,
        outfunc => sub {
            my $line = shift;

            $line = trim($line);

            my (
                $vg_name,
                $lv_name,
                $lv_size,
                $lv_attr,
                $pool_lv,
                $data_percent,
                $meta_percent,
                $snap_percent,
                $uuid,
                $tags,
                $meta_size,
                $ctime,
            ) = split(':', $line);
            return if !$vg_name;
            return if !$lv_name;

            my $lv_type = substr($lv_attr, 0, 1);

            my $d = {
                lv_size => int($lv_size),
                lv_state => substr($lv_attr, 4, 1),
                lv_type => $lv_type,
            };
            $d->{pool_lv} = $pool_lv if $pool_lv;
            $d->{tags} = $tags if $tags;
            $d->{ctime} = $ctime;

            if ($lv_type eq 't') {
                $data_percent ||= 0;
                $meta_percent ||= 0;
                $snap_percent ||= 0;
                $d->{metadata_size} = int($meta_size);
                $d->{metadata_used} = int(($meta_percent * $meta_size) / 100);
                $d->{used} = int(($data_percent * $lv_size) / 100);
            }
            $lvs->{$vg_name}->{$lv_name} = $d;
        },
        errfunc => $ignore_no_medium_warnings,
    );

    return $lvs;
}

# Acquire the storage lock for a cleanup step of volume or snapshot removal, retrying the
# acquisition so transient lock contention on a busy cluster does not leave a half-removed volume
# behind. Only the acquisition is retried; $code_started keeps a failure inside the locked section
# from re-running the non-idempotent cleanup.
my sub lock_storage_with_acquire_retry {
    my ($class, $storeid, $scfg, $code) = @_;

    my $max_attempts = 5;
    my $attempt = 0;
    while (1) {
        $attempt++;
        my $code_started = 0;
        my $res = eval {
            $class->cluster_lock_storage(
                $storeid,
                $scfg->{shared},
                undef,
                sub { $code_started = 1; return $code->(@_); },
            );
        };
        return $res if !$@;

        die $@ if $code_started || $attempt >= $max_attempts;
        warn "could not acquire storage lock, retrying (attempt $attempt/$max_attempts): $@";
        sleep($attempt < 3 ? 1 : 3);
    }
}

my sub rename_after_failed_cleanup {
    my ($class, $scfg, $storeid, $vg, $name) = @_;

    eval {
        my $failed_name;
        lock_storage_with_acquire_retry(
            $class,
            $storeid,
            $scfg,
            sub {
                my $vgs = lvm_vgs();
                die "volume group '$vg' not found\n"
                    if !defined($vgs->{$vg});

                my $lvs = lvm_list_volumes($vg);
                my $existing = $lvs->{$vg} // {};

                my $prefix = 'failed-';
                my $suffix = "-del-$name";

                my $last_fail = max(
                    -1,
                    map {
                        /^\Q$prefix\E(\d+)\Q$suffix\E$/ ? $1 : ()
                    } keys %$existing,
                );

                $failed_name = $prefix . ($last_fail + 1) . $suffix;

                my $cmd = ['/sbin/lvrename', $vg, "del-$name", $failed_name];
                run_command(
                    $cmd,
                    errmsg => "lvrename '$vg/del-$name' to '$vg/$failed_name' error",
                );
                print "renamed '$vg/del-$name' to '$vg/$failed_name'\n";
            },
        );
    };
    if (my $rename_err = $@) {
        print STDERR "ERROR: unable to rename '$vg/del-$name': $rename_err";
    }
}

my sub blockdev_ioctl_range {
    my ($fh, $ioctl, $offset, $length) = @_;

    my $range = pack('QQ', $offset, $length);
    ioctl($fh, $ioctl, $range) or die "$!\n";
}

# The limit is in bytes per second, with an optional k, m or g suffix for powers of 1024, as taken by
# cstream's -t option, which existing configurations were written for.
my sub parse_saferemove_throughput {
    my ($value) = @_;

    my ($number, $unit) = $value =~ m/^(-?\d+)([kmg])?$/i
        or die "invalid saferemove throughput '$value'\n";
    my $multiplier = { k => 1024, m => 1024**2, g => 1024**3 }->{ lc($unit // '') } // 1;

    return $number * $multiplier;
}

my sub free_lvm_volumes_locked {
    my ($class, $scfg, $storeid, $volnames) = @_;

    my $vg = $scfg->{vgname};

    my $on_remove_opts = {};
    if ($scfg->{'on-volume-remove'}) {
        $on_remove_opts =
            PVE::JSONSchema::parse_property_string('on-volume-remove', $scfg->{'on-volume-remove'});
    }

    my $secure_delete_cmd = sub {
        my ($lvmpath) = @_;

        my $stepsize = $scfg->{'saferemove-stepsize'} // 32;
        $stepsize = $stepsize * 1024 * 1024;

        my $bdev = abs_path($lvmpath);

        my $sysdir = undef;
        if ($bdev && $bdev =~ m|^/dev/(dm-\d+)|) {
            $sysdir = "/sys/block/$1";
        } else {
            # removing the volume without zeroing it out would leave its data readable by new volumes
            die "cannot zero out volume '$lvmpath' - no device mapper link\n"
                if $scfg->{saferemove};
            warn "skip discarding volume '$lvmpath' - no device mapper link\n";
            return;
        }

        my $write_zeroes_max_bytes =
            file_read_firstline("$sysdir/queue/write_zeroes_max_bytes") // 0;
        ($write_zeroes_max_bytes) = $write_zeroes_max_bytes =~ m/^(\d+)$/; #untaint

        my $discard_granularity = file_read_firstline("$sysdir/queue/discard_granularity") // 0;
        ($discard_granularity) = $discard_granularity =~ m/^(\d+)$/; #untaint

        # Discard support is checked when the option gets set, but the volume group can lose it
        # later, so degrade to a plain removal instead of keeping the volume around.
        my $discard_supported = $on_remove_opts->{discard} ? 1 : 0;
        if ($discard_supported && !$discard_granularity) {
            log_warn("device does not support discard, not discarding '$lvmpath'");
            $discard_supported = 0;
        }

        my $size = file_read_firstline("$sysdir/size")
            or die "size from $sysdir cannot be read\n";
        ($size) = $size =~ m/^(\d+)$/; # untaint
        $size *= 512; # sysfs size is in 512-byte sectors

        my $zero_out = $scfg->{saferemove};
        my $zeroout_variant = $zero_out ? 'blkzeroout' : 'none';
        my $throughput = undef; # discards alone are not rate limited
        if ($zero_out && (my $value = $scfg->{saferemove_throughput})) {
            # Normalize zero spellings before selecting the default or dividing by the rate.
            $throughput = abs(parse_saferemove_throughput($value)) || undef;
        }
        if (defined($throughput)) {
            my $rendered_throughput = render_bytes($throughput);
            print "using saferemove throughput limit: $rendered_throughput/s\n";
        }

        # If the storage does not support write_zeroes fall back to writing zeroes manually using
        # syswrite. Otherwise if the storage supports write_zeroes but stepsize is too big,
        # reduce the stepsize to the maximum supported by the storage.
        my $zeroes;
        if ($zero_out && $write_zeroes_max_bytes == 0) {
            print "WRITE_ZEROES operation not supported,"
                . " falling back to syswrite to zero-out '$lvmpath'\n";
            $zeroout_variant = 'syswrite';
            $stepsize = 1024 * 1024; # 1 MiB
            print "reduce stepsize to 1 MiB for syswrite\n";
            $zeroes = "\0" x $stepsize;
            # limit throughput to 10MiB/s for syswrite, if throughput was not set
            if (!defined($throughput)) {
                # FIXME: MAJOR VERSION: increase to 100 MiB/s
                $throughput = 10485760;
                print "using default syswrite-saferemove throughput limit: 10 MiB/s\n";
            }
        } elsif ($zero_out && $stepsize > $write_zeroes_max_bytes) {
            print "reduce stepsize to the maximum supported by the storage:"
                . " $write_zeroes_max_bytes bytes\n";
            $stepsize = $write_zeroes_max_bytes;
        }

        # The block layer only discards allocation units that a request covers completely, and the
        # units are offset from the start of the device by the discard alignment. End each discard
        # batch on a unit boundary so that no unit is left partially discarded.
        my $discard_alignment = file_read_firstline("$sysdir/discard_alignment") // 0;
        ($discard_alignment) = $discard_alignment =~ m/^(\d+)$/; #untaint
        my $discard_offset = 0;

        return if !$zero_out && !$discard_supported; # nothing left to do before the removal

        # Open exclusively, so that a volume that is mounted or claimed by another kernel subsystem is
        # refused instead of wiped.
        sysopen(my $fh, $lvmpath, O_RDWR | O_EXCL) or die "can't open '$lvmpath' - $!\n";

        # eval block, so filehandle is closed even if something fails below
        eval {
            my $start = clock_gettime(CLOCK_MONOTONIC);
            my $written_total = 0;
            my $lastprint = -1;
            my $written;

            my $discard_attempts = 0;
            my $discard_failures = 0;

            for (my $offset = 0; $offset < $size; $offset += $written) {

                if ($offset + $stepsize > $size) {
                    $stepsize = $size - $offset;
                }

                if ($zeroout_variant eq 'blkzeroout') {
                    eval { blockdev_ioctl_range($fh, BLKZEROOUT, $offset, $stepsize); };
                    if (my $err = $@) {
                        die "blkzeroout for $stepsize bytes at offset $offset failed: $err";
                    }
                    $written = $stepsize;
                } elsif ($zeroout_variant eq 'syswrite') {

                    # allow retrying once if syswrite writes zero bytes
                    $written = syswrite($fh, $zeroes, $stepsize, 0);
                    if (!defined($written)) {
                        die "syswrite failed: $!\n";
                    } elsif ($written == 0) {
                        warn "syswrite wrote 0 bytes, retrying\n";
                    }

                    while ($written < $stepsize) {
                        my $remaining = $stepsize - $written;
                        my $retried_write = syswrite($fh, $zeroes, $remaining, $written);
                        if (!defined($retried_write)) {
                            die "syswrite failed: $!\n";
                        } elsif ($retried_write == 0) {
                            die "syswrite failed: wrote 0 bytes\n";
                        }
                        $written += $retried_write;
                    }
                } elsif ($zeroout_variant eq 'none') {
                    $written = $stepsize;
                }
                $written_total += $written;

                my $discard_end = 0;
                if ($discard_supported) {
                    $discard_end =
                        $written_total == $size
                        ? $size
                        : $written_total -
                        (($written_total - $discard_alignment) % $discard_granularity);
                }
                if ($discard_end > $discard_offset) {
                    if ($zeroout_variant eq 'syswrite') {
                        # Flush zeroes written using syswrite before discarding the
                        # corresponding range
                        $fh->sync()
                            or die "fsync before discard at offset $discard_offset failed: $!\n";
                    }

                    my $discard_length = $discard_end - $discard_offset;
                    $discard_attempts++;

                    eval {
                        blockdev_ioctl_range($fh, BLKDISCARD, $discard_offset, $discard_length);
                    };
                    if (my $err = $@) {
                        # nothing in between touches errno, so this still refers to the ioctl
                        if ($!{EOPNOTSUPP}) {
                            log_warn("device does not support discard, not discarding the"
                                . " remaining $discard_length bytes of '$lvmpath'");
                            $discard_supported = 0;
                        } else {
                            if ($discard_failures == 0) {
                                log_warn("blkdiscard for $discard_length bytes at offset"
                                    . " $discard_offset failed: $err");
                            }
                            $discard_failures += 1;
                        }
                    }
                    $discard_offset = $discard_end;
                }
                last if !$zero_out && !$discard_supported; # discard turned out to be unsupported

                my $curr_time = clock_gettime(CLOCK_MONOTONIC);
                if (($curr_time - $lastprint) >= 3) {
                    my $percent_finished = 100 * $written_total / $size;
                    my $curr_seconds = $curr_time - $start;

                    printf(
                        "%s %s of %s (%.2f%%)%s in %s\n",
                        $zero_out ? 'zeroed out' : 'discarded',
                        render_bytes($written_total),
                        render_bytes($size),
                        $percent_finished,
                        $zero_out ? " using $zeroout_variant" : '',
                        render_duration($curr_seconds),
                    );
                    $lastprint = $curr_time;
                }

                if (defined($throughput)) {
                    my $expected_elapsed = $written_total / $throughput;
                    my $actual_elapsed = $curr_time - $start;
                    my $delay = $expected_elapsed - $actual_elapsed;
                    if ($delay > 0) {
                        Time::HiRes::sleep($delay);
                    }
                }
            }
            # syswrite only fills the page cache, a writeback error would otherwise be lost on close
            if ($zeroout_variant eq 'syswrite') {
                $fh->sync() or die "fsync after zeroing out '$lvmpath' failed: $!\n";
            }
            if ($discard_failures != 0) {
                die "$discard_failures out of $discard_attempts blkdiscards failed\n";
            }
        };
        # close filehandle before throwing an error
        my $err = $@;
        close($fh);
        if ($err) {
            die "$err";
        }
    };
    # we need to zero out LVM data for security reasons
    # and discard images to free storage space to allow
    # thin provisioning
    my $cleanup_worker = sub {

        my $total_cleanup_errors = 0;
        for my $name (@$volnames) {
            my $lvmpath = "/dev/$vg/del-$name";

            my $discard_action;
            if ($scfg->{saferemove} && $on_remove_opts->{discard}) {
                $discard_action = 'zero-out and discard (TRIM)';
            } elsif ($scfg->{saferemove}) {
                $discard_action = 'zero-out';
            } elsif ($on_remove_opts->{discard}) {
                $discard_action = 'discard (TRIM)';
            }
            print "$discard_action data on image $name ($lvmpath)\n";

            eval {
                # drop the lvm stderr output here, a failure is reported below with its last line
                # and more context
                my $cmd_activate = ['/sbin/lvchange', '-aly', $lvmpath];
                run_command(
                    $cmd_activate,
                    errmsg => "can't activate LV '$lvmpath' to $discard_action its data",
                    errfunc => sub { },

                );
                $cmd_activate = ['/sbin/lvchange', '--refresh', $lvmpath];
                run_command(
                    $cmd_activate,
                    errmsg => "can't refresh LV '$lvmpath' to $discard_action its data",
                    errfunc => sub { },
                );
            };
            if (my $activation_err = $@) {
                print STDERR "ERROR: $activation_err";
                eval { rename_after_failed_cleanup($class, $scfg, $storeid, $vg, $name) };
                $total_cleanup_errors += 1;
                next;
            }

            eval {
                $secure_delete_cmd->($lvmpath);
                lock_storage_with_acquire_retry(
                    $class,
                    $storeid,
                    $scfg,
                    sub {
                        my $cmd = ['/sbin/lvremove', '-f', "$vg/del-$name"];
                        run_command($cmd, errmsg => "lvremove '$vg/del-$name' error");
                    },
                );
            };
            if (my $cleanup_err = $@) {
                print STDERR "ERROR: cleanup failed for lv $name: $cleanup_err";
                eval { rename_after_failed_cleanup($class, $scfg, $storeid, $vg, $name) };
                $total_cleanup_errors += 1;
                next;
            }
            print "successfully removed volume $name ($vg/del-$name)\n";
        }
        if ($total_cleanup_errors != 0) {
            my $number_of_vols = scalar @$volnames;
            die "cleanup failed for $total_cleanup_errors out of $number_of_vols volumes\n";
        }
    };

    if ($scfg->{saferemove} || $on_remove_opts->{discard}) {
        # The cleanup worker overwrites or discards the data, so refuse volumes that are still in
        # use, like lvremove does for a direct removal. Deactivating does the same in-use check and
        # waits for transient openers like udev. The worker activates the volumes again.
        for my $name (@$volnames) {
            my $cmd = ['/sbin/lvchange', '-aln', "$vg/$name"];
            run_command($cmd, errmsg => "can't deactivate LV '$vg/$name'");
        }
        for my $name (@$volnames) {
            # avoid long running task, so we only rename here
            my $cmd = ['/sbin/lvrename', $vg, $name, "del-$name"];
            run_command($cmd, errmsg => "lvrename '$vg/$name' error");
        }
        return $cleanup_worker;
    } else {
        for my $name (@$volnames) {
            my $cmd = ['/sbin/lvremove', '-f', "$vg/$name"];
            run_command($cmd, errmsg => "lvremove '$vg/$name' error");
        }
    }

    return undef;
}

# Configuration

sub type {
    return 'lvm';
}

sub plugindata {
    return {
        content => [{ images => 1, rootdir => 1 }, { images => 1 }],
        format => [{ raw => 1, qcow2 => 1 }, 'raw'],
        'sensitive-properties' => {},
    };
}

my $on_volume_remove_format = {
    discard => {
        description => "Issue discard (TRIM) requests for LVs before removing them.",
        type => 'boolean',
        optional => 1,
        verbose_description =>
            "If enabled, discard (TRIM) requests are issued for the LV's block"
            . " range before removing it, allowing thin-provisioned storage to reclaim previously"
            . " allocated physical space, provided the storage supports discard.",
    },
};

sub verify_on_volume_remove {
    my ($value, $noerr) = @_;

    return undef if !defined($value);

    if (!keys %$value) {
        return undef if $noerr;
        die "at least one on-volume-remove option must be specified if the property is set\n";
    }
    return $value;
}

PVE::JSONSchema::register_format(
    'on-volume-remove',
    $on_volume_remove_format,
    \&verify_on_volume_remove,
);

sub properties {
    return {
        vgname => {
            description => "Volume group name.",
            type => 'string',
            format => 'pve-storage-vgname',
        },
        base => {
            description => "Base volume. This volume is automatically activated.",
            type => 'string',
            format => 'pve-volume-id',
        },
        saferemove => {
            description => "Zero-out data when removing LVs.",
            type => 'boolean',
        },
        'on-volume-remove' => {
            description => "Optional actions when removing LVs.",
            type => 'string',
            format => 'on-volume-remove',
            verbose_description => "Configure actions performed before removing an LV."
                . " Use 'discard=1' to issue discard (TRIM) requests before removal.",
        },
        'saferemove-stepsize' => {
            description => "Wipe step size in MiB."
                . " It will be capped to the maximum supported by the storage.",
            default => 32,
            enum => [qw(1 2 4 8 16 32)],
            type => 'integer',
        },
        saferemove_throughput => {
            description => "Wipe throughput in bytes per second (optional suffix: k, m, or g).",
            type => 'string',
            pattern => '-?\d+[kmgKMG]?',
        },
        tagged_only => {
            description => "Only list logical volumes tagged with 'pve-vm-ID'.",
            type => 'boolean',
        },
    };
}

sub options {
    return {
        vgname => { fixed => 1 },
        nodes => { optional => 1 },
        shared => { optional => 1 },
        disable => { optional => 1 },
        saferemove => { optional => 1 },
        'on-volume-remove' => { optional => 1 },
        'saferemove-stepsize' => { optional => 1 },
        saferemove_throughput => { optional => 1 },
        content => { optional => 1 },
        base => { fixed => 1, optional => 1 },
        tagged_only => { optional => 1 },
        bwlimit => { optional => 1 },
        'snapshot-as-volume-chain' => { optional => 1 },
    };
}

# Storage implementation

sub get_formats {
    my ($class, $scfg, $storeid) = @_;

    if ($scfg->{'snapshot-as-volume-chain'}) {
        return { default => 'qcow2', valid => { 'qcow2' => 1, 'raw' => 1 } };
    }

    return { default => 'raw', valid => { 'raw' => 1 } };
}

my sub get_discard_max {
    my ($dev_path) = @_;

    my $output = '';
    # use lsblk as it resolves discard support in setups with nested partitions or lv/vgs
    my $cmd = [
        'lsblk', '--json', '--bytes', '--discard', '--nodeps', '--output', 'DISC-MAX',
        $dev_path,
    ];

    eval {
        run_command($cmd, outfunc => sub { $output .= "$_[0]\n"; });
    };
    if (my $err = $@) {
        chomp $err;
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but lsblk could not "
                . "query discard support for the backing device '$dev_path': $err",
        });
    }

    my $parsed = eval { decode_json($output) };
    if (my $err = $@ || ref($parsed) ne 'HASH') {
        chomp $err if $err;
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but lsblk could not "
                . "parse discard support for the backing device '$dev_path'"
                . ($err ? ": $err" : ""),
        });
    }

    my $blockdevices = $parsed->{blockdevices};
    if (ref($blockdevices) ne 'ARRAY' || scalar($blockdevices->@*) == 0) {
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but lsblk could not "
                . "parse discard support for the backing device '$dev_path'",
        });

    }

    if (scalar($blockdevices->@*) > 1) {
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but lsblk returned "
                . "ambiguous discard support for the backing device '$dev_path'",
        });
    }

    return $blockdevices->[0]->{'disc-max'} // 0;
}

my sub assert_device_discard_supported {
    my ($dev_path) = @_;

    $dev_path = abs_path($dev_path) // $dev_path;

    if ($dev_path =~ m!^(/dev/.+)$!) {
        $dev_path = $1; # untaint
    } else {
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but discard support "
                . "cannot be resolved for the backing device '$dev_path'",
        });
    }

    # use discard_max_bytes as indicator if discard is supported
    if (!get_discard_max($dev_path)) {
        raise_param_exc({
            'on-volume-remove' => "discard on remove is enabled, but discard is not "
                . "supported by the backing device '$dev_path'",
        });
    }
}

my sub discard_on_remove_requested {
    my ($on_remove) = @_;

    return 0 if !defined($on_remove);

    my $on_remove_opts = PVE::JSONSchema::parse_property_string('on-volume-remove', $on_remove);

    return $on_remove_opts->{discard} ? 1 : 0;
}

sub assert_discard_supported {
    my ($vgname, $on_remove, $nodes) = @_;

    return if !discard_on_remove_requested($on_remove);

    # Only the node handling the request can be probed, and it need not see the volume group if the
    # storage is restricted to other nodes. The cleanup worker skips discards on nodes without
    # support anyway, so the probe just catches a misconfiguration early.
    my $nodename = PVE::INotify::nodename();
    if ($nodes && !$nodes->{$nodename}) {
        warn "storage is not available on node '$nodename', not checking discard support of"
            . " volume group '$vgname'\n";
        return;
    }

    my $vgs = lvm_vgs(1);
    my $vg = $vgs->{$vgname};
    die "no such volume group '$vgname'\n" if !$vg;

    my $pvs = $vg->{pvs};
    die "volume group '$vgname' has no physical volumes\n"
        if !defined($pvs) || scalar($pvs->@*) == 0;

    # check if all the block devices configured to the volume group support discard
    for my $pv ($pvs->@*) {
        assert_device_discard_supported($pv->{name});
    }
}

sub on_add_hook {
    my ($class, $storeid, $scfg, %param) = @_;

    if (my $base = $scfg->{base}) {
        my ($baseid, $volname) = PVE::Storage::Plugin::parse_volume_id($base);

        my $cfg = PVE::Storage::config();
        my $basecfg = PVE::Storage::storage_config($cfg, $baseid, 1);
        die "base storage ID '$baseid' does not exist\n" if !$basecfg;

        # we only support iscsi for now
        die "unsupported base type '$basecfg->{type}'"
            if $basecfg->{type} ne 'iscsi';

        my $path = PVE::Storage::path($cfg, $base);

        PVE::Storage::activate_storage($cfg, $baseid);

        # a failing hook does not remove a volume group it created, so check the device first
        assert_device_discard_supported($path)
            if discard_on_remove_requested($scfg->{'on-volume-remove'});

        lvm_create_volume_group($path, $scfg->{vgname}, $scfg->{shared});
    }

    assert_discard_supported($scfg->{vgname}, $scfg->{'on-volume-remove'}, $scfg->{nodes});

    return;
}

sub on_update_hook_full {
    my ($class, $storeid, $scfg, $update, $delete, $sensitive) = @_;

    if (
        $scfg->{'snapshot-as-volume-chain'} # currently set
        && ( # and won't be set after update, because:
            (
                defined($update->{'snapshot-as-volume-chain'})
                && !$update->{'snapshot-as-volume-chain'}
            ) # explicitly set to disabled
            || grep { $_ eq 'snapshot-as-volume-chain' } $delete->@* # or deleted
        )
    ) {
        my $images = $class->list_images($storeid, $scfg, undef, undef, undef);
        die "$storeid - cannot disable 'snapshot-as-volume-chain' while a qcow2 image exists\n"
            if grep { $_->{format} eq 'qcow2' } $images->@*;
    }
    # the check probes the node handling the request, so only run it when the setting changes
    if (($update->{'on-volume-remove'} // '') ne ($scfg->{'on-volume-remove'} // '')) {
        my $nodes = $update->{nodes} // $scfg->{nodes};
        $nodes = undef if grep { $_ eq 'nodes' } ($delete // [])->@*;
        assert_discard_supported($scfg->{vgname}, $update->{'on-volume-remove'}, $nodes);
    }
}

sub parse_volname {
    my ($class, $volname) = @_;

    PVE::Storage::Plugin::parse_lvm_name($volname);

    if ($volname =~ m/^(vm-(\d+)-\S+)$/) {
        my $name = $1;
        my $vmid = $2;
        my $format = $volname =~ m/\.qcow2$/ ? 'qcow2' : 'raw';
        return ('images', $name, $vmid, undef, undef, undef, $format);
    }

    die "unable to parse lvm volume name '$volname'\n";
}

my sub get_snap_name {
    my ($class, $volname, $snapname) = @_;

    die "missing snapname\n" if !$snapname;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);
    if ($snapname eq 'current') {
        return $name;
    } else {
        $name =~ s/\.[^.]+$//;
        return "snap_${name}_${snapname}.qcow2";
    }
}

my sub parse_snap_name {
    my ($name, $short_volname) = @_;

    $short_volname =~ s/\.(qcow2)$//;

    if ($name =~ m/^snap_\Q$short_volname\E_(.*)\.qcow2$/) {
        return $1;
    }
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    die "snapshot is working with qcow2 format only" if defined($snapname) && $format ne 'qcow2';

    my $vg = $scfg->{vgname};
    $name = get_snap_name($class, $volname, $snapname) if $snapname;

    my $path = "/dev/$vg/$name";

    return wantarray ? ($path, $vmid, $vtype) : $path;
}

sub qemu_blockdev_options {
    my ($class, $scfg, $storeid, $volname, $machine_version, $options) = @_;

    my ($path) = $class->path($scfg, $volname, $storeid, $options->{'snapshot-name'});

    my $blockdev = { driver => 'host_device', filename => $path };

    return $blockdev;
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in lvm storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in lvm storage\n";
}

sub find_free_diskname {
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;

    my $vg = $scfg->{vgname};

    my $lvs = lvm_list_volumes($vg);

    my $disk_list = [keys %{ $lvs->{$vg} }];

    $add_fmt_suffix = $fmt && $fmt eq 'qcow2' ? 1 : undef;

    return PVE::Storage::Plugin::get_next_vm_diskname(
        $disk_list, $storeid, $vmid, $fmt, $scfg, $add_fmt_suffix,
    );
}

sub lvcreate {
    my ($vg, $name, $size, $tags) = @_;

    if ($size =~ m/\d$/) { # no unit is given
        $size .= "k"; # default to kilobytes
    }

    my $cmd = [
        '/sbin/lvcreate',
        '-aly',
        '-Wy',
        '--yes',
        '--size',
        $size,
        '--name',
        $name,
        '--setautoactivation',
        'n',
    ];
    for my $tag (@$tags) {
        push @$cmd, '--addtag', $tag;
    }
    push @$cmd, $vg;

    run_command($cmd, errmsg => "lvcreate '$vg/$name' error");
}

sub lvrename {
    my ($scfg, $oldname, $newname) = @_;

    my $vg = $scfg->{vgname};
    my $lvs = lvm_list_volumes($vg);
    die "target volume '${newname}' already exists\n"
        if ($lvs->{$vg}->{$newname});

    run_command(
        ['/sbin/lvrename', $vg, $oldname, $newname],
        errmsg => "lvrename '${vg}/${oldname}' to '${newname}' error",
    );
}

my sub lvm_qcow2_format {
    my ($class, $storeid, $scfg, $name, $fmt, $backing_snap, $size) = @_;

    $class->activate_volume($storeid, $scfg, $name);
    my $path = $class->path($scfg, $name, $storeid);

    my $options = {
        preallocation => PVE::Storage::Plugin::preallocation_cmd_opt($scfg, $fmt),
    };
    if ($backing_snap) {
        my $backing_volname = get_snap_name($class, $name, $backing_snap);
        PVE::Storage::Common::qemu_img_create_qcow2_backed($path, $backing_volname, $fmt, $options);
    } else {
        PVE::Storage::Common::qemu_img_create($fmt, $size, $path, $options);
    }
}

my sub calculate_lvm_size {
    my ($size, $fmt, $backing_snap) = @_;
    #input size = qcow2 image size in kb

    return $size if $fmt ne 'qcow2';

    my $options = $backing_snap ? ['extended_l2=on', 'cluster_size=128k'] : [];

    my $json = PVE::Storage::Common::qemu_img_measure($size, $fmt, 5, $options);
    die "failed to query file information with qemu-img measure\n" if !$json;
    my $info = eval { decode_json($json) };
    if ($@) {
        die "Invalid JSON: $@\n";
    }

    die "Missing fully-allocated value from json" if !$info->{'fully-allocated'};

    return $info->{'fully-allocated'} / 1024;
}

my sub alloc_lvm_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size, $backing_snap) = @_;

    die "unsupported format '$fmt'" if $fmt ne 'raw' && $fmt ne 'qcow2';

    die "snapshot-as-volume-chain option need to be enabled to use qcow2 format"
        if $fmt eq 'qcow2'
        && !$scfg->{'snapshot-as-volume-chain'};

    $class->parse_volname($name);

    my $vgs = lvm_vgs();

    my $vg = $scfg->{vgname};

    die "no such volume group '$vg'\n" if !defined($vgs->{$vg});

    my $free = int($vgs->{$vg}->{free});
    my $lvmsize = calculate_lvm_size($size, $fmt, $backing_snap);

    die "not enough free space ($free < $size)\n" if $free < $size;

    my $tags = ["pve-vm-$vmid"];
    # TODO PVE 10 - stop setting the volume name as a tag? It was initially used for (de)activation,
    # but there are false positives if there are multiple storages, see bug #7143. Including the
    # storage ID as a fix would've made moving disks more involved and break manual moving or
    # renaming. Instead (de)activation switched to using '--select'. The rename_volume() method
    # needs to be adapted as well.
    # tags all snapshots volumes with the main volume tag
    push @$tags, "\@pve-$name" if $fmt eq 'qcow2';
    lvcreate($vg, $name, $lvmsize, $tags);

    return if $fmt ne 'qcow2';

    #format the lvm volume with qcow2 format
    eval { lvm_qcow2_format($class, $storeid, $scfg, $name, $fmt, $backing_snap, $size) };
    if ($@) {
        my $err = $@;
        #no need to safe cleanup as the volume is still empty
        eval {
            my $cmd = ['/sbin/lvremove', '-f', "$vg/$name"];
            run_command($cmd, errmsg => "lvremove '$vg/$name' error");
        };
        die $err;
    }

}

sub get_parsed_format {
    my ($class, $name) = @_;

    $class->parse_volname($name); # dies for names that are not valid volume names

    return $name =~ m/\.(raw|qcow2|vmdk|subvol)$/ ? $1 : 'raw';
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    $name = $class->find_free_diskname($storeid, $scfg, $vmid, $fmt)
        if !$name;

    $name = $class->volname_for_format($name, $fmt, 0);

    alloc_lvm_image($class, $storeid, $scfg, $vmid, $fmt, $name, $size);

    return $name;
}

my sub alloc_snap_image {
    my ($class, $storeid, $scfg, $volname, $backing_snap) = @_;

    my ($vmid, $format) = ($class->parse_volname($volname))[2, 6];
    my $path = $class->path($scfg, $volname, $storeid, $backing_snap);

    #we need to use same size than the backing image qcow2 virtual-size
    my $size = PVE::Storage::Plugin::file_size_info($path, 5, $format);
    die "file_size_info on '$volname' failed\n" if !defined($size);

    $size = $size / 1024; #we use kb in lvcreate

    alloc_lvm_image($class, $storeid, $scfg, $vmid, $format, $volname, $size, $backing_snap);
}

my sub free_snap_image_locked {
    my ($class, $storeid, $scfg, $volname, $snap) = @_;

    my $snap_volname = get_snap_name($class, $volname, $snap);
    return free_lvm_volumes_locked($class, $scfg, $storeid, [$snap_volname]);
}

# Must be called with the storage lock held.
sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    my $name = ($class->parse_volname($volname))[1];

    my $volnames = [$volname];

    if ($format eq 'qcow2') {
        # Activate the volume to read its snapshot chain.
        $class->activate_volume($storeid, $scfg, $volname);
        my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
        for my $snapid (
            sort { $snapshots->{$a}->{order} <=> $snapshots->{$b}->{order} }
            keys %$snapshots
        ) {
            my $snap = $snapshots->{$snapid};
            next if $snapid eq 'current';
            next if !$snap->{volid};
            my ($snap_storeid, $snap_volname) =
                PVE::Storage::Plugin::parse_volume_id($snap->{volid});
            push @$volnames, $snap_volname;
        }
    }

    return free_lvm_volumes_locked($class, $scfg, $storeid, $volnames);
}

my $check_tags = sub {
    my ($tags) = @_;

    return defined($tags) && $tags =~ /(^|,)pve-vm-\d+(,|$)/;
};

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $vgname = $scfg->{vgname};

    $cache->{lvs} = lvm_list_volumes() if !$cache->{lvs};

    my $res = [];

    if (my $dat = $cache->{lvs}->{$vgname}) {

        foreach my $volname (keys %$dat) {

            next if $volname !~ m/^vm-(\d+)-/;
            my $owner = $1;

            my $info = $dat->{$volname};

            next if $scfg->{tagged_only} && !&$check_tags($info->{tags});

            # Allow mirrored and RAID LVs
            next if $info->{lv_type} !~ m/^[-mMrR]$/;

            my $volid = "$storeid:$volname";

            if ($vollist) {
                my $found = grep { $_ eq $volid } @$vollist;
                next if !$found;
            } else {
                next if defined($vmid) && ($owner ne $vmid);
            }

            my $format = ($class->parse_volname($volname))[6];
            my $entry = {
                volid => $volid,
                format => $format,
                vmid => $owner,
                ctime => $info->{ctime},
            };

            if ($format eq 'qcow2') {
                my $size;
                if ($info->{lv_state} eq 'a') {
                    $size = $class->volume_size_info($scfg, $storeid, $volname);
                }
                if (defined($size)) {
                    $entry->{size} = $size;
                } else {
                    $entry->{'approximate-size'} = $info->{lv_size};
                }
            } else {
                $entry->{size} = $info->{lv_size};
            }

            push @$res, $entry;
        }
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    my $vgname = $scfg->{vgname};

    if (my $info = $cache->{vgs}->{$vgname}) {
        return ($info->{size}, $info->{free}, $info->{size} - $info->{free}, 1);
    }

    return undef;
}

sub volume_snapshot_info {
    my ($class, $scfg, $storeid, $volname) = @_;

    return {} if !$scfg->{'snapshot-as-volume-chain'};

    my $short_volname = ($class->parse_volname($volname))[1];

    my $get_snapname_from_path = sub {
        my ($path) = @_;

        my $name = basename($path);
        if (my $snapname = parse_snap_name($name, $short_volname)) {
            return $snapname;
        } elsif ($name eq $volname) {
            return 'current';
        }
        return undef;
    };

    my $path = $class->filesystem_path($scfg, $volname);

    my $json = PVE::Storage::Common::qemu_img_info($path, undef, 10, 1);
    die "failed to query file information with qemu-img\n" if !$json;
    my $json_decode = eval { decode_json($json) };
    if ($@) {
        die "Can't decode qemu snapshot list. Invalid JSON: $@\n";
    }
    my $info = {};
    my $order = 0;

    my $snapshots = $json_decode;
    for my $snap (@$snapshots) {
        my $snapfile = $snap->{filename};
        ($snapfile) = $snapfile =~ m|^(/.*)|; # untaint
        my $snapname = $get_snapname_from_path->($snapfile);
        #not a proxmox snapshot
        next if !$snapname;

        my $snapvolname = get_snap_name($class, $volname, $snapname);

        $info->{$snapname}->{order} = $order;
        $info->{$snapname}->{file} = $snapfile;
        $info->{$snapname}->{volname} = "$snapvolname";
        $info->{$snapname}->{volid} = "$storeid:$snapvolname";
        $info->{$snapname}->{'virtual-size'} = $snap->{'virtual-size'};

        my $parentfile = $snap->{'backing-filename'};
        if ($parentfile) {
            my $parentname = $get_snapname_from_path->($parentfile);
            $info->{$snapname}->{parent} = $parentname;
            $info->{$parentname}->{child} = $snapname;
        }
        $order++;
    }
    return $info;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    $cache->{vgs} = lvm_vgs() if !$cache->{vgs};

    # In LVM2, vgscans take place automatically;
    # this is just to be sure
    if (
        $cache->{vgs}
        && !$cache->{vgscaned}
        && !$cache->{vgs}->{ $scfg->{vgname} }
    ) {
        $cache->{vgscaned} = 1;
        my $cmd = ['/sbin/vgscan', '--ignorelockingfailure', '--mknodes'];
        eval {
            run_command($cmd, outfunc => sub { });
        };
        warn $@ if $@;
    }

    # we do not acticate any volumes here ('vgchange -aly')
    # instead, volumes are activate individually later
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $cmd = ['/sbin/vgchange', '-aln', $scfg->{vgname}];
    run_command($cmd, errmsg => "can't deactivate VG '$scfg->{vgname}'");
}

=head3 get_activate_volume_target_opts()

    my ($target_opts, $target_str) =
        get_activate_volume_target_opts($class, $scfg, $path, $volname);

Returns C<$target_opts>, which is an array reference with parameters for C<lvchange> for selecting
the volume given by C<($path,$volname)>, or volume chain for C<qcow2>. Also returns a description of
the target C<$target_str> that can be used for log or error messages.

=cut

my sub get_activate_volume_target_opts {
    my ($class, $scfg, $path, $volname) = @_;

    my ($name, $format) = ($class->parse_volname($volname))[1, 6];

    my ($target_opts, $target_str);

    if ($format eq 'qcow2') {
        my $vg = $scfg->{vgname};
        $name =~ s/\.qcow2$//;
        # The LVM name schema allows '.', which is also a regex metacharacter, so escape it
        # before using the name in the regex.
        my $name_re = $name =~ s/\./\\./gr;

        my $filter =
            "vg_name = \"$vg\"" . " && lv_name =~ '^(${name_re}|snap_${name_re}_.+)\\.qcow2\$'";

        $target_opts = ['--select', $filter];
        $target_str = "volume chain for LV '$path'";
    } else {
        $target_opts = [$path];
        $target_str = "LV '$path'";
    }

    return ($target_opts, $target_str);
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $path = $class->path($scfg, $volname, $storeid, $snapname);

    my ($target_opts, $target_str) =
        get_activate_volume_target_opts($class, $scfg, $path, $volname);

    my $cmd = ['/sbin/lvchange', '-aey', $target_opts->@*];
    run_command($cmd, errmsg => "can't activate $target_str");
    $cmd = ['/sbin/lvchange', '--refresh', $target_opts->@*];
    run_command($cmd, errmsg => "can't refresh $target_str for activation");
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    my $path = $class->path($scfg, $volname, $storeid, $snapname);
    return if !-b $path;

    my ($target_opts, $target_str) =
        get_activate_volume_target_opts($class, $scfg, $path, $volname);

    my $cmd = ['/sbin/lvchange', '-aln', $target_opts->@*];
    run_command($cmd, errmsg => "can't deactivate $target_str");
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running, $snapname) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    my $lvmsize = calculate_lvm_size($size / 1024, $format);
    $lvmsize = "${lvmsize}k";

    my $path = $class->path($scfg, $volname, $storeid, $snapname);

    my $cmd = ['/sbin/lvextend', '-L', $lvmsize, $path];

    $class->cluster_lock_storage(
        $storeid,
        $scfg->{shared},
        undef,
        sub {
            run_command($cmd, errmsg => "error resizing volume '$path'");
        },
    );

    if (!$running && $format eq 'qcow2') {
        my $preallocation = PVE::Storage::Plugin::preallocation_cmd_opt($scfg, $format);
        PVE::Storage::Common::qemu_img_resize($path, $format, $size, $preallocation);
    }

    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my ($format) = ($class->parse_volname($volname))[6];
    my $path = $class->filesystem_path($scfg, $volname);

    return PVE::Storage::Plugin::file_size_info($path, $timeout, $format) if $format eq 'qcow2';

    my $cmd = [
        '/sbin/lvs',
        '--separator',
        ':',
        '--noheadings',
        '--units',
        'b',
        '--unbuffered',
        '--nosuffix',
        '--options',
        'lv_size',
        $path,
    ];

    my $size;
    run_command(
        $cmd,
        timeout => $timeout,
        errmsg => "can't get size of '$path'",
        outfunc => sub {
            $size = int(shift);
        },
    );
    return wantarray ? ($size, 'raw', 0, undef) : $size;
}

my sub volume_snapshot_locked {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my ($vmid, $format) = ($class->parse_volname($volname))[2, 6];

    die "can't snapshot '$format' volume\n" if $format ne 'qcow2';

    $class->activate_volume($storeid, $scfg, $volname);

    #rename current volume to snap volume
    eval { $class->rename_snapshot($scfg, $storeid, $volname, 'current', $snap) };
    die "error rename $volname to $snap - $@\n" if $@;

    eval { alloc_snap_image($class, $storeid, $scfg, $volname, $snap) };
    if ($@) {
        my $err = $@;
        eval { $class->rename_snapshot($scfg, $storeid, $volname, $snap, 'current') };
        die $err;
    }
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    return $class->cluster_lock_storage(
        $storeid,
        $scfg->{shared},
        undef,
        sub { return volume_snapshot_locked($class, $scfg, $storeid, $volname, $snap); },
    );
}

# Asserts that a rollback to $snap on $volname is possible.
# If certain snapshots are preventing the rollback and $blockers is an array
# reference, the snapshot names can be pushed onto $blockers prior to dying.
sub volume_rollback_is_possible {
    my ($class, $scfg, $storeid, $volname, $snap, $blockers) = @_;

    my $format = ($class->parse_volname($volname))[6];
    die "can't rollback snapshot for '$format' volume\n" if $format ne 'qcow2';

    $class->activate_volume($storeid, $scfg, $volname);

    my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
    my $found;
    $blockers //= []; # not guaranteed to be set by caller
    for my $snapid (
        sort { $snapshots->{$b}->{order} <=> $snapshots->{$a}->{order} }
        keys %$snapshots
    ) {
        next if $snapid eq 'current';

        if ($snapid eq $snap) {
            $found = 1;
        } elsif ($found) {
            push $blockers->@*, $snapid;
        }
    }

    die "can't rollback, snapshot '$snap' does not exist on '$volname'\n"
        if !$found;

    die "can't rollback, '$snap' is not most recent snapshot on '$volname'\n"
        if scalar($blockers->@*) > 0;

    return 1;
}

my sub volume_snapshot_rollback_locked {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $format = ($class->parse_volname($volname))[6];

    die "can't rollback snapshot for '$format' volume\n" if $format ne 'qcow2';

    my $cleanup_worker =
        eval { free_snap_image_locked($class, $storeid, $scfg, $volname, 'current'); };
    die "error deleting snapshot $snap $@\n" if $@;

    eval { alloc_snap_image($class, $storeid, $scfg, $volname, $snap) };
    if (my $err = $@) {
        my $original_state = '';
        if ($cleanup_worker) { # rename original image back
            eval { lvrename($scfg, "del-${volname}", $volname) };
            if ($@) {
                warn $@;
                # no cleanup worker is started, so the original stays available under that name
                $original_state = " (original volume kept as 'del-${volname}')";
            } else {
                $original_state = ' (original volume restored)';
            }
        }
        chomp($err);
        die "can't allocate new volume ${volname}${original_state}: $err\n";
    }

    return $cleanup_worker;
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;

    my $cleanup_worker = $class->cluster_lock_storage(
        $storeid,
        $scfg->{shared},
        undef,
        sub { volume_snapshot_rollback_locked($class, $scfg, $storeid, $volname, $snap); },
    );

    # Spawn outside of the locked section, because with 'saferemove', the cleanup worker also needs
    # to obtain the lock, and in CLI context, it will be awaited synchronously, see fork_worker().
    fork_cleanup_worker($cleanup_worker);

    return;
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap, $running) = @_;

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    die "can't delete snapshot for '$format' volume\n" if $format ne 'qcow2';

    if ($running) {
        my $cleanup_worker = eval {
            return lock_storage_with_acquire_retry(
                $class,
                $storeid,
                $scfg,
                sub {
                    return free_snap_image_locked($class, $storeid, $scfg, $volname, $snap);
                },
            );
        };
        die "error deleting snapshot $snap $@\n" if $@;
        fork_cleanup_worker($cleanup_worker);
        return;
    }

    my $cmd = "";
    my $path = $class->filesystem_path($scfg, $volname);

    $class->activate_volume($storeid, $scfg, $volname);

    my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $volname);
    my $snappath = $snapshots->{$snap}->{file};
    my $snapvolname = $snapshots->{$snap}->{volname};
    die "volume $snappath is missing" if !-e $snappath;

    my $parentsnap = $snapshots->{$snap}->{parent};

    my $childsnap = $snapshots->{$snap}->{child};
    my $childpath = $snapshots->{$childsnap}->{file};
    my $childvolname = $snapshots->{$childsnap}->{volname};

    my $err = undef;
    # if first snapshot, as it should be bigger in terms of actual data, we merge child, and rename
    # the snapshot to child
    if (!$parentsnap) {
        print "$volname: deleting snapshot '$snap' by commiting snapshot '$childsnap'\n";

        my $snap_size = $snapshots->{$snap}->{'virtual-size'};
        my $child_size = $snapshots->{$childsnap}->{'virtual-size'};
        if (defined($child_size) && defined($snap_size) && $child_size > $snap_size) {
            print "resize '$snap' ($snap_size bytes) to match '$childsnap' ($child_size bytes)\n";
            $class->volume_resize($scfg, $storeid, $volname, $child_size, $running, $snap);
        }

        print "running 'qemu-img commit $childpath'\n";
        #can't use -d here, as it's an lvm volume
        $cmd = ['/usr/bin/qemu-img', 'commit', '--', $childpath];
        eval { run_command($cmd) };
        if ($@) {
            warn
                "The state of $snap is now invalid. Don't try to clone or rollback it. You can only try to delete it again later\n";
            die "error commiting $childsnap to $snap; $@\n";
        }

        print "delete $childvolname\n";
        my $cleanup_worker = eval {
            return lock_storage_with_acquire_retry(
                $class,
                $storeid,
                $scfg,
                sub {
                    my $cleanup_worker_sub = eval {
                        free_snap_image_locked($class, $storeid, $scfg, $volname, $childsnap);
                    };
                    if ($@) {
                        die "error delete old snapshot volume $childvolname: $@\n";
                    }

                    print "rename $snapvolname to $childvolname\n";
                    eval { lvrename($scfg, $snapvolname, $childvolname) };
                    if ($@) {
                        warn $@;
                        $err = "error renaming snapshot: $@\n";
                    }

                    return $cleanup_worker_sub;
                },
            );
        };
        die "error deleting snapshot $snap: $@" if $@;
        fork_cleanup_worker($cleanup_worker);

    } else {
        #we rebase the child image on the parent as new backing image
        print
            "$volname: deleting snapshot '$snap' by rebasing '$childsnap' on top of '$parentsnap'\n";
        my $rel_parent_path = get_snap_name($class, $volname, $parentsnap);
        $cmd = [
            '/usr/bin/qemu-img',
            'rebase',
            '-b',
            $rel_parent_path,
            '-F',
            'qcow2',
            '-f',
            'qcow2',
            '--',
            $childpath,
        ];
        print "running '" . join(' ', $cmd->@*) . "'\n";
        eval { run_command($cmd) };
        if ($@) {
            #in case of abort, the state of the snap is still clean, just a little bit bigger
            die "error rebase $childsnap from $parentsnap; $@\n";
        }
        #delete the snapshot
        my $cleanup_worker = eval {
            return lock_storage_with_acquire_retry(
                $class,
                $storeid,
                $scfg,
                sub {
                    return free_snap_image_locked($class, $storeid, $scfg, $volname, $snap);
                },
            );
        };
        die "error deleting old snapshot volume $snapvolname: $@\n" if $@;
        fork_cleanup_worker($cleanup_worker);
    }

    die $err if $err;
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => {
            base => { qcow2 => 1, raw => 1 },
            current => { qcow2 => 1, raw => 1 },
            snap => { qcow2 => 1 },
        },
        'rename' => {
            current => { qcow2 => 1, raw => 1 },
        },
        snapshot => {
            current => { qcow2 => 1 },
            snap => { qcow2 => 1 },
        },
        #       fixme: add later ? (we need to handle basepath, volume activation,...)
        #       template => {
        #           current => { raw => 1, qcow2 => 1},
        #       },
        #       clone => {
        #           base => { qcow2 => 1 },
        #       },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $format) =
        $class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    } else {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if defined($features->{$feature}->{$key}->{$format});

    return undef;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    return () if defined($snapshot); # lvm-thin only
    return volume_import_formats(
        $class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots,
    );
}

sub volume_export {
    my ($class, $scfg, $storeid, $fh, $volname, $format, $snapshot, $base_snapshot, $with_snapshots)
        = @_;
    die "volume export format $format not available for $class\n"
        if $format ne 'raw+size';
    die "cannot export volumes together with their snapshots in $class\n"
        if $with_snapshots;
    die "cannot export a snapshot in $class\n" if defined($snapshot);
    die "cannot export an incremental stream in $class\n" if defined($base_snapshot);
    # Streaming a qcow2 volume as raw ships the container bytes into a raw-allocated target, an
    # unusable image; refuse it instead of emitting a corrupt stream.
    die "cannot export a qcow2-formatted volume as a raw stream in $class\n"
        if ($class->parse_volname($volname))[6] eq 'qcow2';
    my $file = $class->path($scfg, $volname, $storeid);
    my $size;
    # should be faster than querying LVM, also checks for the device file's availability
    run_command(
        ['/sbin/blockdev', '--getsize64', $file],
        outfunc => sub {
            my ($line) = @_;
            die "unexpected output from /sbin/blockdev: $line\n" if $line !~ /^(\d+)$/;
            $size = int($1);
        },
    );
    PVE::Storage::Plugin::write_common_header($fh, $size);
    run_command(
        ['dd', "if=$file", "bs=64k", "status=progress"],
        output => '>&' . fileno($fh),
        # split dd's carriage-return driven progress output into individual log lines
        errfunc => sub { print STDERR "$_[0]\n" },
    );
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;
    return () if $with_snapshots; # not supported
    return () if defined($base_snapshot); # not supported
    # refuse qcow2: only 'raw+size' is offered, which would yield an unusable raw-registered image
    return () if ($class->parse_volname($volname))[6] eq 'qcow2';
    return ('raw+size');
}

sub volume_import {
    my (
        $class,
        $scfg,
        $storeid,
        $fh,
        $volname,
        $format,
        $snapshot,
        $base_snapshot,
        $with_snapshots,
        $allow_rename,
    ) = @_;
    die "volume import format $format not available for $class\n"
        if $format ne 'raw+size';
    die "cannot import volumes together with their snapshots in $class\n"
        if $with_snapshots;
    die "cannot import an incremental stream in $class\n" if defined($base_snapshot);

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase, $file_format) =
        $class->parse_volname($volname);
    die "cannot import format $format into a file of format $file_format\n"
        if $file_format ne 'raw';

    my $allocname = $class->cluster_lock_storage(
        $storeid,
        $scfg->{shared},
        undef,
        sub {
            my $vg = $scfg->{vgname};
            my $lvs = lvm_list_volumes($vg);
            if ($lvs->{$vg}->{$volname}) {
                die "volume $vg/$volname already exists\n" if !$allow_rename;
                warn "volume $vg/$volname already exists - importing with a different name\n";
                $name = undef;
            }

            my ($size) = PVE::Storage::Plugin::read_common_header($fh);
            $size = PVE::Storage::Common::align_size_up($size, 1024) / 1024;

            return $class->alloc_image($storeid, $scfg, $vmid, 'raw', $name, $size);
        },
    );

    eval {
        my $oldname = $volname;
        $volname = $allocname;
        if (defined($name) && $allocname ne $oldname) {
            die "internal error: unexpected allocated name: '$allocname' != '$oldname'\n";
        }
        my $file = $class->path($scfg, $volname, $storeid)
            or die "internal error: failed to get path to newly allocated volume $volname\n";

        $class->volume_import_write($fh, $file);
    };
    if (my $err = $@) {
        my $cleanup_worker = eval {
            return $class->cluster_lock_storage(
                $storeid,
                $scfg->{shared},
                undef,
                sub { return $class->free_image($storeid, $scfg, $volname, 0); },
            );
        };
        warn $@ if $@;
        fork_cleanup_worker($cleanup_worker);
        die $err;
    }

    return "$storeid:$volname";
}

sub volume_import_write {
    my ($class, $input_fh, $output_file) = @_;
    run_command(['dd', "of=$output_file", 'bs=64k'], input => '<&' . fileno($input_fh));
}

sub rename_volume {
    my ($class, $scfg, $storeid, $source_volname, $target_vmid, $target_volname) = @_;

    my (
        undef, $source_image, $source_vmid, $base_name, $base_vmid, undef, $format,
    ) = $class->parse_volname($source_volname);

    if ($format eq 'qcow2') {
        $class->activate_volume($storeid, $scfg, $source_volname);
        my $snapshots = $class->volume_snapshot_info($scfg, $storeid, $source_volname);
        die "can't rename volume '$source_volname' - external snapshot exists\n"
            if $snapshots->{current}->{parent};
    }

    $target_volname = $class->find_free_diskname($storeid, $scfg, $target_vmid, $format)
        if !$target_volname;

    $target_volname = $class->volname_for_format($target_volname, $format, 0);

    my $vg = $scfg->{vgname};
    my $lvs = lvm_list_volumes($vg);
    die "target volume '${target_volname}' already exists\n"
        if ($lvs->{$vg}->{$target_volname});

    lvrename($scfg, $source_volname, $target_volname);

    eval {
        my $tag_opts = [];
        if ($source_vmid ne $target_vmid) {
            push $tag_opts->@*, '--addtag', "pve-vm-${target_vmid}";
            push $tag_opts->@*, '--deltag', "pve-vm-${source_vmid}";
        }
        if ($format eq 'qcow2') {
            push $tag_opts->@*, '--addtag', "pve-$target_volname";
            push $tag_opts->@*, '--deltag', "pve-$source_volname";
        }
        run_command(['lvchange', $tag_opts->@*, "${vg}/${target_volname}"])
            if scalar($tag_opts->@*);
    };
    warn "unable to update tags for '$target_volname' - $@" if $@;

    return "${storeid}:${target_volname}";
}

sub rename_snapshot {
    my ($class, $scfg, $storeid, $volname, $source_snap, $target_snap) = @_;

    my $source_snap_volname = get_snap_name($class, $volname, $source_snap);
    my $target_snap_volname = get_snap_name($class, $volname, $target_snap);

    lvrename($scfg, $source_snap_volname, $target_snap_volname);
}

sub volume_qemu_snapshot_method {
    my ($class, $storeid, $scfg, $volname) = @_;

    my $format = ($class->parse_volname($volname))[6];
    return 'mixed' if $format eq 'qcow2';
    return 'storage';
}

1;
