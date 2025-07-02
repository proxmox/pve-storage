package PVE::Storage::ISCSIDirectPlugin;

use strict;
use warnings;

use IO::File;
use JSON qw(decode_json);
use HTTP::Request;
use LWP::UserAgent;

use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use base qw(PVE::Storage::Plugin);

sub iscsi_ls {
    my ($scfg) = @_;

    my $portal = $scfg->{portal};
    my $cmd = ['/usr/bin/iscsi-ls', '-s', 'iscsi://' . $portal];
    my $list = {};
    my %unittobytes = (
        "k" => 1024,
        "M" => 1024 * 1024,
        "G" => 1024 * 1024 * 1024,
        "T" => 1024 * 1024 * 1024 * 1024,
    );
    eval {
        run_command(
            $cmd,
            errmsg => "iscsi error",
            errfunc => sub { },
            outfunc => sub {
                my $line = shift;
                $line = trim($line);
                if ($line =~ /Lun:(\d+)\s+([A-Za-z0-9\-\_\.\:]*)\s+\(Size:([0-9\.]*)(k|M|G|T)\)/
                ) {
                    my $image = "lun" . $1;
                    my $size = $3;
                    my $unit = $4;

                    $list->{$image} = {
                        name => $image,
                        size => $size * $unittobytes{$unit},
                        format => 'raw',
                    };
                }
            },
        );
    };

    my $err = $@;
    die $err if $err && $err !~ m/TESTUNITREADY failed with SENSE KEY/;

    return $list;
}

# Configuration

sub type {
    return 'iscsidirect';
}

sub plugindata {
    return {
        content => [{ images => 1, none => 1 }, { images => 1 }],
        select_existing => 1,
        'sensitive-properties' => {},
    };
}

sub options {
    return {
        portal => { fixed => 1 },
        target => { fixed => 1 },
        nodes => { optional => 1 },
        disable => { optional => 1 },
        content => { optional => 1 },
        bwlimit => { optional => 1 },
    };
}

# Storage implementation

sub parse_volname {
    my ($class, $volname) = @_;

    if ($volname =~ m/^lun(\d+)$/) {
        return ('images', $1, undef, undef, undef, undef, 'raw');
    }

    die "unable to parse iscsi volume name '$volname'\n";

}

sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;

    die "volume snapshot is not possible on iscsi device\n"
        if defined($snapname);

    my ($vtype, $lun, $vmid) = $class->parse_volname($volname);

    my $target = $scfg->{target};
    my $portal = $scfg->{portal};

    my $path = "iscsi://$portal/$target/$lun";

    return ($path, $vmid, $vtype);
}

sub qemu_blockdev_options {
    my ($class, $scfg, $storeid, $volname, $machine_version, $options) = @_;

    die "volume snapshot is not possible on iscsi device\n"
        if $options->{'snapshot-name'};

    my $lun = ($class->parse_volname($volname))[1];

    return {
        driver => 'iscsi',
        transport => 'tcp',
        portal => "$scfg->{portal}",
        target => "$scfg->{target}",
        lun => int($lun),
    };
}

sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    die "can't create base images in iscsi storage\n";
}

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;

    die "can't clone images in iscsi storage\n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "can't allocate space in iscsi storage\n";
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase) = @_;

    die "can't free space in iscsi storage\n";
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $res = [];

    # we have no owner for iscsi devices

    my $dat = iscsi_ls($scfg);
    foreach my $volname (keys %$dat) {
        my $volid = "$storeid:$volname";

        if ($vollist) {
            my $found = grep { $_ eq $volid } @$vollist;
            next if !$found;
        } else {
            # we have no owner for iscsi devices
            next if defined($vmid);
        }

        my $info = $dat->{$volname};
        $info->{volid} = $volid;

        push @$res, $info;
    }

    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    my $total = 0;
    my $free = 0;
    my $used = 0;
    my $active = 1;
    return ($total, $free, $used, $active);

    return undef;
}

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "volume snapshot is not possible on iscsi device\n" if $snapname;

    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;

    die "volume snapshot is not possible on iscsi device\n" if $snapname;

    return 1;
}

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;

    my $vollist = iscsi_ls($scfg);
    my $info = $vollist->{$volname};

    return wantarray ? ($info->{size}, 'raw', 0, undef) : $info->{size};
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    die "volume resize is not possible on iscsi device\n";
}

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot is not possible on iscsi device\n";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot rollback is not possible on iscsi device\n";
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    die "volume snapshot delete is not possible on iscsi device\n";
}

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        copy => { current => 1 },
    };

    my ($vtype, $name, $vmid, $basename, $basevmid, $isBase) = $class->parse_volname($volname);

    my $key = undef;
    if ($snapname) {
        $key = 'snap';
    } else {
        $key = $isBase ? 'base' : 'current';
    }
    return 1 if $features->{$feature}->{$key};

    return undef;
}

sub volume_export_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    return () if defined($snapshot); # not supported
    return () if defined($base_snapshot); # not supported
    return () if $with_snapshots; # not supported
    return ('raw+size');
}

sub volume_export {
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
    ) = @_;

    die "volume export format $format not available for $class\n" if $format ne 'raw+size';
    die "cannot export volumes together with their snapshots in $class\n" if $with_snapshots;
    die "cannot export an incremental stream in $class\n" if defined($base_snapshot);
    die "cannot export a snapshot in $class\n" if defined($snapshot);

    my ($file) = $class->path($scfg, $volname, $storeid, $snapshot);

    my $json = '';
    run_command(
        ['/usr/bin/qemu-img', 'info', '-f', 'raw', '--output=json', $file],
        outfunc => sub { $json .= shift },
    );
    die "failed to query size information for '$file' with qemu-img\n" if !$json;
    my $info = eval { decode_json($json) };
    die "could not parse qemu-img info command output for '$file' - $@\n" if $@;

    my ($size) = ($info->{'virtual-size'} =~ /^(\d+)$/); # untaint
    die "size '$size' not an integer\n" if !defined($size);
    $size = int($size); # coerce back from string

    PVE::Storage::Plugin::write_common_header($fh, $size);
    run_command(
        ['qemu-img', 'dd', 'bs=64k', "if=$file", '-f', 'raw', '-O', 'raw'],
        output => '>&' . fileno($fh),
    );
    return;
}

sub volume_import_formats {
    my ($class, $scfg, $storeid, $volname, $snapshot, $base_snapshot, $with_snapshots) = @_;

    return ();
}

sub volume_import {
    die "volume import is not possible on iscsi storage\n";
}

1;
