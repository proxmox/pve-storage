package PVE::ISCSI::Topology;

use v5.36;

use PVE::Tools qw(dir_glob_foreach dir_glob_regex file_read_firstline);

our $SYSFS = '/sys';

# Offload initiators can run several sessions on one SCSI host, so the LUNs of a session are the
# ones below its own target, not all LUNs of its host.
sub foreach_session_lun($sessiondir, $code) {
    dir_glob_foreach(
        $sessiondir,
        'target\d+:\d+:\d+',
        sub($targetdir) {
            dir_glob_foreach(
                "$sessiondir/$targetdir",
                '\d+:(\d+):(\d+):(\d+)',
                sub($hctl, $channel, $id, $lun) {
                    $code->("$sessiondir/$targetdir/$hctl", $channel, $id, $lun);
                },
            );
        },
    );
}

sub sessions($sysfs = undef) {
    $sysfs //= $SYSFS;
    my $res = {};
    dir_glob_foreach(
        "$sysfs/class/iscsi_session",
        'session(\d+)',
        sub($ent, $sid) {
            my $dir = "$sysfs/class/iscsi_session/$ent";
            my $target = file_read_firstline("$dir/targetname") // return;
            my $session = {
                target => $target,
                state => file_read_firstline("$dir/state") // '',
                portal => '',
                devices => {},
            };
            my (undef, $conn) = dir_glob_regex("$dir/device", '(connection\d+:\d+)');
            if (defined($conn)) {
                my $cdir = "$dir/device/$conn/iscsi_connection/$conn";
                my $address = file_read_firstline("$cdir/persistent_address") // '';
                my $port = file_read_firstline("$cdir/persistent_port") // '';
                $address = "[$address]" if $address =~ m/:/;
                $session->{portal} = normalize_portal("$address:$port");
            }
            foreach_session_lun(
                "$dir/device",
                sub($devdir, $channel, $id, $lun) {
                    return if (file_read_firstline("$devdir/type") // '') ne '0';
                    my (undef, $dev) = dir_glob_regex("$devdir/block/", '([A-Za-z]\S*)');
                    return if !defined($dev);
                    $session->{devices}->{$dev} = {
                        target => $target,
                        session => int($sid),
                        channel => int($channel),
                        id => int($id),
                        lun => int($lun),
                    };
                },
            );
            $res->{$sid} = $session;
        },
    );
    return $res;
}

sub device_map($sysfs = undef) {
    my $res = {};
    for my $session (values %{ sessions($sysfs) }) {
        $res->{$_} = $session->{devices}->{$_} for keys $session->{devices}->%*;
    }
    return $res;
}

sub normalize_portal($portal) {
    return $portal if $portal !~ m/^(.+):(\d+)$/;
    my ($addr, $port) = ($1, $2);
    $addr =~ s/^\[(.*)\]$/$1/;
    $addr =~ s/^::ffff:(\d+\.\d+\.\d+\.\d+)$/$1/i;
    $addr = "[$addr]" if $addr =~ m/:/;
    return "$addr:$port";
}

1;
