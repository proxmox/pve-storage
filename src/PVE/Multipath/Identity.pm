package PVE::Multipath::Identity;

use v5.36;

use PVE::Tools qw(dir_glob_foreach file_get_contents file_read_firstline);

use PVE::ISCSI::Topology;
use PVE::Multipath;
use PVE::Multipath::Config;

our $SYSFS = '/sys';
our $UDEV_DATA = '/run/udev/data';

sub collect(%param) {
    my $devices = PVE::ISCSI::Topology::device_map($SYSFS);
    my $daemon = eval { PVE::Multipath::query_path_identities() };
    my %paths;
    dir_glob_foreach(
        $SYSFS . '/class/block',
        'sd[a-z]+',
        sub($dev, @) {
            my $session = $devices->{$dev};
            return if !$param{all} && (!$session || !$param{targets}->{ $session->{target} });
            my $wwid = $daemon ? $daemon->{$dev}->{wwid} : undef;
            # multipathd does not list every disk, for example not the ones it has blacklisted
            if (!defined($wwid)) {
                my $devt = file_read_firstline("$SYSFS/class/block/$dev/dev") // return;
                my $data = eval { file_get_contents("$UDEV_DATA/b$devt") } // '';
                ($wwid) = $data =~ m/^E:ID_SERIAL=(.+)$/m;
            }
            return if !defined($wwid) || $wwid !~ $PVE::Multipath::Config::WWID_RE;
            $paths{$dev} = { wwid => $wwid };
        },
    );
    return {
        paths => \%paths,
    };
}

sub path_wwid($ids, $name) {
    return $ids->{paths}->{$name}->{wwid};
}

1;
