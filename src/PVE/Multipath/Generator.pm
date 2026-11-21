package PVE::Multipath::Generator;

use v5.36;

use File::Path qw(make_path);

use PVE::INotify;
use PVE::Tools qw(file_get_contents file_set_contents);

use PVE::Multipath;
use PVE::Multipath::Config;
use PVE::Multipath::Identity;

# Local drop-ins let multipathd start before pmxcfs is available.
our $DEFAULTS_DROPIN = '/etc/multipath/conf.d/pve-defaults.conf';
our $OVERRIDES_DROPIN = '/etc/multipath/conf.d/pve-overrides.conf';
our $ALIASES_DROPIN = '/etc/multipath/conf.d/pve-aliases.conf';
our $RECONFIGURE_PENDING = '/run/pve-multipath.reconfigure-pending';
my ($failed_at, $failed_error);
my %refused; # WWID => [time, error] of the last refusal

sub derived_wwids($cfg, $ids = undef) {
    require PVE::Storage;
    my @storages = grep {
        my $scfg = $cfg->{ids}->{$_};
        $scfg->{type} eq 'iscsi'
            && $scfg->{'managed-multipath'}
            && PVE::Storage::storage_check_enabled($cfg, $_, undef, 1)
    } sort keys $cfg->{ids}->%*;
    if (!@storages) {
        return {};
    }
    $ids //= PVE::Multipath::Identity::collect(
        targets => { map { $cfg->{ids}->{$_}->{target} => 1 } @storages });
    return { map { $_ => PVE::Storage::ISCSIPlugin->multipath_lun_wwids($cfg->{ids}->{$_}, $ids) }
        @storages };
}

my sub pending() { file_set_contents($RECONFIGURE_PENDING, '') if !-e $RECONFIGURE_PENDING }
my sub dropin_changes($path, $content) {
    my $old = -e $path ? file_get_contents($path) : '';
    return 0 if $old eq $content;
    # a file of this name that an administrator wrote stays
    if (length($old) && $old !~ m/\A# Managed by Proxmox VE\.\n/) {
        die "cannot replace '$path' - it was not written by Proxmox VE, rename it\n"
            if length($content);
        return 0;
    }
    return 1;
}
my sub write_dropin($path, $content) {
    pending();
    if (length($content)) {
        make_path($path =~ s!/[^/]+$!!r);
        file_set_contents($path, $content);
    } else {
        unlink($path) or die "cannot remove '$path' - $!\n";
    }
}

# The WWIDs of the explicit entries for the node and of the storages, with the storages that request
# each and the highest of the expected path counts of the entry and the storages.
sub wanted_wwids($cfg, $node, $derived) {
    my %wanted = map {
        my $count = $cfg->{ids}->{$_}->{'paths-expected'};
        $_ => defined($count) ? { 'paths-expected' => $count } : {}
    } PVE::Multipath::Config::wwids_for_node($cfg, $node)->@*;
    for my $sid (sort keys %$derived) {
        for my $wwid (sort keys $derived->{$sid}->{wwids}->%*) {
            push $wanted{$wwid}->{storages}->@*, $sid;
            my $expected = $derived->{$sid}->{wwids}->{$wwid}->{'paths-expected'};
            $wanted{$wwid}->{'paths-expected'} = $expected
                if defined($expected) && $expected > ($wanted{$wwid}->{'paths-expected'} // 0);
        }
    }
    return \%wanted;
}

sub regenerate($cfg = undef, $overrides = undef, $derived = undef) {
    $cfg //= PVE::Multipath::Config::read_config();
    $overrides //= PVE::Multipath::Config::read_overrides();
    $derived //= {};
    my $node = PVE::INotify::nodename();
    my @explicit = PVE::Multipath::Config::wwids_for_node($cfg, $node)->@*;
    my %wanted = wanted_wwids($cfg, $node, $derived)->%*;
    my @added;
    eval {
        # a hand edit of the cluster file has not passed the check of the API
        PVE::Multipath::Config::check_overrides($overrides);
        my $in_use = @explicit || keys(%$derived) || length($overrides);
        my @dropins = (
            [$DEFAULTS_DROPIN, $in_use ? PVE::Multipath::Config::generate_managed_conf() : ''],
            [
                $OVERRIDES_DROPIN,
                length($overrides) ? "# Managed by Proxmox VE.\n$overrides" : '',
            ],
            [
                $ALIASES_DROPIN,
                @explicit ? PVE::Multipath::Config::build_multipaths_block($cfg, $node) : '',
            ],
        );
        # check every drop-in before one changes, as a restart of multipathd loads what is there
        my @changes = grep { dropin_changes($_->@*) } @dropins;
        write_dropin($_->@*) for @changes;
        # A WWID that multipath refuses, for example a blacklisted one, must not hold up the others
        # or the reload, nor cause a reload on every pass. Retry it after five minutes, or after a
        # drop-in changed, which can lift the refusal, as it can fix a failed reload.
        if (@changes) {
            %refused = ();
            $failed_at = undef;
        }
        my %current = map { $_ => 1 } PVE::Multipath::list_etc_multipath_wwids()->@*;
        delete @refused{ grep { !$wanted{$_} || $current{$_} } keys %refused };
        my @failed;
        for my $wwid (sort keys %wanted) {
            next if $current{$wwid};
            my $age = $refused{$wwid} ? time() - $refused{$wwid}->[0] : undef;
            if (defined($age) && $age >= 0 && $age < 300) {
                push @failed, $refused{$wwid}->[1];
                next;
            }
            eval { PVE::Multipath::add_wwid($wwid) };
            if (my $err = $@) {
                # if multipath hangs for one WWID, it would for the next one as well, so stop here
                # and report the timeout first, where the summary of refused WWIDs cannot hide it
                if ($err =~ /got timeout/) {
                    unshift(@failed, $err);
                    last;
                }
                push @failed, $err;
                $refused{$wwid} = [time(), $err];
                next;
            }
            pending();
            push @added, $wwid;
        }
        # multipath also takes the name of a map, adds the WWID of that map instead and succeeds
        if (@added) {
            my %listed = map { $_ => 1 } PVE::Multipath::list_etc_multipath_wwids()->@*;
            for my $wwid (grep { !$listed{$_} } @added) {
                my $err = "multipath did not list '$wwid' after adding it - it is probably the"
                    . " name of a map, not a WWID\n";
                push @failed, $err;
                $refused{$wwid} = [time(), $err];
            }
            @added = grep { $listed{$_} } @added;
        }
        if (-e $RECONFIGURE_PENDING) {
            die "multipath reload pending after a failed reload - $failed_error\n"
                if defined($failed_at) && time() >= $failed_at && time() - $failed_at < 60;
            eval { PVE::Multipath::reconfigure() };
            if (my $error = $@) {
                $failed_at = time();
                chomp($failed_error = $error);
                die $error;
            }
            unlink($RECONFIGURE_PENDING) or die "cannot clear multipath reload marker - $!\n";
            $failed_at = undef;
        }
        if (@failed > 4) {
            my $more = @failed - 3;
            splice(@failed, 3, $more, "and $more more WWIDs could not be added\n");
        }
        die join('', @failed) if @failed;
    };
    my $error = $@;
    return { wwids => \%wanted, added => \@added, $error ? (error => $error) : () };
}

sub sync() {
    return {} if !PVE::Multipath::is_supported();
    my $res = eval {
        require PVE::Storage;
        regenerate(undef, undef, derived_wwids(PVE::Storage::config()));
    };
    return $res // { error => "$@" };
}

1;
