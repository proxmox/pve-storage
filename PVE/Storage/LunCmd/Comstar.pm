package PVE::Storage::LunCmd::Comstar;

use strict;
use warnings;
use Digest::MD5 qw(md5_hex);
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use Data::Dumper;

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';

my $get_lun_cmd_map = sub {
    my ($method) = @_;

    my $stmfadmcmd = "/usr/sbin/stmfadm";
    my $sbdadmcmd = "/usr/sbin/sbdadm";

    my $cmdmap = {
        create_lu   => { cmd => $stmfadmcmd, method => 'create-lu' },
        delete_lu   => { cmd => $stmfadmcmd, method => 'delete-lu' },
        import_lu   => { cmd => $stmfadmcmd, method => 'import-lu' },
        modify_lu   => { cmd => $stmfadmcmd, method => 'modify-lu' },
        add_view    => { cmd => $stmfadmcmd, method => 'add-view' },
        list_view   => { cmd => $stmfadmcmd, method => 'list-view' },
        list_lu => { cmd => $sbdadmcmd, method => 'list-lu' },
    };

    die "unknown command '$method'" unless exists $cmdmap->{$method};

    return $cmdmap->{$method};
};

sub get_base {
    return '/dev/zvol/rdsk';
}

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    my $msg = '';
    my $luncmd;
    my $target;
    my $guid;
    $timeout = 10 if !$timeout;

    my $output = sub {
    my $line = shift;
    $msg .= "$line\n";
    };

    if ($method eq 'create_lu') {
        my $wcd = 'false'; 
        if ($scfg->{nowritecache}) {
          $wcd = 'true';
	}
        my $prefix = '600144f';
        my $digest = md5_hex($params[0]);
        $digest =~ /(\w{7}(.*))/;
        $guid = "$prefix$2";
        @params = ('-p', "wcd=$wcd", '-p', "guid=$guid", @params);
    } elsif ($method eq 'modify_lu') {
        @params = ('-s', @params);
    } elsif ($method eq 'list_view') {
        @params = ('-l', @params);
    } elsif ($method eq 'list_lu') {
        $guid = $params[0];
        @params = undef;
    } elsif ($method eq 'add_view') {
        if ($scfg->{comstar_tg}) {
          unshift @params, $scfg->{comstar_tg};
          unshift @params, '--target-group';
	}
        if ($scfg->{comstar_hg}) {
          unshift @params, $scfg->{comstar_hg};
          unshift @params, '--host-group';
	}
    }

    my $cmdmap = $get_lun_cmd_map->($method);
    $luncmd = $cmdmap->{cmd};
    my $lunmethod = $cmdmap->{method};

    $target = 'root@' . $scfg->{portal};

    my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $luncmd, $lunmethod, @params];

    run_command($cmd, outfunc => $output, timeout => $timeout);

    if ($method eq 'list_view') {
        my @lines = split /\n/, $msg;
        $msg = undef;
        foreach my $line (@lines) {
            if ($line =~ /^\s*LUN\s*:\s*(\d+)$/) {
                $msg = $1;
                last;
            }
        }
    } elsif ($method eq 'list_lu') {
        my $object = $guid;
        my @lines = split /\n/, $msg;
        $msg = undef;
        foreach my $line (@lines) {
            if ($line =~ /(\w+)\s+\d+\s+$object$/) {
                $msg = $1;
                last;
            }
        }
    } elsif ($method eq 'create_lu') {
        $msg = $guid;
    }

    return $msg;
}

