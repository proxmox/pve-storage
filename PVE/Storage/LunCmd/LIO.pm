package PVE::Storage::LunCmd::LIO;

# lightly based on code from Iet.pm
#
# additional changes:
# -----------------------------------------------------------------
# Copyright (c) 2018 BestSolution.at EDV Systemhaus GmbH
# All Rights Reserved.
#
# This software is released under the terms of the
#
#            "GNU Affero General Public License"
#
# and may only be distributed and used under the terms of the
# mentioned license. You should have received a copy of the license
# along with this software product, if not you can download it from
# https://www.gnu.org/licenses/agpl-3.0.en.html
#
# Author: udo.rader@bestsolution.at
# -----------------------------------------------------------------

use strict;
use warnings;
use PVE::Tools qw(run_command);
use JSON;

sub get_base;

# targetcli constants
# config file location differs from distro to distro
my @CONFIG_FILES = (
	'/etc/rtslib-fb-target/saveconfig.json',	# Debian 9.x et al
	'/etc/target/saveconfig.json' ,			# ArchLinux, CentOS
);
my $BACKSTORE = '/backstores/block';

my $SETTINGS = undef;
my $SETTINGS_TIMESTAMP = 0;
my $SETTINGS_MAXAGE = 15; # in seconds

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';
my $targetcli = '/usr/bin/targetcli';

my $execute_remote_command = sub {
    my ($scfg, $timeout, $remote_command, @params) = @_;

    my $msg = '';
    my $err = undef;
    my $target;
    my $cmd;
    my $res = ();

    $timeout = 10 if !$timeout;

    my $output = sub { $msg .= "$_[0]\n" };
    my $errfunc = sub { $err .= "$_[0]\n" };

    $target = 'root@' . $scfg->{portal};
    $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, '--', $remote_command, @params];

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

# fetch targetcli configuration from the portal
my $read_config = sub {
    my ($scfg, $timeout) = @_;

    my $msg = '';
    my $err = undef;
    my $luncmd = 'cat';
    my $target;
    my $retry = 1;

    $timeout = 10 if !$timeout;

    my $output = sub { $msg .= "$_[0]\n" };
    my $errfunc = sub { $err .= "$_[0]\n" };

    $target = 'root@' . $scfg->{portal};

    foreach my $oneFile (@CONFIG_FILES) {
	my $cmd = [@ssh_cmd, '-i', "$id_rsa_path/$scfg->{portal}_id_rsa", $target, $luncmd, $oneFile];
	eval {
	    run_command($cmd, outfunc => $output, errfunc => $errfunc, timeout => $timeout);
	};
	if ($@) {
	    die $err if ($err !~ /No such file or directory/);
	}
	return $msg if $msg ne '';
    }

    die "No configuration found. Install targetcli on $scfg->{portal}\n" if $msg eq '';

    return $msg;
};

my $get_config = sub {
    my ($scfg) = @_;
    my @conf = undef;

    my $config = $read_config->($scfg, undef);
    die "Missing config file" unless $config;

    return $config;
};

# fetches and parses targetcli config from the portal
my $parser = sub {
    my ($scfg) = @_;
    my $tpg = $scfg->{lio_tpg} || die "Target Portal Group not set, aborting!\n";
    my $tpg_tag;

    if ($tpg =~ /^tpg(\d+)$/) {
	$tpg_tag = $1;
    } else {
	die "Target Portal Group has invalid value, must contain string 'tpg' and a suffix number, eg 'tpg17'\n";
    }

    my $base = get_base;

    my $config = $get_config->($scfg);
    my $jsonconfig = JSON->new->utf8->decode($config);

    my $haveTarget = 0;
    foreach my $target (@{$jsonconfig->{targets}}) {
	# only interested in iSCSI targets
	next if !($target->{fabric} eq 'iscsi' && $target->{wwn} eq $scfg->{target});
	# find correct TPG
	foreach my $tpg (@{$target->{tpgs}}) {
	    if ($tpg->{tag} == $tpg_tag) {
		$SETTINGS->{target} = $tpg;
		$haveTarget = 1;
		last;
	    }
	}
    }

    # seriously unhappy if the target server lacks iSCSI target configuration ...
    if (!$haveTarget) {
	die "target portal group tpg$tpg_tag not found!\n";
    }
};

# removes the given lu_name from the local list of luns
my $free_lu_name = sub {
    my ($lu_name) = @_;

    my $new = [];
    foreach my $lun (@{$SETTINGS->{target}->{luns}}) {
	if ($lun->{storage_object} ne "$BACKSTORE/$lu_name") {
	    push @$new, $lun;
	}
    }

    $SETTINGS->{target}->{luns} = $new;
};

# locally registers a new lun
my $register_lun = sub {
    my ($scfg, $idx, $volname) = @_;

    my $conf = {
	index => $idx,
	storage_object => "$BACKSTORE/$volname",
	is_new => 1,
    };
    push @{$SETTINGS->{target}->{luns}}, $conf;

    return $conf;
};

# extracts the ZFS volume name from a device path
my $extract_volname = sub {
    my ($scfg, $lunpath) = @_;
    my $volname = undef;

    my $base = get_base;
    if ($lunpath =~ /^$base\/$scfg->{pool}\/([\w\-]+)$/) {
	$volname = $1;
    }

    return $volname;
};

# retrieves the LUN index for a particular object
my $list_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $lun = undef;

    my $object = $params[0];
    my $volname = $extract_volname->($scfg, $params[0]);

    foreach my $lun (@{$SETTINGS->{target}->{luns}}) {
	if ($lun->{storage_object} eq "$BACKSTORE/$volname") {
	    return $lun->{index};
	}
    }

    return $lun;
};

# determines, if the given object exists on the portal
my $list_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $name = undef;

    my $object = $params[0];
    my $volname = $extract_volname->($scfg, $params[0]);

    foreach my $lun (@{$SETTINGS->{target}->{luns}}) {
	if ($lun->{storage_object} eq "$BACKSTORE/$volname") {
	    return $object;
	}
    }

    return $name;
};

# adds a new LUN to the target
my $create_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    if ($list_lun->($scfg, $timeout, $method, @params)) {
	die "$params[0]: LUN already exists!";
    }

    my $device = $params[0];
    my $volname = $extract_volname->($scfg, $device);
    my $tpg = $scfg->{lio_tpg} || die "Target Portal Group not set, aborting!\n";

    # step 1: create backstore for device
    my @cliparams = ($BACKSTORE, 'create', "name=$volname", "dev=$device" );
    my $res = $execute_remote_command->($scfg, $timeout, $targetcli, @cliparams);
    die $res->{msg} if !$res->{result};

    # step 2: register lun with target
    # targetcli /iscsi/iqn.2018-04.at.bestsolution.somehost:target/tpg1/luns/ create /backstores/block/foobar
    @cliparams = ("/iscsi/$scfg->{target}/$tpg/luns/", 'create', "$BACKSTORE/$volname" );
    $res = $execute_remote_command->($scfg, $timeout, $targetcli, @cliparams);
    die $res->{msg} if !$res->{result};

    # targetcli responds with "Created LUN 99"
    # not calculating the index ourselves, because the index at the portal might have
    # changed without our knowledge, so relying on the number that targetcli returns
    my $lun_idx;
    if ($res->{msg} =~ /LUN (\d+)/) {
	$lun_idx = $1;
    } else {
	die "unable to determine new LUN index: $res->{msg}";
    }

    $register_lun->($scfg, $lun_idx, $volname);

    # step 3: unfortunately, targetcli doesn't always save changes, no matter
    #         if auto_save_on_exit is true or not. So saving to be safe ...
    $execute_remote_command->($scfg, $timeout, $targetcli, 'saveconfig');

    return $res->{msg};
};

my $delete_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $res = {msg => undef};

    my $tpg = $scfg->{lio_tpg} || die "Target Portal Group not set, aborting!\n";

    my $path = $params[0];
    my $volname = $extract_volname->($scfg, $params[0]);

    foreach my $lun (@{$SETTINGS->{target}->{luns}}) {
	next if $lun->{storage_object} ne "$BACKSTORE/$volname";

	# step 1: delete the lun
	my @cliparams = ("/iscsi/$scfg->{target}/$tpg/luns/", 'delete', "lun$lun->{index}" );
	my $res = $execute_remote_command->($scfg, $timeout, $targetcli, @cliparams);
	do {
	    die $res->{msg};
	} unless $res->{result};

	# step 2: delete the backstore
	@cliparams = ($BACKSTORE, 'delete', $volname);
	$res = $execute_remote_command->($scfg, $timeout, $targetcli, @cliparams);
	do {
	    die $res->{msg};
	} unless $res->{result};

	# step 3: save to be safe ...
	$execute_remote_command->($scfg, $timeout, $targetcli, 'saveconfig');

	# update interal cache
	$free_lu_name->($volname);

	last;
    }

    return $res->{msg};
};

my $import_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    return $create_lun->($scfg, $timeout, $method, @params);
};

# needed for example when the underlying ZFS volume has been resized
my $modify_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $msg;

    $msg = $delete_lun->($scfg, $timeout, $method, @params);
    if ($msg) {
	$msg = $create_lun->($scfg, $timeout, $method, @params);
    }

    return $msg;
};

my $add_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    return '';
};

my %lun_cmd_map = (
    create_lu   =>  $create_lun,
    delete_lu   =>  $delete_lun,
    import_lu   =>  $import_lun,
    modify_lu   =>  $modify_lun,
    add_view    =>  $add_view,
    list_view   =>  $list_view,
    list_lu     =>  $list_lun,
);

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    # fetch configuration from target if we haven't yet or if it is stale
    my $timediff = time - $SETTINGS_TIMESTAMP;
    if (!$SETTINGS || $timediff > $SETTINGS_MAXAGE) {
	$SETTINGS_TIMESTAMP = time;
	$parser->($scfg);
    }

    die "unknown command '$method'" unless exists $lun_cmd_map{$method};
    my $msg = $lun_cmd_map{$method}->($scfg, $timeout, $method, @params);

    return $msg;
}

sub get_base {
    return '/dev';
}

1;
