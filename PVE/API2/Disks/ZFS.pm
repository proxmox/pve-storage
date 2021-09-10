package PVE::API2::Disks::ZFS;

use strict;
use warnings;

use PVE::Diskmanage;
use PVE::JSONSchema qw(get_standard_option);
use PVE::Systemd;
use PVE::API2::Storage::Config;
use PVE::Storage;
use PVE::Tools qw(run_command lock_file trim);

use PVE::RPCEnvironment;
use PVE::RESTHandler;

use base qw(PVE::RESTHandler);

my $ZPOOL = '/sbin/zpool';
my $ZFS = '/sbin/zfs';

__PACKAGE__->register_method ({
    name => 'index',
    path => '',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Audit', 'Datastore.Audit'], any => 1],
    },
    description => "List Zpools.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	},
    },
    returns => {
	type => 'array',
	items => {
	    type => 'object',
	    properties => {
		name => {
		    type => 'string',
		    description => "",
		},
		size => {
		    type => 'integer',
		    description => "",
		},
		alloc => {
		    type => 'integer',
		    description => "",
		},
		free => {
		    type => 'integer',
		    description => "",
		},
		frag => {
		    type => 'integer',
		    description => "",
		},
		dedup => {
		    type => 'number',
		    description => "",
		},
		health => {
		    type => 'string',
		    description => "",
		},
	    },
	},
	links => [ { rel => 'child', href => "{name}" } ],
    },
    code => sub {
	my ($param) = @_;

	if (!-f $ZPOOL) {
	    die "zfsutils-linux not installed\n";
	}

	my $propnames = [qw(name size alloc free frag dedup health)];
	my $numbers = {
	    size => 1,
	    alloc => 1,
	    free => 1,
	    frag => 1,
	    dedup => 1,
	};

	my $cmd = [$ZPOOL,'list', '-HpPLo', join(',', @$propnames)];

	my $pools = [];

	run_command($cmd, outfunc => sub {
	    my ($line) = @_;

		my @props = split('\s+', trim($line));
		my $pool = {};
		for (my $i = 0; $i < scalar(@$propnames); $i++) {
		    if ($numbers->{$propnames->[$i]}) {
			$pool->{$propnames->[$i]} = $props[$i] + 0;
		    } else {
			$pool->{$propnames->[$i]} = $props[$i];
		    }
		}

		push @$pools, $pool;
	});

	return $pools;
    }});

sub preparetree {
    my ($el) = @_;
    delete $el->{lvl};
    if ($el->{children} && scalar(@{$el->{children}})) {
	$el->{leaf} = 0;
	foreach my $child (@{$el->{children}}) {
	    preparetree($child);
	}
    } else {
	$el->{leaf} = 1;
    }
}


__PACKAGE__->register_method ({
    name => 'detail',
    path => '{name}',
    method => 'GET',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Audit', 'Datastore.Audit'], any => 1],
    },
    description => "Get details about a zpool.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    name => get_standard_option('pve-storage-id'),
	},
    },
    returns => {
	type => 'object',
	properties => {
	    name => {
		type => 'string',
		description => 'The name of the zpool.',
	    },
	    state => {
		type => 'string',
		description => 'The state of the zpool.',
	    },
	    status => {
		optional => 1,
		type => 'string',
		description => 'Information about the state of the zpool.',
	    },
	    action => {
		optional => 1,
		type => 'string',
		description => 'Information about the recommended action to fix the state.',
	    },
	    scan => {
		optional => 1,
		type => 'string',
		description => 'Information about the last/current scrub.',
	    },
	    errors => {
		type => 'string',
		description => 'Information about the errors on the zpool.',
	    },
	    children => {
		type => 'array',
		description => "The pool configuration information, including the vdevs for each section (e.g. spares, cache), may be nested.",
		items => {
		    type => 'object',
		    properties => {
			name => {
			    type => 'string',
			    description => 'The name of the vdev or section.',
			},
			state => {
			    optional => 1,
			    type => 'string',
			    description => 'The state of the vdev.',
			},
			read => {
			    optional => 1,
			    type => 'number',
			},
			write => {
			    optional => 1,
			    type => 'number',
			},
			cksum => {
			    optional => 1,
			    type => 'number',
			},
			msg => {
			    type => 'string',
			    description => 'An optional message about the vdev.'
			}
		    },
		},
	    },
	},
    },
    code => sub {
	my ($param) = @_;

	if (!-f $ZPOOL) {
	    die "zfsutils-linux not installed\n";
	}

	my $cmd = [$ZPOOL, 'status', '-P', $param->{name}];

	my $pool = {
	    lvl => 0,
	};

	my $curfield;
	my $config = 0;

	my $stack = [$pool];
	my $curlvl = 0;

	run_command($cmd, outfunc => sub {
	    my ($line) = @_;

	    if ($line =~ m/^\s*(\S+): (\S+.*)$/) {
		$curfield = $1;
		$pool->{$curfield} = $2;

		$config = 0 if $curfield eq 'errors';
	    } elsif (!$config && $line =~ m/^\s+(\S+.*)$/) {
		$pool->{$curfield} .= " " . $1;
	    } elsif (!$config && $line =~ m/^\s*config:/) {
		$config = 1;
	    } elsif ($config && $line =~ m/^(\s+)(\S+)\s*(\S+)?(?:\s+(\S+)\s+(\S+)\s+(\S+))?\s*(.*)$/) {
		my ($space, $name, $state, $read, $write, $cksum, $msg) = ($1, $2, $3, $4, $5, $6, $7);
		if ($name ne "NAME") {
		    my $lvl = int(length($space) / 2) + 1; # two spaces per level
		    my $vdev = {
			name => $name,
			msg => $msg,
			lvl => $lvl,
		    };

		    $vdev->{state} = $state if defined($state);
		    $vdev->{read} = $read + 0 if defined($read);
		    $vdev->{write} = $write + 0 if defined($write);
		    $vdev->{cksum} = $cksum + 0 if defined($cksum);

		    my $cur = pop @$stack;

		    if ($lvl > $curlvl) {
			$cur->{children} = [ $vdev ];
		    } elsif ($lvl == $curlvl) {
			$cur = pop @$stack;
			push @{$cur->{children}}, $vdev;
		    } else {
			while ($lvl <= $cur->{lvl} && $cur->{lvl} != 0) {
			    $cur = pop @$stack;
			}
			push @{$cur->{children}}, $vdev;
		    }

		    push @$stack, $cur;
		    push @$stack, $vdev;
		    $curlvl = $lvl;
		}
	    }
	});

	# change treenodes for extjs tree
	$pool->{name} = delete $pool->{pool};
	preparetree($pool);

	return $pool;
    }});

__PACKAGE__->register_method ({
    name => 'create',
    path => '',
    method => 'POST',
    proxyto => 'node',
    protected => 1,
    permissions => {
	check => ['perm', '/', ['Sys.Modify', 'Datastore.Allocate']],
    },
    description => "Create a ZFS pool.",
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    name => get_standard_option('pve-storage-id'),
	    raidlevel => {
		type => 'string',
		description => 'The RAID level to use.',
		enum => ['single', 'mirror', 'raid10', 'raidz', 'raidz2', 'raidz3'],
	    },
	    devices => {
		type => 'string', format => 'string-list',
		description => 'The block devices you want to create the zpool on.',
	    },
	    ashift => {
		type => 'integer',
		minimum => 9,
		maximum => 16,
		optional => 1,
		default => 12,
		description => 'Pool sector size exponent.',
	    },
	    compression => {
		type => 'string',
		description => 'The compression algorithm to use.',
		enum => ['on', 'off', 'gzip', 'lz4', 'lzjb', 'zle', 'zstd'],
		optional => 1,
		default => 'on',
	    },
	    add_storage => {
		description => "Configure storage using the zpool.",
		type => 'boolean',
		optional => 1,
		default => 0,
	    },
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	my $rpcenv = PVE::RPCEnvironment::get();
	my $user = $rpcenv->get_user();

	my $name = $param->{name};
	my $devs = [PVE::Tools::split_list($param->{devices})];
	my $raidlevel = $param->{raidlevel};
	my $node = $param->{node};
	my $ashift = $param->{ashift} // 12;
	my $compression = $param->{compression} // 'on';

	foreach my $dev (@$devs) {
	    $dev = PVE::Diskmanage::verify_blockdev_path($dev);
	    PVE::Diskmanage::assert_disk_unused($dev);
	    my $sysfsdev = $dev =~ s!^/dev/!/sys/block/!r;
	    my $udevinfo = PVE::Diskmanage::get_udev_info($sysfsdev);
	    $dev = $udevinfo->{by_id_link} if defined($udevinfo->{by_id_link});
	}

	PVE::Storage::assert_sid_unused($name) if $param->{add_storage};

	my $numdisks = scalar(@$devs);
	my $mindisks = {
	    single => 1,
	    mirror => 2,
	    raid10 => 4,
	    raidz => 3,
	    raidz2 => 4,
	    raidz3 => 5,
	};

	# sanity checks
	die "raid10 needs an even number of disks\n"
	    if $raidlevel eq 'raid10' && $numdisks % 2 != 0;

	die "please give only one disk for single disk mode\n"
	    if $raidlevel eq 'single' && $numdisks > 1;

	die "$raidlevel needs at least $mindisks->{$raidlevel} disks\n"
	    if $numdisks < $mindisks->{$raidlevel};

	my $worker = sub {
	    PVE::Diskmanage::locked_disk_action(sub {
		# create zpool with desired raidlevel

		my $cmd = [$ZPOOL, 'create', '-o', "ashift=$ashift", $name];

		if ($raidlevel eq 'raid10') {
		    for (my $i = 0; $i < @$devs; $i+=2) {
			push @$cmd, 'mirror', $devs->[$i], $devs->[$i+1];
		    }
		} elsif ($raidlevel eq 'single') {
		    push @$cmd, $devs->[0];
		} else {
		    push @$cmd, $raidlevel, @$devs;
		}

		print "# ", join(' ', @$cmd), "\n";
		run_command($cmd);

		$cmd = [$ZFS, 'set', "compression=$compression", $name];
		print "# ", join(' ', @$cmd), "\n";
		run_command($cmd);

		if (-e '/lib/systemd/system/zfs-import@.service') {
		    my $importunit = 'zfs-import@'. PVE::Systemd::escape_unit($name, undef) . '.service';
		    $cmd = ['systemctl', 'enable', $importunit];
		    print "# ", join(' ', @$cmd), "\n";
		    run_command($cmd);
		}

		if ($param->{add_storage}) {
		    my $storage_params = {
			type => 'zfspool',
			pool => $name,
			storage => $name,
			content => 'rootdir,images',
			nodes => $node,
		    };

		    PVE::API2::Storage::Config->create($storage_params);
		}
	    });
	};

	return $rpcenv->fork_worker('zfscreate', $name, $user, $worker);
    }});

1;
