#!/usr/bin/perl

use strict;
use warnings;

use Test::MockModule;
use Test::More;

use lib ('.', '..');
use PVE::RPCEnvironment;
use PVE::Cluster;
use PVE::Storage;

my $datacenter_cfg = <<'EOF';
bwlimit: default=100,move=80,restore=60
EOF

my $storage_cfg = <<'EOF';
dir: nolimit
	path /dir/a

dir: d50
	path /dir/b
	bwlimit default=50

dir: d50m40r30
	path /dir/c
	bwlimit default=50,move=40,restore=30

dir: d20m40r30
	path /dir/c
	bwlimit default=20,move=40,restore=30

dir: d200m400r300
	path /dir/c
	bwlimit default=200,move=400,restore=300

dir: d10
	path /dir/d
	bwlimit default=10

dir: m50
	path /dir/e
	bwlimit move=50

dir: d200
	path /dir/f
	bwlimit default=200

EOF

my $permissions = {
    'user1@test' => {},
    'user2@test' => { '/' => ['Sys.Modify'], },
    'user3@test' => { '/storage' => ['Datastore.Allocate'], },
    'user4@test' => { '/storage/d20m40r30' => ['Datastore.Allocate'], },
};

my $pve_cluster_module;
$pve_cluster_module = Test::MockModule->new('PVE::Cluster');
$pve_cluster_module->mock(
    cfs_update => sub {},
    get_config => sub {
	my ($file) = @_;
	if ($file eq 'datacenter.cfg') {
	    return $datacenter_cfg;
	} elsif ($file eq 'storage.cfg') {
	    return $storage_cfg;
	}
	die "TODO: mock get_config($file)\n";
    },
);

my $rpcenv_module;
$rpcenv_module = Test::MockModule->new('PVE::RPCEnvironment');
$rpcenv_module->mock(
    check => sub {
	my ($env, $user, $path, $perms, $noerr) = @_;
	return 1 if $user eq 'root@pam';
	my $userperms = $permissions->{$user}
	    or die "no permissions defined for user $user\n";
	if (defined(my $pathperms = $userperms->{$path})) {
	    foreach my $pp (@$pathperms) {
		foreach my $reqp (@$perms) {
		    return 1 if $pp eq $reqp;
		}
	    }
	}
	die "permission denied\n" if !$noerr;
	return 0;
    },
);

my $rpcenv = PVE::RPCEnvironment->init('pub');

my @tests = (
    [ user => 'root@pam' ],
    [ ['unknown', ['nolimit'],   undef], undef, 'root / generic default limit' ],
    [ ['move',    ['nolimit'],   undef], undef, 'root / specific default limit (move)' ],
    [ ['restore', ['nolimit'],   undef], undef, 'root / specific default limit (restore)' ],
    [ ['unknown', ['d50m40r30'], undef], undef, 'root / storage default limit' ],
    [ ['move',    ['d50m40r30'], undef], undef, 'root / specific storage limit (move)' ],
    [ ['restore', ['d50m40r30'], undef], undef, 'root / specific storage limit (restore)' ],

    [ user => 'user1@test' ],
    [ ['unknown', ['nolimit'],      undef], 100, 'generic default limit' ],
    [ ['move',    ['nolimit'],      undef],  80, 'specific default limit (move)' ],
    [ ['restore', ['nolimit'],      undef],  60, 'specific default limit (restore)' ],
    [ ['unknown', ['d50m40r30'],    undef],  50, 'storage default limit' ],
    [ ['move',    ['d50m40r30'],    undef],  40, 'specific storage limit (move)' ],
    [ ['restore', ['d50m40r30'],    undef],  30, 'specific storage limit (restore)' ],
    [ ['unknown', ['d200m400r300'], undef], 200, 'storage default limit above datacenter limits' ],
    [ ['move',    ['d200m400r300'], undef], 400, 'specific storage limit above datacenter limits (move)' ],
    [ ['restore', ['d200m400r300'], undef], 300, 'specific storage limit above datacenter limits (restore)' ],
    [ ['unknown', ['d50'],          undef],  50, 'storage default limit' ],
    [ ['move',    ['d50'],          undef],  50, 'storage default limit (move)' ],
    [ ['restore', ['d50'],          undef],  50, 'storage default limit (restore)' ],

    [ user => 'user2@test' ],
    [ ['unknown', ['nolimit'],       0],     0, 'generic default limit with Sys.Modify, passing unlimited' ],
    [ ['unknown', ['nolimit'],   undef], undef, 'generic default limit with Sys.Modify' ],
    [ ['restore', ['nolimit'],   undef], undef, 'specific default limit with Sys.Modify (restore)' ],
    [ ['move',    ['nolimit'],   undef], undef, 'specific default limit with Sys.Modify (move)' ],
    [ ['unknown', ['d50m40r30'], undef],    50, 'storage default limit with Sys.Modify' ],
    [ ['move',    ['d50m40r30'], undef],    40, 'specific storage limit with Sys.Modify (move)' ],
    [ ['restore', ['d50m40r30'], undef],    30, 'specific storage limit with Sys.Modify (restore)' ],

    [ user => 'user3@test' ],
    [ ['unknown', ['nolimit'],      80],    80, 'generic default limit with privileges on /, passing an override value' ],
    [ ['unknown', ['nolimit'],       0],     0, 'generic default limit with privileges on /, passing unlimited' ],
    [ ['unknown', ['nolimit'],   undef], undef, 'generic default limit with privileges on /' ],
    [ ['move',    ['nolimit'],   undef], undef, 'specific default limit with privileges on / (move)' ],
    [ ['restore', ['nolimit'],   undef], undef, 'specific default limit with privileges on / (restore)' ],
    [ ['unknown', ['d50m20r20'],     0],     0, 'storage default limit with privileges on /, passing unlimited' ],
    [ ['unknown', ['d50m20r20'], undef], undef, 'storage default limit with privileges on /' ],
    [ ['move',    ['d50m20r20'], undef], undef, 'specific storage limit with privileges on / (move)' ],
    [ ['restore', ['d50m20r20'], undef], undef, 'specific storage limit with privileges on / (restore)' ],

    [ user => 'user4@test' ],
    [ ['unknown', ['nolimit'],                   10],     10, 'generic default limit with privileges on a different storage, passing lower override' ],
    [ ['unknown', ['nolimit'],                undef],    100, 'generic default limit with privileges on a different storage' ],
    [ ['unknown', ['nolimit'],                    0],    100, 'generic default limit with privileges on a different storage, passing unlimited' ],
    [ ['move',    ['nolimit'],                undef],     80, 'specific default limit with privileges on a different storage (move)' ],
    [ ['restore', ['nolimit'],                undef],     60, 'specific default limit with privileges on a different storage (restore)' ],
    [ ['unknown', ['d50m40r30'],              undef],     50, 'storage default limit with privileges on a different storage' ],
    [ ['move',    ['d50m40r30'],              undef],     40, 'specific storage limit with privileges on a different storage (move)' ],
    [ ['restore', ['d50m40r30'],              undef],     30, 'specific storage limit with privileges on a different storage (restore)' ],
    [ ['unknown', ['d20m40r30'],              undef],  undef, 'storage default limit with privileges on that storage' ],
    [ ['unknown', ['d20m40r30'],                  0],      0, 'storage default limit with privileges on that storage, passing unlimited' ],
    [ ['move',    ['d20m40r30'],              undef],  undef, 'specific storage limit with privileges on that storage (move)' ],
    [ ['restore', ['d20m40r30'],              undef],  undef, 'specific storage limit with privileges on that storage (restore)' ],
    [ ['unknown', ['d50m40r30', 'd20m40r30'], undef],     50, 'multiple storages default limit with privileges on one of them' ],
    [ ['move',    ['d50m40r30', 'd20m40r30'], undef],     40, 'multiple storages specific limit with privileges on one of them (move)' ],
    [ ['restore', ['d50m40r30', 'd20m40r30'], undef],     30, 'multiple storages specific limit with privileges on one of them (restore)' ],
    [ ['unknown', ['d10', 'd20m40r30'],       undef],     10, 'multiple storages default limit with privileges on one of them (storage limited)' ],
    [ ['move',    ['d10', 'd20m40r30'],       undef],     10, 'multiple storages specific limit with privileges on one of them (storage limited) (move)' ],
    [ ['restore', ['d10', 'd20m40r30'],       undef],     10, 'multiple storages specific limit with privileges on one of them (storage limited) (restore)' ],
    [ ['restore', ['d10', 'd20m40r30'],           5],      5, 'multiple storages specific limit (storage limited) (restore), passing lower override' ],
    [ ['restore', ['d200', 'd200m400r300'],      65],     65, 'multiple storages specific limit (storage limited) (restore), passing lower override' ],
    [ ['restore', ['d200', 'd200m400r300'],     400],    200, 'multiple storages specific limit (storage limited) (restore), passing higher override' ],
    [ ['restore', ['d200', 'd200m400r300'],       0],    200, 'multiple storages specific limit (storage limited) (restore), passing unlimited' ],
    [ ['restore', ['d200', 'd200m400r300'],       1],      1, 'multiple storages specific limit (storage limited) (restore), passing 1' ],
    [ ['restore', ['d10', 'd20m40r30'],         500],     10, 'multiple storages specific limit with privileges on one of them (storage limited) (restore), passing higher override' ],
    [ ['unknown', ['nolimit', 'd20m40r30'],   undef],    100, 'multiple storages default limit with privileges on one of them (default limited)' ],
    [ ['move',    ['nolimit', 'd20m40r30'],   undef],     80, 'multiple storages specific limit with privileges on one of them (default limited) (move)' ],
    [ ['restore', ['nolimit', 'd20m40r30'],   undef],     60, 'multiple storages specific limit with privileges on one of them (default limited) (restore)' ],
    [ ['restore', ['d20m40r30', 'm50'],         200],     60, 'multiple storages specific limit with privileges on one of them (default limited) (restore)' ],
);

foreach my $t (@tests) {
    my ($args, $expected, $description) = @$t;
    if (!ref($args)) {
	if ($args eq 'user') {
	    $rpcenv->set_user($expected);
	} else {
	    die "not a test specification\n";
	}
	next;
    }
    is(PVE::Storage::get_bandwidth_limit(@$args), $expected, $description);
}
done_testing();
