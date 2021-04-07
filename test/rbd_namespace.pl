#!/usr/bin/perl

# This script is meant to be run manually on hyperconverged PVE server with a
# Ceph cluster. It tests how PVE handles RBD namespaces.
#
# The pool (default: rbd) must already exist. The namespace and VMs will be
# created.
#
# Parameters like names for the pool an namespace and the VMID can be
# configured.  The VMIDs for the clones is $vmid -1 and $vmid -2.
#
# Cleanup is done after a successful run. Cleanup can also be called manually.
#
# Known issues:
#
# * Snapshot rollback can sometimes be racy with stopping the VM and Ceph
#  recognizing that the disk image is not in use anymore.

use strict;
use warnings;

use Test::More;
use Getopt::Long;
use JSON;

use PVE::Tools qw(run_command);

my $pool = "testpool";
my $use_existing= undef;
my $namespace = "testspace";
my $showhelp = '';
my $vmid = 999999;
my $cleanup = undef;
my $DEBUG = 0;

my $helpstring = "To override default values, set them as named parameters:

--pool		pool name, default: ${pool}
--use_existing  use existing pool, default: 0, needs --pool set
--namespace	rbd namespace, default: ${namespace}
--vmid		VMID of the test VM, default: ${vmid}
--cleanup	Remove the storage definitions, namespaces and VMs
--debug		Enable debug output\n";

GetOptions (
	"pool=s" => \$pool,
	"use_existing" => \$use_existing,
	"namespace=s" => \$namespace,
	"vmid=i" => \$vmid,
	"help" => \$showhelp,
	"cleanup" => \$cleanup,
	"debug" => \$DEBUG,
) or die ($helpstring);

die $helpstring if $showhelp;

my $storage_name = "${pool}-${namespace}";

my $vmid_clone = int($vmid) - 1;
my $vmid_linked_clone = int($vmid) - 2;

sub jp {
    return if !$DEBUG;
    print to_json($_[0], { utf8 => 8, pretty => 1, canonical => 1 }) . "\n";
}

sub run_cmd {
    my ($cmd, $json, $ignore_errors) = @_;

    my $raw = '';
    my $parser = sub {$raw .= shift;};

    eval {
	run_command($cmd, outfunc => $parser);
    };
    if (my $err = $@) {
	die $err if !$ignore_errors;
    }

    if ($json) {
	my $result;
	if ($raw eq '') {
	    $result = [];
	} elsif ($raw =~ m/^(\[.*\])$/s) { # untaint
	    $result = JSON::decode_json($1);
	} else {
	    die "got unexpected data from command: '$cmd' -> '$raw'\n";
	}
	return $result;
	}
    return $raw;
}

sub run_test_cmd {
    my ($cmd) = @_;

    my $raw = '';
    my $out = sub {
	my $line = shift;
	$raw .= "${line}\n";
    };

    eval {
	run_command($cmd, outfunc => $out);
    };
    if (my $err = $@) {
	print $raw;
	print $err;
	return 0;
    }
    print $raw;
    return 1;
}

sub prepare {
    print "Preparing test environent\n";

    my $pools = run_cmd("ceph osd pool ls --format json", 1);

    my %poolnames = map {$_ => 1} @$pools;
    die "Pool '$pool' does not exist!\n"
	if !exists($poolnames{$pool}) && $use_existing;

    run_cmd(['pveceph', 'pool', 'create', ${pool}, '--add_storages', 1])
	if !$use_existing;

    my $namespaces = run_cmd(['rbd', '-p', ${pool}, 'namespace', 'ls', '--format', 'json'], 1);
    my $ns_found = 0;
    for my $i (@$namespaces) {
	#print Dumper $i;
	$ns_found = 1 if $i->{name} eq $namespace;
    }

    if (!$ns_found) {
	print "Create namespace '${namespace}' in pool '${pool}'\n";
	run_cmd(['rbd', 'namespace', 'create', "${pool}/${namespace}"]);
    }

    my $storages = run_cmd(['pvesh', 'get', 'storage', '--output-format', 'json'], 1);
    #print Dumper $storages;
    my $rbd_found = 0;
    my $pool_found = 0;

    print "Create storage definition\n";
    for my $stor (@$storages) {
	$pool_found = 1 if $stor->{storage} eq $pool;
	$rbd_found = 1 if $stor->{storage} eq $storage_name;

	if ($rbd_found) {
	    run_cmd(['pvesm', 'set', ${storage_name}, '--krbd', '0']);
	    die "Enable the storage '$stor->{storage}'!" if $stor->{disable};
	}
    }
    if (!$pool_found) {
	die "No storage for pool '${pool}' found! Must have same name as pool!\n"
	    if $use_existing;

	run_cmd(['pvesm', 'add', 'rbd', $pool, '--pool', $pool, '--content', 'images,rootdir']);
    }
    # create PVE storages (librbd / krbd)
    run_cmd(['pvesm', 'add', 'rbd', ${storage_name}, '--krbd', '0', '--pool', ${pool}, '--namespace', ${namespace}, '--content', 'images,rootdir'])
	if !$rbd_found;


    # create test VM
    print "Create test VM ${vmid}\n";
    my $vms = run_cmd(['pvesh', 'get', 'cluster/resources', '--type', 'vm', '--output-format', 'json'], 1);
    for my $vm (@$vms) {
	# TODO: introduce a force flag to make this behaviour configurable

	if ($vm->{vmid} eq $vmid) {
	    print "Test VM '${vmid}' already exists. It will be removed and recreated!\n";
	    run_cmd(['qm', 'stop', ${vmid}], 0, 1);
	    run_cmd(['qm', 'destroy', ${vmid}]);
	}
    }
    run_cmd(['qm', 'create', ${vmid}, '--bios', 'ovmf', '--efidisk0', "${storage_name}:1", '--scsi0', "${storage_name}:2"]);
}


sub cleanup {
    print "Cleaning up test environment!\n";
    print "Removing VMs\n";
    run_cmd(['qm', 'stop', ${vmid}], 0, 1);
    run_cmd(['qm', 'stop', ${vmid_linked_clone}], 0, 1);
    run_cmd(['qm', 'stop', ${vmid_clone}], 0, 1);
    run_cmd(['qm', 'destroy', ${vmid_linked_clone}], 0, 1);
    run_cmd(['qm', 'destroy', ${vmid_clone}], 0, 1);
    run_cmd(['for', 'i', 'in', "/dev/rbd/${pool}/${namespace}/*;", 'do', '/usr/bin/rbd', 'unmap', '\$i;', 'done'], 0, 1);
    run_cmd(['qm', 'unlock', ${vmid}], 0, 1);
    run_cmd(['qm', 'destroy', ${vmid}], 0, 1);

    print "Removing Storage definition for ${storage_name}\n";
    run_cmd(['pvesm', 'remove', ${storage_name}], 0, 1);

    print "Removing RBD namespace '${pool}/${namespace}'\n";
    run_cmd(['rbd', 'namespace', 'remove', "${pool}/${namespace}"], 0, 1);

    if (!$use_existing) {
	print "Removing Storage definition for ${pool}\n";
	run_cmd(['pvesm', 'remove', ${pool}], 0, 1);
	print "Removing test pool\n";
	run_cmd(['pveceph', 'pool', 'destroy', $pool]);
    }
}

my $tests = [
    # Example structure for tests
    # {
    #     name => "name of test section",
    #     preparations => [
    #         ['some', 'prep', 'command'],
    #     ],
    #     steps => [
    #         ['test', 'cmd', $vmid],
    #         ['second', 'step', $vmid],
    #     ],
    #     cleanup => [
    #         ['cleanup', 'command'],
    #     ],
    # },
    {
	name => 'first VM start',
	steps => [
	    ['qm', 'start', $vmid],
	],
    },
    {
	name => 'snapshot/rollback',
	steps => [
	    ['qm', 'snapshot', $vmid, 'test'],
	    ['qm', 'rollback', $vmid, 'test'],
	],
	cleanup => [
	    ['qm', 'unlock', $vmid],
	],
    },
    {
	name => 'remove snapshot',
	steps => [
	    ['qm', 'delsnapshot', $vmid, 'test'],
	],
    },
    {
	name => 'moving disk between namespaces',
	steps => [
	    ['qm', 'move_disk', $vmid, 'scsi0', $pool, '--delete', 1],
	    ['qm', 'move_disk', $vmid, 'scsi0', $storage_name, '--delete', 1],
	],
    },
    {
	name => 'switch to krbd',
	preparations => [
	    ['qm', 'stop', $vmid],
	    ['pvesm', 'set', $storage_name, '--krbd', 1]
	],
    },
    {
	name => 'start VM with krbd',
	steps => [
	    ['qm', 'start', $vmid],
	],
    },
    {
	name => 'snapshot/rollback with krbd',
	steps => [
	    ['qm', 'snapshot', $vmid, 'test'],
	    ['qm', 'rollback', $vmid, 'test'],
	],
	cleanup => [
	    ['qm', 'unlock', $vmid],
	],
    },
    {
	name => 'remove snapshot with krbd',
	steps => [
	    ['qm', 'delsnapshot', $vmid, 'test'],
	],
    },
    {
	name => 'moving disk between namespaces with krbd',
	steps => [
	    ['qm', 'move_disk', $vmid, 'scsi0', $pool, '--delete', 1],
	    ['qm', 'move_disk', $vmid, 'scsi0', $storage_name, '--delete', 1],
	],
    },
    {
	name => 'clone VM with krbd',
	steps => [
	    ['qm', 'clone', $vmid, $vmid_clone],
	],
    },
    {
	name => 'switch to non krbd',
	preparations => [
	    ['qm', 'stop', $vmid],
	    ['qm', 'stop', $vmid_clone],
	    ['pvesm', 'set', $storage_name, '--krbd', 0]
	],
    },
    {
	name => 'templates and linked clone',
	steps => [
	    ['qm', 'template', $vmid],
	    ['qm', 'clone', $vmid, $vmid_linked_clone],
	    ['qm', 'start', $vmid_linked_clone],
	    ['qm', 'stop', $vmid_linked_clone],
	],
    },
    {
	name => 'start linked clone with krbd',
	preparations => [
	    ['pvesm', 'set', $storage_name, '--krbd', 1]
	],
	steps => [
	    ['qm', 'start', $vmid_linked_clone],
	    ['qm', 'stop', $vmid_linked_clone],
	],
    },
];

sub run_prep_cleanup {
    my ($cmds) = @_;

    for (@$cmds) {
	print join(' ', @$_). "\n";
	run_cmd($_);
    }
}

sub run_steps {
    my ($steps) = @_;

    for (@$steps) {
	ok(run_test_cmd($_), join(' ', @$_));
    }
}

sub run_tests {
    print "Running tests:\n";

    my $num_tests = 0;
    for (@$tests) {
	$num_tests += scalar(@{$_->{steps}}) if defined $_->{steps};
    }

    print("Tests: $num_tests\n");
    plan tests => $num_tests;

    for my $test (@$tests) {
	print "Section: $test->{name}\n";
	run_prep_cleanup($test->{preparations}) if defined $test->{preparations};
	run_steps($test->{steps}) if defined $test->{steps};
	run_prep_cleanup($test->{cleanup}) if defined $test->{cleanup};
    }

    done_testing();

    if (Test::More->builder->is_passing()) {
	cleanup();
    }
}

if ($cleanup) {
    cleanup();
} else {
    prepare();
    run_tests();
}

