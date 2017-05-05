package PVE::CLI::pvesr;

use strict;
use warnings;
use POSIX qw(strftime);

use PVE::JSONSchema qw(get_standard_option);
use PVE::INotify;
use PVE::RPCEnvironment;
use PVE::Tools qw(extract_param);
use PVE::SafeSyslog;
use PVE::CLIHandler;

use PVE::API2::Storage::Replication;

use base qw(PVE::CLIHandler);

my $nodename = PVE::INotify::nodename();

sub setup_environment {
    PVE::RPCEnvironment->setup_default_cli_env();
}

my $print_list = sub {
    my ($conf, $json) = @_;

    if (defined($json)) {
	print JSON::encode_json($conf);
    } else {
	printf("%-10s%-20s%-20s%-5s%-10s%-5s\n",
	       "VMID", "DEST", "LAST SYNC","IVAL", "STATE", "FAIL");

	foreach my $vmid (sort keys %$conf) {
	    my $job = $conf->{$vmid};
	    my $timestr = strftime("%Y-%m-%d_%H:%M:%S", localtime($job->{lastsync}));

	    printf("%-9s ", $vmid);
	    printf("%-19s ", $job->{tnode});
	    printf("%-19s ", $timestr);
	    printf("%-4s ", $job->{interval});
	    printf("%-9s ", $job->{state});
	    printf("%-9s\n", $job->{fail});
	}
    }

};

sub set_list {
    my ($list, $synctime, $vmid) = @_;

    if (defined($list->{$synctime})) {
	$list = set_list($list,$synctime+1, $vmid);
    } else {
	$list->{$synctime} = $vmid;
    }
    return $list;
}

my $get_replica_list = sub {

    my $jobs = PVE::ReplicationTools::read_state();
    my $list = {};

    foreach my $vmid (keys %$jobs) {
	my $job = $jobs->{$vmid};
	my $lastsync = $job->{lastsync};

	# interval in min
	my $interval = $job->{interval};
	my $now = time();
	my $fail = $job->{fail};

	my $synctime = $lastsync + $interval * 60;

	if ($now >= $synctime && $job->{state} eq 'ok') {
	    $list = set_list($list, $synctime, $vmid);
	} elsif ($job->{state} eq 'sync') {

	    my $synctime += $interval * ($job->{fail}+1);
	    $list = set_list($list, $synctime, $vmid)
		if ($now >= $synctime);

	}
    }

    return $list;
};

my $replicate_vms  = sub {
    my ($list) = @_;

    my @sorted_times = reverse sort keys %$list;

    foreach my $synctime (@sorted_times) {
	eval {
	    PVE::ReplicationTools::sync_guest($list->{$synctime});
	};
	if (my $err = $@) {
	    syslog ('err', $err );
	}
    }
};

__PACKAGE__->register_method ({
    name => 'run',
    path => 'run',
    method => 'POST',
    description => "This method will run by the systemd-timer and sync all jobs",
    permissions => {
	description => {
	    check => ['perm', '/', [ 'Sys.Console' ]],
	},
    },
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	},
    },
    returns => { type => 'null' },
    code => sub {

	my $list = &$get_replica_list();
	&$replicate_vms($list);

	return undef;
    }});

__PACKAGE__->register_method ({
    name => 'destroyjob',
    path => 'destroyjob',
    method => 'DELETE',
    description => "Destroy an async replication job",
    permissions => {
	description => {
	    check => ['perm', '/storage', ['Datastore.Allocate']],
	},
    },
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    vmid => {
		type => 'string', format => 'pve-vmid',
		description => "The VMID of the guest.",
		completion => \&PVE::Cluster::complete_local_vmid,
	    },
	},
    },
    returns => { type => 'null' },
    code => sub {
	my ($param) = @_;

	my $vmid = extract_param($param, 'vmid');

	PVE::ReplicationTools::destroy_replica($vmid);

    }});

__PACKAGE__->register_method ({
    name => 'list',
    path => 'list',
    method => 'GET',
    description => "List of all replication jobs.",
    permissions => {
	user => 'all'
    },
    protected => 1,
    parameters => {
	additionalProperties => 0,
	properties => {
	    vmid => {
		type => 'string', format => 'pve-vmid',
		description => "The VMID of the guest.",
		completion => \&PVE::Cluster::complete_local_vmid,
	    },
	},
    },
    protected => 1,
    proxyto => 'node',
    parameters => {
	additionalProperties => 0,
	properties => {
	    node => get_standard_option('pve-node'),
	    nodes => get_standard_option('pve-node-list' ,
					 {description => "Notes where the jobs is located.",
					  optional => 1}),
	    json => {
		optional => 1,
		type => 'boolean',
		description => "Output in JSON format.",
	    },
	},
    },
    returns => { type => 'string' },
    code => sub {
	my ($param) = @_;

	if ($param->{nodes}) {
	    foreach my $node (PVE::Tools::split_list($param->{nodes})) {
		die "Node: $node does not exists.\n" if
		    !PVE::Cluster::check_node_exists($node);
	    }
	}

	my $nodes = $param->{nodes} ?
	    $param->{nodes} : $param->{node};

	my $list = PVE::ReplicationTools::get_all_jobs($nodes);

	&$print_list($list, $param->{json});
}});


our $cmddef = {
    list => [  __PACKAGE__ , 'list' , [],  {node => $nodename}],
    run => [ __PACKAGE__ , 'run'],
    destroyjob => [ __PACKAGE__ , 'destroyjob', ['vmid']],
};

1;
