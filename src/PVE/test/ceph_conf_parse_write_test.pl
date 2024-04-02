#!/usr/bin/env perl

use strict;
use warnings;

use lib qw(../..);

use Test::More;

use PVE::CephConfig;


# An array of test cases.
# Each test case is comprised of the following keys:
#   description	  => to identify a single test
#   expected_cfg  => the hash that parse_ceph_config should return
#   raw	          => the raw content of the file to test
my $tests = [
    {
	description => 'empty file',
	expected_cfg => {},
	raw => <<~EOF,
	EOF
    },
    {
	description => 'file without section',
	expected_cfg => {},
	raw => <<~EOF,
	While Ceph's format doesn't allow this, we do, because it makes things simpler
	foo = bar
	arbitrary text can go here

	Rust is better than Perl
	EOF
    },
    {
	description => 'single section',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = baz
	EOF
    },
    {
	description => 'single section, no key-value pairs',
	expected_cfg => {
	    foo => {},
	},
	raw => <<~EOF,
	[foo]
	EOF
    },
    {
	description => 'single section, whitespace before key',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	[foo]
	     \t     bar = baz
	EOF
    },
    {
	description => 'single section, section header with preceding whitespace',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	  \t    [foo]
	bar = baz
	EOF
    },
    {
	description => 'single section, section header with comment',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	[foo] # some comment
	bar = baz
	EOF
    },
    {
	description => 'single section, section header ' .
	    'with preceding whitespace and comment',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	  \t  [foo] ; some comment
	bar = baz
	EOF
    },
    {
	description => 'single section, arbitrary text before section',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	Rust is better than Perl

	This text is ignored by our parser, because it makes things simpler
	[foo]
	bar = baz
	EOF
    },
    {
	description => 'single section, invalid key-value pairs',
	expected_cfg => {
	    foo => {
		bar => 'baz',
	    },
	},
	raw => <<~EOF,
	[foo]
	this here will cause a warning and is ignored
	bar = baz
	as well as this
	EOF
    },
    {
	description => 'single section, multiple key-value pairs',
	expected_cfg => {
	    foo => {
		one => 1,
		two => 2,
		three => 3,
	    },
	},
	raw => <<~EOF,
	[foo]
	one = 1
	two = 2

	three = 3
	EOF
    },
    {
	description => 'multiple sections with whitespace in section headers',
	expected_cfg => {
	    'foo bar' => {},
	    ' quo  qux ' => {},
	},
	raw => <<~EOF,
	[foo bar]
	[ quo  qux ]
	EOF
    },
    {
	description => 'single section with whitespace in section header '
	    . 'and multiple key-value pair',
	expected_cfg => {
	    'foo bar' => {
		one => 1,
		two => 2,
	    },
	    '  quo  ' => {
		three => 3,
	    },
	},
	raw => <<~EOF,
	[foo bar]
	one = 1
	two = 2

	[  quo  ]
	three = 3
	EOF
    },
    {
	description => 'single section, numeric key-value pairs',
	expected_cfg => {
	    foo => {
		'0' => 0,
		'1' => 1,
		'2' => 0,
		'3' => 1,
		'3.14' => 1.414,
	    },
	},
	raw => <<~EOF,
	[foo]
	0 = 0
	1 = 1
	2 = 0
	3 = 1
	3.14 = 1.414
	EOF
    },
    {
	description => 'single section, keys with single-quoted values',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = 'baz'
	quo = 'qux'
	EOF
    },
    {
	description => 'single section, keys with double-quoted values',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = "baz"
	quo = "qux"
	EOF
    },
    {
	description => 'single section, keys with quoted values, '
	    . 'comment literals within quotes',
	expected_cfg => {
	    foo => {},
	},
	raw => <<~EOF,
	[foo]
	one = "1;1"
	two = "2#2"
	three = '3;3'
	four = '4#4'
	EOF
    },
    {
	description => 'single section, keys with quoted values, '
	    . 'escaped comment literals within quotes',
	expected_cfg => {
	    foo => {
		one => '1;1',
		two => '2#2',
		three => '3;3',
		four => '4#4',
	    },
	},
	raw => <<~EOF,
	[foo]
	one = "1\\;1"
	two = "2\\#2"
	three = '3\\;3'
	four = '4\\#4'
	EOF
    },
    {
	description => 'single section, keys with quoted values, '
	    . 'comments after values',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = "baz" ; some comment
	quo = 'qux'	# another comment
	EOF
    },
    {
	description => 'single section, keys with quoted values, '
	    . 'continued lines after quotes',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = "baz"\\

	quo = 'qux'\\

	EOF
    },
    {
	description => 'single section, keys with quoted values, '
	    . 'continued lines with comments after quotes',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = "baz"\\
	# believe it or not, this is valid syntax

	quo = 'qux'\\
	\\
	\\
	\\
	    \t \t ; and it "supports" trailing whitespace for some reason
	EOF
    },
    {
	description => 'single section, key-value pairs with whitespace',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	       \t  bar  \t   =\t \tbaz\t

	quo\t=\tqux
	EOF
    },
    {
	description => 'single section, key-value pairs without whitespace',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar=baz
	quo=qux
	EOF
    },
    {
	description => 'single section, key-value pairs with repeated whitespace '
	    . 'in key names',
	expected_cfg => {
	    foo => {
		'one_space' => 1,
		'two_spaces' => 2,
		'three_spaces' => 3,
	    },
	},
	raw => <<~EOF,
	[foo]
	one space = 1
	two  spaces = 2
	three  spaces = 3
	EOF
    },
    {
	description => 'single section, key-value pairs with whitespace and '
	    . 'complex whitespace in key names',
	expected_cfg => {
	    foo => {
		'one_two' => 'foo',
		'three_four' => 'bar',
		'five_six' => 'baz',
	    },
	},
	raw => <<~EOF,
	[foo]
	       \t  one two  \t   =\t \tfoo\t

	three   \t  four\t=\tbar\t
	five     six=baz
	EOF
    },
    {
	description => 'single section, key-value pairs with repeated whitespace '
	    . 'and underlines in key names',
	expected_cfg => {
	    foo => {
		'one_ul' => 2,
		'two_ul' => 0,
		'two__ul' => 0,
		'odd___name' => 4,
	    },
	},
	raw => <<~EOF,
	[foo]
	# these are equivalent
	one ul = 0
	one_ul = 1
	one             ul = 2

	# these are not
	two  ul = 0
	two__ul = 0

	# these are equivalent
	odd _ name = 0
	odd      _     name = 1
	odd__ name = 2
	odd __name = 3
	odd___name = 4

	EOF
    },
    {
	description => 'single section with line continuations, multiple key-value pairs',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[\\
	f\\
	o\\
	o\\
	]\\

	bar = baz
	quo = qux
	EOF
    },
    {
	description => 'single section, key-value pairs with continued lines in keys',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar\\
	= baz
	\\
	quo\\
	\\
	\\
	\\
	\\
	\\
	\\
	= qux
	EOF
    },
    {
	description => 'multiple sections with escaped comment literals, '
	    . 'multiple key-value pairs',
	expected_cfg => {
	    'f;oo' => {
		one => 1,
		two => 2,
	    },
	    'b#ar' => {
		three => 3,
		four => 4,
	    },
	    '###' => {
		five => 5,
	    },
	    ';;;' => {
		six => 6,
	    },
	},
	raw => <<~EOF,
	[f\\;oo]
	one = 1
	two = 2

	[b\\#ar]
	three = 3
	four = 4

	[\\#\\#\\#]
	five = 5

	[\\;\\;\\;]
	six = 6
	EOF
    },
    {
	description => 'single section, key-value pairs with comments',
	expected_cfg => {
	    foo => {
		bar => 'baz',
		quo => 'qux',
	    },
	},
	raw => <<~EOF,
	[foo]
	; preceding comment
	bar = baz # some comment
	### comment inbetween
	;; another one for good measure
	quo = qux ; another comment
	# trailing comment
	EOF
    },
    {
	description => 'single section, key-value pairs with continued lines',
	expected_cfg => {
	    foo => {
		bar => 'baz continued baz',
		quo => "qux continued \tqux",
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = baz \\
	continued baz

	quo =\\
	qux \\
	continued \\
	\tqux
	EOF
    },
    {
	description => 'single section, key-value pairs with ' .
	    'continued lines and comments',
	expected_cfg => {
	    foo => {
		bar => 'baz continued baz',
		quo => 'qux continued qux',
		key => 'value',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = baz \\
	continued baz # comments are allowed here

	quo =\\
	qux \\
	continued \\
	qux # but this continuation will be ignored, because it's in a comment: \\
	key = value\\
	# really weird comment
	EOF
    },
    {
	description => 'single section, key-value pairs with ' .
	    'escaped commment literals in values',
	expected_cfg => {
	    foo => {
		bar => 'baz#escaped',
		quo => 'qux;escaped',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = baz\\#escaped
	quo = qux\\;escaped
	EOF
    },
    {
	description => 'single section, key-value pairs with ' .
	    'continued lines and escaped commment literals in values',
	expected_cfg => {
	    foo => {
		bar => 'baz#escaped',
		quo => 'qux;escaped continued# escaped done',
	    },
	},
	raw => <<~EOF,
	[foo]
	bar = baz\\#escaped

	quo = qux\\;escaped\\
	 continued\\# escaped \\
	done
	EOF
    },
    {
	description => 'single section, key-value pairs with escaped comment '
	    . 'literals in key names',
	expected_cfg => {
	    foo => {
		'b#a#r' => 'baz',
		';q;uo' => 'qux',
		'#' => 1,
		'##' => 2,
		'###' => 3,
		';' => 1,
		';;' => 2,
		';;;' => 3,
	    },
	},
	raw => <<~EOF,
	[foo]
	b\\#a\\#r = baz
	\\;q\\;uo = qux

	\\# = 1
	\\#\\# = 2
	\\#\\#\\# = 3

	\\; = 1
	\\;\\; = 2
	\\;\\;\\; = 3
	EOF
    },
    {
	description => 'multiple sections, multiple key-value pairs',
	expected_cfg => {
	    foo => {
		one => 1,
		two => 2,
	    },
	    bar => {
		three => 3,
		four => 4,
	    },
	},
	raw => <<~EOF,
	[foo]
	one = 1
	two = 2
	[bar]
	three = 3
	four = 4
	EOF
    },
    {
	description => 'multiple sections, multiple key-value pairs, '
	    . 'comments inline and inbetween, escaped comment literals, '
	    . 'continued lines, arbitrary whitespace',
	# NOTE: We don't use '/etc/pve/priv/$cluster.$name.keyring' as value for
	# 'keyring' below, because `ceph-conf` will actually substitute those.
	# Because we don't care for that (not the parser's or writer's job) we
	# just omit the dollar signs.
	expected_cfg => {
	    global => {
		auth_client_required => 'cephx',
		auth_cluster_required => 'cephx',
		auth_service_required => 'cephx',
		cluster_network => '172.16.65.0/24',
		fsid => '0e2f72eb-ffff-ffff-ffff-f480790a5b07',
		mon_allow_pool_delete => 'true',
		mon_host => '172.16.65.12 172.16.65.13 172.16.65.11',
		ms_bind_ipv4 => 'true',
		osd_pool_default_min_size => '2',
		osd_pool_default_size => '3',
		public_network => '172.16.65.0/24',
	    },
	    client => {
		keyring => '/etc/pve/priv/cluster.name.keyring',
	    },
	    'mon.ceph-01' => {
		public_addr => '172.16.65.11',
	    },
	    'mon.ceph-02' => {
		public_addr => '172.16.65.12',
	    },
	    'mon.ceph-03' => {
		public_addr => '172.16.65.13',
	    },
	    'some arbitrary section' => {
		some_key => 'foo;bar;baz',
	    },
	},
	raw => <<~EOF,
	[global]
	auth_client_required = cephx
		auth_cluster_required = \\
	cephx
	auth_service_required =           cephx
			cluster_network =        172.16.65.0/24
	fsid = 0e2f72eb-ffff-ffff-ffff-f480790a5b07
	    mon_allow_pool_delete = true
	mon_host = \\
	172.16.65.12 \\
	172.16.65.13 \\
	172.16.65.11 # why is this one last? nobody knows for sure!

	ms_bind_ipv4 = true
	osd_pool_default_min_size =\\
	\\
	\\
	\\
	\\
	\\
	\\
	\\
	\\
	2
	osd_pool_default_size =\\
					    3
	public_network = 172.16.65.0/24 # some comment

	[client] ### another comment
	keyring = /etc/pve/priv/cluster.name.keyring# cheeky trailing comment

	## mon config ##
	[mon.ceph-01]
	public_addr = 172.16.65.11 ; foo

	;; another arbitrary comment ;;
	[mon.ceph-02] ;; very important comment here
	public_addr = "172.16.65.12" # bar

	[mon.ceph-03]
	public_addr = '172.16.65.13'			# baz

	[some arbitrary section]
	some key = "foo\\;bar\\;baz"    # I am a comment
	EOF
    },
];

sub test_parse_ceph_config {
    my ($case) = @_;

    my $desc = "parse_ceph_config: $case->{description}";

    my $parse_result = eval { PVE::CephConfig::parse_ceph_config(undef, $case->{raw}) };
    if ($@) {
	fail($desc);
	diag('Failed to parse config:');
	diag($@);
	return;
    }

    if (!is_deeply($parse_result, $case->{expected_cfg}, $desc)) {
	diag("=== Expected ===");
	diag(explain($case->{expected_cfg}));
	diag("=== Got ===");
	diag(explain($parse_result));
    }
}

sub test_write_ceph_config {
    my ($case) = @_;

    my $desc = "write_ceph_config: $case->{description}";

    my $write_result = eval { PVE::CephConfig::write_ceph_config(undef, $case->{expected_cfg}) };
    if ($@) {
	fail($desc);
	diag('Failed to write config:');
	diag($@);
	return;
    }

    my $parse_result = eval { PVE::CephConfig::parse_ceph_config(undef, $write_result) };
    if ($@) {
	fail($desc);
	diag('Failed to parse previously written config:');
	diag($@);
	return;
    }

    if (!is_deeply($parse_result, $case->{expected_cfg}, $desc)) {
	diag("=== Expected ===");
	diag(explain($case->{expected_cfg}));
	diag("=== Got ===");
	diag(explain($parse_result));
	diag("=== Write Output ===");
	diag($write_result);
    }
}

sub main {
    my $test_subs = [
	\&test_parse_ceph_config,
	\&test_write_ceph_config,
    ];

    plan(tests => scalar($tests->@*) * scalar($test_subs->@*));

    for my $case ($tests->@*) {
	for my $test_sub ($test_subs->@*) {
	    eval {
		# suppress warnings here to make output less noisy for certain tests
		local $SIG{__WARN__} = sub {};
		$test_sub->($case);
	    };
	    warn "$@\n" if $@;
	};
    }

    done_testing();
}

main();
