#!/usr/bin/perl

use strict;
use warnings;

use lib '..';

use File::Temp qw(tempdir);
use Test::More;
use Test::MockModule;

use PVE::Storage;

# Run the cleanup worker on a regular file without LVM commands or throttle delays.
my $dir = tempdir(CLEANUP => 1);
my $name = 'vm-100-disk-0';
my (@delays, @commands);
my $plugin = Test::MockModule->new('PVE::Storage::LVMPlugin');
$plugin->mock(abs_path => sub { return '/dev/dm-123'; });
$plugin->mock(file_read_firstline => sub { return $_[0] =~ m!/size$! ? 2048 : 0; });
$plugin->mock(clock_gettime => sub { return 100; });
$plugin->mock(run_command => sub { push @commands, $_[0]->[0]; });
$plugin->mock(cluster_lock_storage => sub { return $_[4]->(); });
my $timer = Test::MockModule->new('Time::HiRes');
$timer->mock(sleep => sub { push @delays, $_[0]; });

for my $value ('0m', '-0', '00', '1m') {
    open(my $fh, '>', "$dir/del-$name") or die "create test volume: $!\n";
    close($fh) or die "close test volume: $!\n";
    @delays = ();
    @commands = ();
    my $output = '';
    my $ok = eval {
        open(my $stdout, '>', \$output) or die "capture output: $!\n";
        local *STDOUT = $stdout;
        my $worker = PVE::Storage::LVMPlugin->free_image(
            'test',
            { vgname => "..$dir", saferemove => 1, saferemove_throughput => $value },
            $name,
            0,
            'raw',
        );
        $worker->();
        1;
    };
    ok($ok, "cleanup succeeds for '$value'") or diag($@, $output);
    is_deeply(
        [\@delays, $commands[-1]],
        [[$value eq '1m' ? 1 : 0.1], '/sbin/lvremove'],
        "'$value' uses the normalized rate and completes removal",
    );
}

done_testing();
