#!/usr/bin/perl -T

use v5.36;
use lib ('.', '..');
use File::Temp qw(tempfile);
use Scalar::Util qw(tainted);
use Test::MockModule;
use Test::More;
use PVE::Multipath;
use PVE::Tools;

$ENV{PATH} = '/usr/sbin:/usr/bin:/sbin:/bin';
delete @ENV{qw(IFS CDPATH ENV BASH_ENV)};
my ($fh, $path) = tempfile('multipath-taint-XXXX', DIR => '.', UNLINK => 1);
print $fh '3600a';
close($fh);
my $wwid = PVE::Tools::file_get_contents($path);
ok(tainted($wwid), 'WWID read from file is tainted');
my $mp = Test::MockModule->new('PVE::Multipath');
$mp->mock(is_supported => sub { 1 });
$mp->mock(
    run_command => sub($cmd, %opts) {
        is_deeply($cmd, ['/sbin/multipath', '-a', '--', '3600a'], 'WWID add command');
        ok($opts{timeout}, 'command is bounded');
        PVE::Tools::run_command(['/usr/bin/true', @$cmd]);
    },
);
PVE::Multipath::add_wwid($wwid);
for my $bad ('-bad', '1:2', '../dev', "3600a\n") {
    eval { PVE::Multipath::add_wwid($bad) };
    like($@, qr/invalid WWID/, 'ambiguous identity refused');
}

done_testing();
