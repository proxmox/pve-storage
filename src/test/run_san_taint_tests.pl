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

# the LUN that write operations get must be usable in command lines and lock names
use PVE::Storage::SAN;
my $san = Test::MockModule->new('PVE::Storage::SAN');
my $tainted_dev = PVE::Tools::file_get_contents('/proc/self/comm') =~ s/.*//sr . '/dev/null';
ok(tainted($tainted_dev), 'device path derived from a file is tainted');
$san->mock(
    is_san_device => sub { 1 },
    scan_luns => sub {
        return [{
            devpath => $tainted_dev,
            wwid => $wwid,
            'by-id' => '/dev/null',
            usage => 'unused',
            multipath => 'none',
        }];
    },
);
my $disks = Test::MockModule->new('PVE::Diskmanage');
$disks->mock(verify_blockdev_path => sub { '/dev/null' });
my $storage = Test::MockModule->new('PVE::Storage');
$storage->mock(config => sub { { ids => {} } });
my $lun = PVE::Storage::SAN::lun_for_device('/dev/null');
ok(!tainted($lun->{devpath}), 'LUN device path is untainted');
ok(!tainted($lun->{wwid}), 'LUN WWID is untainted');
is($lun->{wwid}, '3600a', 'LUN WWID is unchanged');

# the label check puts the logical block size of the device, read from sysfs, into the dd command
use PVE::Storage::LVMPlugin;
my $lvm = Test::MockModule->new('PVE::Storage::LVMPlugin');
my $taint = PVE::Tools::file_get_contents('/proc/self/comm') =~ s/.*//sr;
for my $block (512, 4096, 16384) {
    my $sectors = $block > 4096 ? $block / 512 : 8;
    $lvm->mock(
        file_read_firstline => sub($path) {
            return "$taint$sectors" if $path =~ m{/size$};
            return "$taint$block" if $path =~ m{/queue/logical_block_size$};
            return undef;
        },
        run_command => sub($cmd, %opts) {
            return $opts{outfunc}->('{"report":[{"pv":[]}]}') if $cmd->[0] eq '/sbin/pvs';
            PVE::Tools::run_command(['/usr/bin/true', @$cmd]);
            open(my $out, $opts{output}) or die "$!\n";
            print {$out} "\0" x ($block > 4096 ? $block : 4096);
            close($out) or die "$!\n";
        },
    );
    my $vgs = eval { PVE::Storage::LVMPlugin::lun_volume_groups(['/dev/null']) };
    is_deeply([$@, $vgs], ['', []], "the label check runs with a tainted block size of $block");
}

done_testing();
