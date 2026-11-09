#!/usr/bin/perl

use strict;
use warnings;

use File::Path qw(make_path);
use File::Temp qw(tempdir);
use Test::MockModule;
use Test::More;

use lib ('.', '..');

use PVE::API2::Storage::PruneBackups;
use PVE::RPCEnvironment;
use PVE::Storage;
use PVE::Storage::Plugin;
use PVE::Tools;

my $storage_dir = tempdir(CLEANUP => 1);

my $storage_cfg = <<"EOF";
dir: dir
	path $storage_dir
	content vztmpl,snippets,iso,backup,rootdir,images
EOF

my $user_cfg = <<'EOF';
user:root@pam:1:0::::::
user:noperm@pve:1:0::::::
user:otherstorage@pve:1:0::::::
user:dsallocate@pve:1:0::::::
user:dsaudit@pve:1:0::::::
user:backup@pve:1:0::::::
user:vmuser@pve:1:0::::::


role:dsallocate:Datastore.Allocate:
role:dsaudit:Datastore.Audit:
role:vmuser:VM.Config.Disk,Datastore.Audit:
role:backup:VM.Backup,Datastore.AllocateSpace:

acl:1:/storage/foo:otherstorage@pve:dsallocate:
acl:1:/storage/dir:dsallocate@pve:dsallocate:
acl:1:/storage/dir:dsaudit@pve:dsaudit:
acl:1:/vms/100:backup@pve:backup:
acl:1:/storage/dir:backup@pve:backup:
acl:1:/vms/100:vmuser@pve:vmuser:
acl:1:/vms/111:vmuser@pve:vmuser:
acl:1:/storage/dir:vmuser@pve:vmuser:
EOF

my @users =
    qw(root@pam noperm@pve otherstorage@pve dsallocate@pve dsaudit@pve backup@pve vmuser@pve);

my $pve_cluster_module;
$pve_cluster_module = Test::MockModule->new('PVE::Cluster');
$pve_cluster_module->mock(
    cfs_update => sub { },
    get_config => sub {
        my ($file) = @_;
        if ($file eq 'storage.cfg') {
            return $storage_cfg;
        } elsif ($file eq 'user.cfg') {
            return $user_cfg;
        }
        die "TODO: mock get_config($file)\n";
    },
);

my $rpcenv = PVE::RPCEnvironment->init('pub');
$rpcenv->init_request();

my @types = sort keys PVE::Storage::Plugin::get_vtype_subdirs()->%*;
my $all_types = { map { $_ => 1 } @types };

my @tests = (
    {
        volid => 'dir:backup/vzdump-qemu-100-2025_07_29-13_00_55.vma',
        denied_users => {
            'dsaudit@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => {
            'backup' => 1,
        },
    },
    {
        volid => 'dir:100/vm-100-disk-0.qcow2',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
        },
        allowed_types => {
            'images' => 1,
            'rootdir' => 1,
        },
    },
    {
        volid => 'dir:vztmpl/alpine-3.22-default_20250617_amd64.tar.xz',
        denied_users => {},
        allowed_types => {
            'vztmpl' => 1,
        },
    },
    {
        volid => 'dir:iso/virtio-win-0.1.271.iso',
        denied_users => {},
        allowed_types => {
            'iso' => 1,
        },
    },
    {
        volid => 'dir:111/subvol-111-disk-0.subvol',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
        },
        allowed_types => {
            'images' => 1,
            'rootdir' => 1,
        },
    },
    # test different VM IDs
    {
        volid => 'dir:backup/vzdump-qemu-200-2025_07_29-13_00_55.vma',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => {
            'backup' => 1,
        },
    },
    {
        volid => 'dir:200/vm-200-disk-0.qcow2',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => {
            'images' => 1,
            'rootdir' => 1,
        },
    },
    {
        volid => 'dir:backup/vzdump-qemu-200-2025_07_29-13_00_55.vma',
        vmid => 200,
        denied_users => {},
        allowed_types => {
            'backup' => 1,
        },
    },
    {
        volid => 'dir:200/vm-200-disk-0.qcow2',
        vmid => 200,
        denied_users => {},
        allowed_types => {
            'images' => 1,
            'rootdir' => 1,
        },
    },
    {
        volid => 'dir:backup/vzdump-qemu-200-2025_07_29-13_00_55.vma',
        vmid => 300,
        denied_users => {
            'noperm@pve' => 1,
            'otherstorage@pve' => 1,
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => {
            'backup' => 1,
        },
    },
    {
        volid => 'dir:200/vm-200-disk-0.qcow2',
        vmid => 300,
        denied_users => {
            'noperm@pve' => 1,
            'otherstorage@pve' => 1,
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => {
            'images' => 1,
            'rootdir' => 1,
        },
    },
    # test paths
    {
        volid => 'relative_path',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'dsallocate@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => $all_types,
    },
    {
        volid => '/absolute_path',
        denied_users => {
            'backup@pve' => 1,
            'dsaudit@pve' => 1,
            'dsallocate@pve' => 1,
            'vmuser@pve' => 1,
        },
        allowed_types => $all_types,
    },
);

my $cfg = PVE::Storage::config();

is(scalar(@users), 7, 'number of users');

for my $t (@tests) {
    my ($volid, $vmid, $expected_denied_users, $expected_allowed_types) =
        $t->@{qw(volid vmid denied_users allowed_types)};

    # certain users are always expected to be denied, except in the special case where VM ID is set
    $expected_denied_users->{'noperm@pve'} = 1 if !$vmid;
    $expected_denied_users->{'otherstorage@pve'} = 1 if !$vmid;

    for my $user (@users) {
        my $description = "user: $user, volid: $volid";
        $rpcenv->set_user($user);

        my $actual_denied;

        eval { PVE::Storage::check_volume_access($rpcenv, $user, $cfg, $vmid, $volid, undef); };
        if (my $err = $@) {
            $actual_denied = 1;
            note($@) if !$expected_denied_users->{$user} # log the error for easy analysis
        }

        is($actual_denied, $expected_denied_users->{$user}, $description);
    }

    for my $type (@types) {
        my $user = 'root@pam'; # type mismatch should not even work for root!

        my $description = "type $type, volid: $volid";
        $rpcenv->set_user($user);

        my $actual_allowed = 1;

        eval { PVE::Storage::check_volume_access($rpcenv, $user, $cfg, $vmid, $volid, $type); };
        if (my $err = $@) {
            $actual_allowed = undef;
            note($@) if $expected_allowed_types->{$type} # log the error for easy analysis
        }

        is($actual_allowed, $expected_allowed_types->{$type}, $description);
    }
}

my $storage_module = Test::MockModule->new('PVE::Storage');
$storage_module->mock(activate_storage => sub { });

my @backups = (
    'vzdump-qemu-100-2025_07_29-13_00_55.vma',
    'vzdump-qemu-100-2025_07_30-13_00_55.vma',
    'vzdump-qemu-200-2025_07_29-13_00_55.vma',
    'vzdump-qemu-200-before-100-upgrade.vma',
    'vzdump-100-renamed.tar.zst',
);

make_path("$storage_dir/dump");
PVE::Tools::file_set_contents("$storage_dir/dump/$_", '') for @backups;

my @volids = map { "dir:backup/$_" } @backups;
my $guest_100_backups = [sort @volids[0, 1]];
my $all_backups = [sort @volids];

# A dry run for one guest must not list renamed archives of other guests or of none just because
# their names contain its ID, as the content listing hides those from users of that guest.
my @prune_tests = (
    {
        vmid => 100,
        expected => {
            'root@pam' => $guest_100_backups,
            'dsallocate@pve' => $guest_100_backups,
            'backup@pve' => $guest_100_backups,
        },
    },
    {
        expected => {
            'root@pam' => $all_backups,
            'dsallocate@pve' => $all_backups,
        },
    },
);

for my $t (@prune_tests) {
    my ($vmid, $expected) = $t->@{qw(vmid expected)};

    for my $user (@users) {
        my $description = "prune dry run, user: $user, vmid: " . ($vmid // 'none');
        $rpcenv->set_user($user);

        my $param = { node => 'localhost', storage => 'dir', 'prune-backups' => 'keep-last=1' };
        $param->{vmid} = $vmid if defined($vmid);

        my $list = eval { PVE::API2::Storage::PruneBackups->dryrun($param) };
        note($@) if $@ && $expected->{$user}; # log the error for easy analysis
        my $actual = $list ? [sort map { $_->{volid} } $list->@*] : undef;

        is_deeply($actual, $expected->{$user}, $description);
    }
}

done_testing();
