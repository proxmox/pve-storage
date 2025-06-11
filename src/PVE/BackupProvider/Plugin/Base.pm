package PVE::BackupProvider::Plugin::Base;

use strict;
use warnings;

=head1 NAME

PVE::BackupProvider::Plugin::Base - Base Plugin for Backup Provider API

=head1 SYNOPSIS

    use base qw(PVE::BackupProvider::Plugin::Base);

=head1 DESCRIPTION

This module serves as the base for any module implementing the API that Proxmox
VE uses to interface with external backup providers. The API is used for
creating and restoring backups. A backup provider also needs to provide a
storage plugin for integration with the front-end. The API here is used by the
backup stack in the backend.

=head2 1. Backup API

In Proxmox VE, a backup job consists of backup tasks for individual guests.
There are methods for initialization and cleanup of the job, i.e. job_init() and
job_cleanup() and for each guest backup, i.e. backup_init() and
backup_cleanup().

The backup_get_mechanism() method is used to decide on the backup mechanism.
Currently, 'file-handle' or 'nbd' for VMs, and 'directory' for containers is
possible. The method also let's the plugin indicate whether to use a bitmap for
incremental VM backup or not. It is enough to implement one mechanism for VMs
and one mechanism for containers.

Next, there are methods for backing up the guest's configuration and data,
backup_vm() for VM backup and backup_container() for container backup.

Finally, some helpers like provider_name() for getting the name of the backup
provider and backup_handle_log_file() for handling the backup task log.

The backup transaction looks as follows:

First, job_init() is called that can be used to check backup server availability
and prepare the connection. Then for each guest backup_init() followed by
backup_vm() or backup_container() and finally backup_cleanup(). Afterwards
job_cleanup() is called. For containers, there is an additional
backup_container_prepare() call while still privileged. The actual
backup_container() call happens as the (unprivileged) container root user, so
that the file owner and group IDs match the container's perspective.

=head3 1.1 Backup Mechanisms

VM:

Access to the data on the VM's disk is made available via a "snapshot access"
abstraction. This is effectively a snapshot of the data from the time the backup
is started. New guest writes after the backup started do not affect this. The
"snapshot access" represents either the full image, or in case a bitmap is used,
the dirty parts of the image since the last time the bitmap was used for a
successful backup.

NOTE: If a bitmap is used, the "snapshot access" is really only the dirty parts
of the image. You have to query the bitmap to see which parts of the image are
accessible/present. Reading or doing any other operation (like querying the
block allocation status via NBD) outside of the dirty parts of the image will
result in an error. In particular, if there were no new writes since the last
successful backup, i.e. the bitmap is fully clean, then the image cannot be
accessed at all, you can only query the dirty bitmap.

After backing up each part of the disk, it should be discarded in the export to
avoid unnecessary space usage on the Proxmox VE side (there is an associated
fleecing image).

VM mechanism 'file-handle':

The snapshot access is exposed via a file descriptor. A subroutine to read the
dirty regions for incremental backup is provided as well.

VM mechanism 'nbd':

The snapshot access and, if used, bitmap are exported via NBD. For the
specification of the NBD metadata context for dirty bitmaps, see:
L<https://qemu.readthedocs.io/en/master/interop/nbd.html>

Container mechanism 'directory':

A copy or snapshot of the container's filesystem state is made available as a
directory.

=head2 2. Restore API

The restore_get_mechanism() method is used to decide on the restore mechanism.
Currently, 'qemu-img' for VMs, and 'directory' or 'tar' for containers are
possible. It is enough to implement one mechanism for VMs and one mechanism for
containers.

Next, methods for extracting the guest and firewall configuration and the
implementations of the restore mechanism via a pair of methods: an init method,
for making the data available to Proxmox VE and a cleanup method that is called
after restore.

=head3 2.1. Restore Mechanisms

VM mechanism 'qemu-img':

The backup provider gives a path to the disk image that will be restored. The
path needs to be something 'qemu-img' can deal with, e.g. can also be an NBD URI
or similar.

Container mechanism 'directory':

The backup provider gives the path to a directory with the full filesystem
structure of the container.

Container mechanism 'tar':

The backup provider gives the path to a (potentially compressed) tar archive
with the full filesystem structure of the container.

=head1 METHODS

=cut

# plugin methods

=over

=item C<new>

The constructor. Returns a blessed instance of the backup provider class.

Parameters:

=over

=item C<$storage_plugin>

The associated storage plugin class.

=item C<$scfg>

The storage configuration of the associated storage.

=item C<$storeid>

The storage ID of the associated storage.

=item C<$log_function>

The function signature is C<$log_function($log_level, $message)>. This log
function can be used to write to the backup task log in Proxmox VE.

=over

=item C<$log_level>

Either C<info>, C<warn> or C<err> for informational messages, warnings or error
messages.

=item C<$message>

The message to be printed.

=back

=back

=back

=cut

sub new {
    my ($class, $storage_plugin, $scfg, $storeid, $log_function) = @_;

    die "implement me in subclass";
}

=over

=item C<provider_name>

Returns the name of the backup provider. It will be printed in some log lines.

=back

=cut

sub provider_name {
    my ($self) = @_;

    die "implement me in subclass";
}

=over

=item C<job_init>

Called when the job is started. Can be used to check the backup server
availability and prepare for the upcoming backup tasks of individual guests. For
example, to establish a connection to be used during C<backup_container()> or
C<backup_vm()>.

Parameters:

=over

=item C<$start_time>

Unix time-stamp of when the job started.

=back

=back

=cut

sub job_init {
    my ($self, $start_time) = @_;

    die "implement me in subclass";
}

=over

=item C<job_cleanup>

Called when the job is finished to allow for any potential cleanup related to
the backup server. Called in both, success and failure scenarios.

=back

=cut

sub job_cleanup {
    my ($self) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_init>

Called before the backup of the given guest is made. The archive name is
determined for the backup task and returned to the caller via a hash reference:

    my $res = $backup_provider->backup_init($vmid, $vmtype, $start_time);
    my $archive_name = $res->{'archive-name'};

The archive name must contain only characters from the
C<$PVE::Storage::SAFE_CHAR_CLASS_RE> character class as well as forward slash
C</> and colon C<:>.

Use C<$self> to remember it for the C<backup_container()> or C<backup_vm()>
method that will be called later.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$vmtype>

The type of the guest being backed up. Currently, either C<qemu> or C<lxc>.

=item C<$start_time>

Unix time-stamp of when the guest backup started.

=back

=back

=cut

sub backup_init {
    my ($self, $vmid, $vmtype, $start_time) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_cleanup>

Called when the guest backup is finished. Called in both, success and failure
scenarios. In the success case, statistics about the task after completion of
the backup are returned via a hash reference. Currently, only the archive size
is part of the result:

    my $res = $backup_provider->backup_cleanup($vmid, $vmtype, $success, $info);
    my $stats = $res->{stats};
    my $archive_size = $stats->{'archive-size'};

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$vmtype>

The type of the guest being backed up. Currently, either C<qemu> or C<lxc>.
Might be C<undef> in phase C<abort> for certain error scenarios.

=item C<$success>

Boolean indicating whether the job was successful or not. Success means that all
individual guest backups were successful.

=item C<$info>

A hash reference with optional information. Currently, the error message in case
of a failure.

=over

=item C<< $info->{error} >>

Present if there was a failure. The error message indicating the failure.

=back

=back

=back

=cut

sub backup_cleanup {
    my ($self, $vmid, $vmtype, $success, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_get_mechanism>

Tell the caller what mechanism to use for backing up the guest. The backup
method for the guest, i.e. C<backup_vm> for guest type C<qemu> or
C<backup_container> for guest type C<lxc>, will later be called with
mechanism-specific information. See those methods for more information.

Returns the mechanism:

    my $mechanism = $backup_provider->backup_get_mechanism($vmid, $vmtype);

Currently C<nbd> and C<file-handle> for guest type C<qemu> and C<directory> for
guest type C<lxc> are possible. If there is no support for one of the guest
types, the method should either C<die> or return C<undef>.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$vmtype>

The type of the guest being backed up. Currently, either C<qemu> or C<lxc>.

=back

=back

=cut

sub backup_get_mechanism {
    my ($self, $vmid, $vmtype) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_handle_log_file>

Handle the backup's log file which contains the task log for the backup. For
example, a provider might want to upload a copy to the backup server.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$filename>

Path to the file with the backup log.

=back

=back

=cut

sub backup_handle_log_file {
    my ($self, $vmid, $filename) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_vm_query_incremental>

Queries which devices can be backed up in an incrementally.
If incremental backup is not supported, simply return nothing (or C<undef>).

It cannot be guaranteed that the device on the QEMU-side still has the bitmap
used for an incremental backup.
For example, the VM might not be running, or the device might have been resized
or detached and re-attached. The C<$volumes> parameter in C<backup_vm()>
will contain the effective bitmap mode, see the C<backup_vm()> method for
details.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$volumes>

Hash reference with information about the VM's volumes.

=over

=item C<< $volumes->{$device_name} >>

Hash reference with information about the VM volume associated to the device
C<$device_name>.

=over

=item C<< $volumes->{$device_name}->{size} >>

Size of the volume in bytes. If the size does not match what you expect on the
backup server side, the bitmap will not exist anymore on the QEMU side. In this
case, it can be decided early to use a new bitmap name, but it is also possible
to re-use the same name, in which case a bitmap with that name will be newly
created on the volume.

=back

=back

=back

Return value:

This should return a hash mapping the C<$device_name>s found in the C<$volumes>
hash to either C<new> (to create a new bitmap, or force recreation if one
already exists), C<use> (to use an existing bitmap, or create one if it does
not exist). Volumes which do not appear in the return value will not use a
bitmap and existing ones will be discarded.

=back

=cut

sub backup_vm_query_incremental {
    my ($self, $vmid, $volumes) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_vm>

Used when the guest type is C<qemu>. Back up the virtual machine's configuration
and volumes that were made available according to the mechanism returned by
C<backup_get_mechanism>. Returns when done backing up. Ideally, the method
should log the progress during backup.

Access to the data on the VM's disk is made available via a "snapshot access"
abstraction. This is effectively a snapshot of the data from the time the backup
is started. New guest writes after the backup started do not affect this. The
"snapshot access" represents either the full image, or in case a bitmap is used,
the dirty parts of the image since the last time the bitmap was used for a
successful backup.

NOTE: If a bitmap is used, the "snapshot access" is really only the dirty parts
of the image. You have to query the bitmap to see which parts of the image are
accessible/present. Reading or doing any other operation (like querying the
block allocation status via NBD) outside of the dirty parts of the image will
result in an error. In particular, if there were no new writes since the last
successful backup, i.e. the bitmap is fully clean, then the image cannot be
accessed at all, you can only query the dirty bitmap.

After backing up each part of the disk, it should be discarded in the export to
avoid unnecessary space usage on the Proxmox VE side (there is an associated
fleecing image).

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$guest_config>

The guest configuration as raw data.

=item C<$volumes>

Hash reference with information about the VM's volumes. Some parameters are
mechanism-specific.

=over

=item C<< $volumes->{$device_name} >>

Hash reference with information about the VM volume associated to the device
C<$device_name>. The device name needs to be remembered for restoring. The
device name is also the name of the NBD export when the C<nbd> mechanism is
used.

=item C<< $volumes->{$device_name}->{size} >>

Size of the volume in bytes.

=item C<< $volumes->{$device_name}->{'bitmap-mode'} >>

How a bitmap is used for the current volume.

=over

=item C<none>

No bitmap is used.

=item C<new>

A bitmap has been newly created on the volume.

=item C<reuse>

The bitmap with the same ID as requested is being re-used.

=back

=back

Meachanism-specific parameters for mechanism:

=over

=item C<file-handle>

=over

=item C<< $volumes->{$device_name}->{'file-handle'} >>

File handle the backup data can be read from. Discards should be issued via the
C<PVE::Storage::Common::deallocate()> function for ranges that already have been
backed-up successfully to reduce space usage on the source-side.

=item C<< $volumes->{$device_name}->{'next-dirty-region'} >>

A function that will return the offset and length of the next dirty region as a
two-element list. After the last dirty region, it will return C<undef>. If no
bitmap is used, it will return C<(0, $size)> and then C<undef>. If a bitmap is
used, these are the dirty regions according to the bitmap.

=back

=item C<nbd>

For the specification of the NBD metadata context for dirty bitmaps, see:
L<https://qemu.readthedocs.io/en/master/interop/nbd.html>

=over

=item C<< $volumes->{$device_name}->{'nbd-path'} >>

The path to the Unix socket providing the NBD export with the backup data and,
if a bitmap is used, bitmap data. Discards should be issued after reading the
data to reduce space usage on the source-side.

=item C<< $volumes->{$device_name}->{'bitmap-name'} >>

The name of the bitmap in case a bitmap is used.

=back

=back

=item C<$info>

A hash reference containing optional parameters.

Optional parameters:

=over

=item C<< $info->{'bandwidth-limit'} >>

The requested bandwidth limit. The value is in bytes/second. The backup
provider is expected to honor this rate limit for IO on the backup source and
network traffic. A value of C<0>, C<undef> or if there is no such key in the
hash all mean that there is no limit.

=item C<< $info->{'firewall-config'} >>

Present if the firewall configuration exists. The guest's firewall
configuration as raw data.

=back

=back

=back

=cut

sub backup_vm {
    my ($self, $vmid, $guest_config, $volumes, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_container_prepare>

Called right before C<backup_container()> is called. The method
C<backup_container()> is called as the ID-mapped root user of the container, so
as a potentially unprivileged user. The hook is still called as a privileged
user to allow for the necessary preparation.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$info>

The same information that's passed along to C<backup_container()>, see the
description there.

=back

=back

=cut

sub backup_container_prepare {
    my ($self, $vmid, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<backup_container>

Used when the guest type is C<lxc>. Back up the container filesystem structure
that is made available for the mechanism returned by C<backup_get_mechanism>.
Returns when done backing up. Ideally, the method should log the progress during
backup.

Note that this method is executed as the ID-mapped root user of the container,
so a potentially unprivileged user. The ID is passed along as part of C<$info>.
Use the C<backup_container_prepare()> method for preparation. For example, to
make credentials available to the potentially unprivileged user.

Note that changes to C<$self> made during this method will not be visible in
later method calls. This is because the method is executed in a separate
execution context after forking. Use the C<backup_container_prepare()> method
if you need persistent changes to C<$self>.

Parameters:

=over

=item C<$vmid>

The ID of the guest being backed up.

=item C<$guest_config>

Guest configuration as raw data.

=item C<$exclude_patterns>

A list of glob patterns of files and directories to be excluded. C<**> is used
to match current directory and subdirectories. See also the following (note
that PBS implements more than required here, like explicit inclusion when
starting with a C<!>):
L<vzdump documentation|https://pve.proxmox.com/pve-docs/chapter-vzdump.html#_file_exclusions>
and
L<PBS documentation|https://pbs.proxmox.com/docs/backup-client.html#excluding-files-directories-from-a-backup>

=item C<$info>

A hash reference containing optional and mechanism-specific parameters.

Optional parameters:

=over

=item C<< $info->{'bandwidth-limit'} >>

The requested bandwidth limit. The value is in bytes/second. The backup
provider is expected to honor this rate limit for IO on the backup source and
network traffic. A value of C<0>, C<undef> or if there is no such key in the
hash all mean that there is no limit.

=item C<< $info->{'firewall-config'} >>

Present if the firewall configuration exists. The guest's firewall
configuration as raw data.

=back

Mechanism-specific parameters for mechanism:

=over

=item C<directory>

=over

=item C<< $info->{directory} >>

Path to the directory with the container's file system structure.

=item C<< $info->{sources} >>

List of paths (for separate mount points, including "." for the root) inside the
directory to be backed up.

=item C<< $info->{'backup-user-id'} >>

The user ID of the ID-mapped root user of the container. For example, C<100000>
for unprivileged containers by default.

=back

=back

=back

=back

=cut

sub backup_container {
    my ($self, $vmid, $guest_config, $exclude_patterns, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_get_mechanism>

Tell the caller what mechanism to use for restoring the guest. The restore
methods for the guest, i.e. C<restore_qemu_img_init> and
C<restore_qemu_img_cleanup> for guest type C<qemu>, or C<restore_container_init>
and C<restore_container_cleanup> for guest type C<lxc> will be called with
mechanism-specific information and their return value might also depend on the
mechanism. See those methods for more information. Returns
C<($mechanism, $vmtype)>:

=over

=item C<$mechanism>

Currently, C<'qemu-img'> for guest type C<'qemu'> and either C<'tar'> or
C<'directory'> for type C<'lxc'> are possible.

=item C<$vmtype>

Either C<qemu> or C<lxc> depending on what type the guest in the backed-up
archive is.

=back

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=back

=back

=cut

sub restore_get_mechanism {
    my ($self, $volname) = @_;

    die "implement me in subclass";
}

=over

=item C<archive_get_guest_config>

Extract the guest configuration from the given backup. Returns the raw contents
of the backed-up configuration file. Note that this method is called
independently from C<restore_container_init()> or C<restore_vm_init()>.

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=back

=back

=cut

sub archive_get_guest_config {
    my ($self, $volname) = @_;

    die "implement me in subclass";
}

=over

=item C<archive_get_firewall_config>

Extract the guest's firewall configuration from the given backup. Returns the
raw contents of the backed-up configuration file. Returns C<undef> if there is
no firewall config in the archive, C<die> if the configuration can't be
extracted. Note that this method is called independently from
C<restore_container_init()> or C<restore_vm_init()>.

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=back

=back

=cut

sub archive_get_firewall_config {
    my ($self, $volname) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_vm_init>

Prepare a VM archive for restore. Returns the basic information about the
volumes in the backup as a hash reference with the following structure:

    {
	$device_nameA => { size => $sizeA },
	$device_nameB => { size => $sizeB },
	...
    }

=over

=item C<$device_name>

The device name that was given as an argument to the backup routine when the
backup was created.

=item C<$size>

The virtual size of the VM volume that was backed up. A volume with this size is
created for the restore operation. In particular, for the C<qemu-img> mechanism,
this should be the size of the block device referenced by the C<qemu-img-path>
returned by C<restore_vm_volume>.

=back

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=back

=back

=cut

sub restore_vm_init {
    my ($self, $volname) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_vm_cleanup>

For VM backups, clean up after the restore. Called in both, success and
failure scenarios.

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=back

=back

=cut

sub restore_vm_cleanup {
    my ($self, $volname) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_vm_volume_init>

Prepare a VM volume in the archive for restore. Returns a hash reference with
the mechanism-specific information for the restore:

=over

=item C<qemu-img>

    { 'qemu-img-path' => $path }

The volume will be restored using the C<qemu-img convert> command.

=over

=item C<$path>

A path to the volume that C<qemu-img> can use as a source for the
C<qemu-img convert> command. For example, the path could also be an NBD URI. The
image contents are interpreted as being in C<raw> format and copied verbatim.
Other formats like C<qcow2> will not be detected currently.

=back

=back

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=item C<$device_name>

The device name associated to the volume that should be prepared for the
restore. Same as the argument to the backup routine when the backup was created.

=item C<$info>

A hash reference with optional and mechanism-specific parameters. Currently
empty.

=back

=back

=cut

sub restore_vm_volume_init {
    my ($self, $volname, $device_name, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_vm_volume_cleanup>

For VM backups, clean up after the restore of a given volume. Called in both,
success and failure scenarios.

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=item C<$device_name>

The device name associated to the volume that should be prepared for the
restore. Same as the argument to the backup routine when the backup was created.

=item C<$info>

A hash reference with optional and mechanism-specific parameters. Currently
empty.

=back

=back

=cut

sub restore_vm_volume_cleanup {
    my ($self, $volname, $device_name, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_container_init>

Prepare a container archive for restore. Returns a hash reference with the
mechanism-specific information for the restore:

=over

=item C<tar>

    { 'tar-path' => $path }

The archive will be restored via the C<tar> command.

=over

=item C<$path>

The path to the tar archive containing the full filesystem structure of the
container.

=back

=item C<directory>

    { 'archive-directory' => $path }

The archive will be restored via C<rsync> from a directory containing the full
filesystem structure of the container.

=over

=item C<$path>

The path to the directory containing the full filesystem structure of the
container.

=back

=back

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=item C<$info>

A hash reference with optional and mechanism-specific parameters. Currently
empty.

=back

=back

=cut

sub restore_container_init {
    my ($self, $volname, $info) = @_;

    die "implement me in subclass";
}

=over

=item C<restore_container_cleanup>

For container backups, clean up after the restore. Called in both, success and
failure scenarios.

Parameters:

=over

=item C<$volname>

The volume ID of the archive being restored.

=item C<$info>

A hash reference with optional and mechanism-specific parameters. Currently
empty.

=back

=back

=cut

sub restore_container_cleanup {
    my ($self, $volname, $info) = @_;

    die "implement me in subclass";
}

1;
