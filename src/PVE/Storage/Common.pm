package PVE::Storage::Common;

use v5.36;

use PVE::JSONSchema;
use PVE::RPCEnvironment;
use PVE::Syscall;
use PVE::Tools qw(run_command);

use constant {
    FALLOC_FL_KEEP_SIZE => 0x01, # see linux/falloc.h
    FALLOC_FL_PUNCH_HOLE => 0x02, # see linux/falloc.h
};

=pod

=head1 NAME

PVE::Storage::Common - Shared functions and utilities for storage plugins and storage operations

=head1 DESCRIPTION

This module contains common subroutines that are mainly to be used by storage
plugins. This module's submodules contain subroutines that are tailored towards
a more specific or related purpose.

Functions concerned with storage-related C<PVE::SectionConfig> things, helpers
for the C<PVE::Storage> API can be found in this module. Functions that can't
be grouped in a submodule can also be found here.

=head1 SUBMODULES

=over

=back

=head1 STANDARD OPTIONS FOR JSON SCHEMA

=over

=back

=head3 pve-storage-image-format

Possible formats a guest image can have.

=cut

PVE::JSONSchema::register_standard_option(
    'pve-storage-image-format',
    {
        type => 'string',
        enum => ['raw', 'qcow2', 'subvol', 'vmdk'],
        description => "Format of the image.",
    },
);

=head3 pve-vm-image-format

Possible formats a VM image can have.

=cut

# TODO PVE 9 - Note that currently, qemu-server allows more formats for VM images, so third party
# storage plugins might potentially allow more too, but none of the plugins we are aware of do that.
# Those formats should either be allowed here or support for them should be phased out (at least in
# the storage layer). Can still be added again in the future, should any plugin provider request it.

PVE::JSONSchema::register_standard_option(
    'pve-vm-image-format',
    {
        type => 'string',
        enum => ['raw', 'qcow2', 'vmdk'],
        description => "Format of the image.",
    },
);

=pod

=head1 VOLUME TYPES

A volume that belongs to a guest has one of the following types.

=over

=item C<vm-vol>

A volume that a VM uses as one of its drives.

=item C<vm-state>

The saved state of a VM, from a snapshot or from hibernation.

=item C<ct-vol>

A volume that a container uses as its root file system or as a mount point.

=back

The in-tree plugins report the raw volume of a container as C<vm-vol>, as it is named like the
disk of a VM. Content listings go by the owning guest to report it as C<ct-vol>.

The types C<images> and C<rootdir> are generic. As content type of a storage, C<images> allows all
volume types of VMs and C<rootdir> all volume types of containers. As volume type, C<images> is
what a plugin reports that cannot tell the types of its volumes apart.

=cut

my $GENERIC_VTYPE = {
    'vm-vol' => 'images',
    'vm-state' => 'images',
    'ct-vol' => 'rootdir',
};

=pod

=head1 FUNCTIONS

=cut

=pod

=head3 generic_vtype

    $generic = generic_vtype($vtype)

Returns the generic type that includes the volume type C<$vtype>, or C<undef> if there is none.

=cut

sub generic_vtype : prototype($) ($vtype) {
    return $GENERIC_VTYPE->{$vtype};
}

=pod

=head3 is_guest_vtype

    $is_guest_vtype = is_guest_vtype($vtype)

Returns whether volumes of type C<$vtype> belong to a guest. These are the volume types of VMs and
containers, and C<images> for plugins that cannot tell them apart. The C<rootdir> volume type that
an external plugin may report is not one of them, callers that support it check for it themselves.

=cut

sub is_guest_vtype : prototype($) ($vtype) {
    return $vtype eq 'images' || defined($GENERIC_VTYPE->{$vtype});
}

=pod

=head3 vtype_matches

    $matches = vtype_matches($vtype, $expected)

Returns whether a volume of type C<$vtype> can be used where one of type C<$expected> is required.
This is the case for equal types, for a generic C<$expected> that includes C<$vtype>, and for a
C<$vtype> that is the generic type of C<$expected>, as such a volume comes from a plugin that cannot
tell its volume types apart.

=cut

sub vtype_matches : prototype($$) ($vtype, $expected) {
    return 1 if $vtype eq $expected;
    return 1 if ($GENERIC_VTYPE->{$vtype} // '') eq $expected;
    return 1 if ($GENERIC_VTYPE->{$expected} // '') eq $vtype;
    return 0;
}

=pod

=head3 content_allows

    $allowed = content_allows($content, $type)

Returns whether the content types in the hash C<$content>, as found in a storage configuration,
allow the content type C<$type>. A volume type is also allowed by its generic type, and a generic
C<$type> if at least one of its volume types is allowed.

=cut

sub content_allows : prototype($$) ($content, $type) {
    return 1 if $content->{$type};
    my $generic = $GENERIC_VTYPE->{$type};
    return 1 if defined($generic) && $content->{$generic};
    return 1 if grep { $GENERIC_VTYPE->{$_} eq $type && $content->{$_} } keys $GENERIC_VTYPE->%*;
    return 0;
}

=pod

=head3 align_size_up

    $aligned_size = align_size_up($size, $granularity)

Returns the next size bigger than or equal to C<$size> that is aligned with a
granularity of C<$granularity>. Prints a message if the aligned size is not
equal to the aligned size.

=cut

sub align_size_up : prototype($$) {
    my ($size, $granularity) = @_;

    my $padding = ($granularity - $size % $granularity) % $granularity;
    my $aligned_size = $size + $padding;
    print "size $size is not aligned to granularity $granularity, rounding up to $aligned_size\n"
        if $aligned_size != $size;
    return $aligned_size;
}

=pod

=head3 deallocate

    deallocate($file_handle, $offset, $length)

Deallocates the range with C<$length> many bytes starting from offset C<$offset>
for the file associated to the file handle C<$file_handle>. Dies on failure.

=cut

sub deallocate : prototype($$$) {
    my ($file_handle, $offset, $length) = @_;

    my $mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
    $offset = int($offset);
    $length = int($length);

    if (syscall(PVE::Syscall::fallocate, fileno($file_handle), $mode, $offset, $length) != 0) {
        die "fallocate: punch hole failed (offset: $offset, length: $length) - $!\n";
    }
}

my sub run_qemu_img_json {
    my ($cmd, $timeout) = @_;
    my $json = '';
    my $err_output = '';
    eval {
        run_command(
            $cmd,
            timeout => $timeout,
            outfunc => sub { $json .= shift },
            errfunc => sub { $err_output .= shift . "\n" },
        );
    };
    warn $@ if $@;
    if ($err_output) {
        # if qemu did not output anything to stdout we die with stderr as an error
        die $err_output if !$json;
        # otherwise we warn about it and try to parse the json
        warn $err_output;
    }
    return $json;
}

=pod

=head3 qemu_img_create

    qemu_img_create($fmt, $size, $path, $options)

Create a new qemu image with a specific format C<$format> and size C<$size> KiB for a target
C<$path>.

C<$options> currently allows setting the C<preallocation> value.

=cut

sub qemu_img_create {
    my ($fmt, $size, $path, $options) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'create'];

    push @$cmd, '-o', "preallocation=$options->{preallocation}"
        if defined($options->{preallocation});

    push @$cmd, '-f', $fmt, '--', $path, "${size}K";

    run_command($cmd, errmsg => "unable to create image");
}

=pod

=head3 qemu_img_create_qcow2_backed

    qemu_img_create_qcow2_backed($path, $backing_path, $backing_format, $options)

Create a new qemu qcow2 image C<$path> using an existing backing image C<$backing_path> with
backing format C<$backing_format>.

C<$options> currently allows setting the C<preallocation> value.

=cut

sub qemu_img_create_qcow2_backed {
    my ($path, $backing_path, $backing_format, $options) = @_;

    my $cmd = [
        '/usr/bin/qemu-img', 'create', '-F', $backing_format, '-b', $backing_path, '-f',
        'qcow2',
    ];

    # TODO make this configurable for all volumes/types and pass in via $options
    my $opts = ['extended_l2=on', 'cluster_size=128k'];

    push @$opts, "preallocation=$options->{preallocation}"
        if defined($options->{preallocation});
    push @$cmd, '-o', join(',', @$opts) if @$opts > 0;

    push @$cmd, '--', $path;

    run_command($cmd, errmsg => "unable to create image");
}

=pod

=head3 qemu_img_info

    qemu_img_info($filename, $file_format, $timeout, $follow_backing_files)

Returns a json with qemu image C<$filename> information with format <$file_format>. If
C<$follow_backing_files> option is defined, return a json with the whole chain of backing files
images.

=cut

sub qemu_img_info {
    my ($filename, $file_format, $timeout, $follow_backing_files) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'info', '--output=json'];
    push $cmd->@*, '-f', $file_format if $file_format;
    push $cmd->@*, '--backing-chain' if $follow_backing_files;
    push $cmd->@*, '--', $filename;

    return run_qemu_img_json($cmd, $timeout);
}

=pod

=head3 qemu_img_measure

    qemu_img_measure($size, $fmt, $timeout, $options)

Returns a json with the maximum size including all metadata overhead for an image with format
C<$fmt> and original size C<$size> KiB.

C<$options> allows specifying qemu-img options that might affect the sizing calculation, such as
cluster size.

=cut

sub qemu_img_measure {
    my ($size, $fmt, $timeout, $options) = @_;

    die "format is missing" if !$fmt;

    my $cmd = ['/usr/bin/qemu-img', 'measure', '--output=json', '--size', "${size}K", '-O', $fmt];
    if ($options) {
        push $cmd->@*, '-o', join(',', @$options) if @$options > 0;
    }
    return run_qemu_img_json($cmd, $timeout);
}

=pod

=head3 qemu_img_resize

    qemu_img_resize($path, $format, $size, $preallocation, $timeout)

Resize a qemu image C<$path> with format C<$format> to a target size C<$size> in bytes.
C<$preallocation> allows to specify the preallocation option for the resize operation. If no
C<$timeout> is provided, it defaults to 10 seconds, or 1 hour when running in a worker context.

=cut

sub qemu_img_resize {
    my ($path, $format, $size, $preallocation, $timeout) = @_;

    die "format is missing" if !$format;

    my $cmd = ['/usr/bin/qemu-img', 'resize'];
    push $cmd->@*, "--preallocation=$preallocation" if $preallocation;
    push $cmd->@*, '-f', $format, '--', $path, $size;

    if (!$timeout) {
        $timeout = PVE::RPCEnvironment->is_worker() ? 60 * 60 : 10;
    }
    run_command($cmd, timeout => $timeout);
}

1;
