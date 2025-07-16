package PVE::Storage::Common;

use strict;
use warnings;

use PVE::JSONSchema;
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

# TODO PVE 9 - Note that currently, qemu-server allows more formats for VM images, so third party
# storage plugins might potentially allow more too, but none of the plugins we are aware of do that.
# Those formats should either be allowed here or support for them should be phased out (at least in
# the storage layer). Can still be added again in the future, should any plugin provider request it.

PVE::JSONSchema::register_standard_option(
    'pve-storage-image-format',
    {
        type => 'string',
        enum => ['raw', 'qcow2', 'subvol', 'vmdk'],
        description => "Format of the image.",
    },
);

=pod

=head1 FUNCTIONS

=cut

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

Create a new qemu image with a specific format C<$format> and size C<$size> for a target C<$path>.

C<$options> currently allows setting the C<preallocation> value

=cut

sub qemu_img_create {
    my ($fmt, $size, $path, $options) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'create'];

    push @$cmd, '-o', "preallocation=$options->{preallocation}"
        if defined($options->{preallocation});

    push @$cmd, '-f', $fmt, $path, "${size}K";

    run_command($cmd, errmsg => "unable to create image");
}

=pod

=head3 qemu_img_create_qcow2_backed

    qemu_img_create_qcow2_backed($path, $backing_path, $backing_format, $options)

Create a new qemu qcow2 image C<$path> using an existing backing image C<$backing_path> with backing_format C<$backing_format>.

C<$options> currently allows setting the C<preallocation> value.

=cut

sub qemu_img_create_qcow2_backed {
    my ($path, $backing_path, $backing_format, $options) = @_;

    my $cmd = [
        '/usr/bin/qemu-img',
        'create',
        '-F',
        $backing_format,
        '-b',
        $backing_path,
        '-f',
        'qcow2',
        $path,
    ];

    # TODO make this configurable for all volumes/types and pass in via $options
    my $opts = ['extended_l2=on', 'cluster_size=128k'];

    push @$opts, "preallocation=$options->{preallocation}"
        if defined($options->{preallocation});
    push @$cmd, '-o', join(',', @$opts) if @$opts > 0;

    run_command($cmd, errmsg => "unable to create image");
}

=pod

=head3 qemu_img_info

    qemu_img_info($filename, $file_format, $timeout, $follow_backing_files)

Returns a json with qemu image C<$filename> informations with format <$file_format>.
If C<$follow_backing_files> option is defined, return a json with the whole chain
of backing files images.

=cut

sub qemu_img_info {
    my ($filename, $file_format, $timeout, $follow_backing_files) = @_;

    my $cmd = ['/usr/bin/qemu-img', 'info', '--output=json', $filename];
    push $cmd->@*, '-f', $file_format if $file_format;
    push $cmd->@*, '--backing-chain' if $follow_backing_files;

    return run_qemu_img_json($cmd, $timeout);
}

=pod

=head3 qemu_img_measure

    qemu_img_measure($size, $fmt, $timeout, $options)

Returns a json with the maximum size including all metadatas overhead for an image with format C<$fmt> and original size C<$size>Kb.

C<$options> allows specifying qemu-img options that might affect the sizing calculation, such as cluster size.

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

Resize a qemu image C<$path> with format C<$format> to a target Kb size C<$size>.
Default timeout C<$timeout> is 10s if not specified.
C<$preallocation> allows to specify the preallocation option for the resize operation.

=cut

sub qemu_img_resize {
    my ($path, $format, $size, $preallocation, $timeout) = @_;

    die "format is missing" if !$format;

    my $cmd = ['/usr/bin/qemu-img', 'resize'];
    push $cmd->@*, "--preallocation=$preallocation" if $preallocation;
    push $cmd->@*, '-f', $format, $path, $size;

    $timeout = 10 if !$timeout;
    run_command($cmd, timeout => $timeout);
}

1;
