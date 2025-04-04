package PVE::Storage::Common;

use strict;
use warnings;

use PVE::JSONSchema;
use PVE::Syscall;

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

PVE::JSONSchema::register_standard_option('pve-storage-image-format', {
    type => 'string',
    enum => ['raw', 'qcow2', 'subvol', 'vmdk'],
    description => "Format of the image.",
});

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

1;
