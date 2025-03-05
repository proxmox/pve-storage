package PVE::Storage::Common;

use strict;
use warnings;

use PVE::JSONSchema;

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

1;
