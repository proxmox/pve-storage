package PVE::Storage::Common;

use strict;
use warnings;

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
