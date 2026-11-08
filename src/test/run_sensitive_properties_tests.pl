#!/usr/bin/perl

use v5.36;

use lib ('.', '..');

use Test::More;

use PVE::Storage;
use PVE::Storage::Plugin;

# Tools that keep secrets out of their output only know about the sensitive flag, not about
# 'sensitive-properties', so every property a plugin keeps out of storage.cfg must be flagged.
my $not_secret = {
    'master-pubkey' => 1, # public key, in a file only because the backup client reads it from one
};

for my $type (sort PVE::Storage::Plugin->lookup_types()->@*) {
    for my $property (PVE::Storage::Plugin::sensitive_properties($type)->@*) {
        next if $not_secret->{$property};

        my $schema = PVE::Storage::Plugin->get_property_schema($type, $property);
        ok($schema && $schema->{sensitive}, "$type: '$property' is marked as sensitive");
    }
}

done_testing();
