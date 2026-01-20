#!/usr/bin/perl

# perl bloom_filter_test.pl
#
# ./bloom_filter_test.pl

use strict;
use warnings;
use Digest::MD5 qw(md5);

# --- Configuration ---
my $size       = 100; # Total bits (m)
my $hash_count = 3;   # Number of hashes (k)
my $bit_vector = "";  # The string that will hold our bits

# Initialize the vector (ensure it has enough space)
vec($bit_vector, $size - 1, 1) = 0;

sub get_hashes {
    my ($item) = @_;
    my @indices;

    for (my $i = 0; $i < $hash_count; $i++) {
        # Create a unique hash by combining item and seed
        # We use the first 4 bytes of an MD5 hash as an integer
        my $raw_hash = md5($item . $i);
        my $val = unpack("L", $raw_hash); # Unpack as unsigned long
        push @indices, $val % $size;
    }
    return @indices;
}

sub bloom_add {
    my ($item) = @_;
    foreach my $idx (get_hashes($item)) {
        # vec(EXPR, OFFSET, BITS)
        # Sets the bit at $idx to 1
        vec($bit_vector, $idx, 1) = 1;
    }
}

sub bloom_exists {
    my ($item) = @_;
    foreach my $idx (get_hashes($item)) {
        return 0 if vec($bit_vector, $idx, 1) == 0;
    }
    return 1;
}

# --- Execution ---

my @additions = ("Fisher-Net", "Alabaster-Jar", "Thorns-Crown", "Seamless-Robe");

print "--- Seeding Perl Bloom Filter ---\n";
foreach my $item (@additions) {
    bloom_add($item);
    print "Added: $item\n";
}

print "\n--- Testing Membership ---\n";
my @tests = ("Fisher-Net", "Thorns-Crown", "Golden-Calf", "Centurion-Spear", "Fisher-Boat");

foreach my $test (@tests) {
    my $found = bloom_exists($test);
    printf("%-18s : %s\n", $test, $found ? "PROBABLY PRESENT" : "DEFINITELY ABSENT");
}