#!/usr/bin/env python3
#
# python3 bloom_filter_test.py

import hashlib

class BloomFilterTest:
    def __init__(self, size=128, hash_count=4):
        self.size = size
        self.hash_count = hash_count
        # Initialize a bit-array of zeros
        self.bit_array = [0] * size

    def _get_hashes(self, item):
        """Generates k different indices for an item."""
        indices = []
        for i in range(self.hash_count):
            # Create a unique salt for each hash pass
            combined = f"{item}{i}".encode('utf-8')
            # Use MD5 to get a consistent large number
            hex_digest = hashlib.md5(combined).hexdigest()
            # Convert to integer and map to our bit_array size
            idx = int(hex_digest, 16) % self.size
            indices.append(idx)
        return indices

    def record_judge(self, name):
        """Adds a Judge to our historical record."""
        for idx in self._get_hashes(name):
            self.bit_array[idx] = 1

    def is_remembered(self, name):
        """Checks if a name is in our historical record."""
        for idx in self._get_hashes(name):
            if self.bit_array[idx] == 0:
                return False  # Definitely forgotten
        return True  # Possibly remembered

# --- Execution ---

# Setup: 128 bits, 4 hash functions
history = BloomFilterTest(128, 4)

# Data from the Book of Judges
famous_judges = ["Othniel", "Ehud", "Deborah", "Gideon", "Jephthah", "Samson"]

print("--- Recording the Judges of Israel ---")
for judge in famous_judges:
    history.record_judge(judge)
    print(f"Recorded: {judge}")

print("\n--- Testing the Memory of the Filter ---")
test_names = [
    "Samson",    # Should be present
    "Deborah",   # Should be present
    "Delilah",   # Adversary (Not added)
    "Eglon",     # Adversary (Not added)
    "Gideon"     # Should be present
]

for name in test_names:
    remembered = history.is_remembered(name)
    status = "POSSIBLY REMEMBERED" if remembered else "DEFINITELY FORGOTTEN"
    print(f"{name:<12} : {status}")