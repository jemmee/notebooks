// javac BloomFilterTest.java
//
// java BloomFilterTest

import java.util.BitSet;

public class BloomFilterTest {
    private BitSet bits;
    private int size;
    private int hashCount;

    // Constructor
    public BloomFilterTest(int m, int k) {
        this.size = m;
        this.hashCount = k;
        this.bits = new BitSet(m);
    }

    // A simple hash function using the item's hashCode and a seed
    private int getHash(String item, int seed) {
        // Combining item and seed to create unique hashes
        int hash = (item + seed).hashCode();
        return Math.abs(hash % size);
    }

    // Add item to filter
    public void add(String item) {
        for (int i = 0; i < hashCount; i++) {
            bits.set(getHash(item, i));
        }
    }

    // Check if item might exist
    public boolean exists(String item) {
        for (int i = 0; i < hashCount; i++) {
            if (!bits.get(getHash(item, i))) {
                return false; // Definitely not there
            }
        }
        return true; // Might be there
    }

    public static void main(String[] args) {
        // Initialize: 100 bits, 3 hash functions
        BloomFilterTest bf = new BloomFilterTest(100, 3);

        // 1. New Testament Seed Items
        String[] additions = { "Fisher-Net", "Alabaster-Jar", "Thorns-Crown", "Seamless-Robe", "Five-Loaves" };

        System.out.println("--- Seeding Bloom Filter ---");
        for (String item : additions) {
            bf.add(item);
            System.out.println("Added: " + item);
        }

        // 2. Testing
        System.out.println("\n--- Testing Membership ---");
        String[] tests = { "Fisher-Net", "Thorns-Crown", "Golden-Calf", "Centurion-Spear", "Fisher-Boat" };

        for (String test : tests) {
            boolean result = bf.exists(test);
            String status = result ? "PROBABLY PRESENT" : "DEFINITELY ABSENT";
            System.out.printf("%-18s : %s%n", test, status);
        }
    }
}