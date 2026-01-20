// rustc BloomFilterTest.rs
//
// ./BloomFilterTest

struct BloomFilterTest {
    bits: Vec<u8>,
    size: usize,      // Number of bits
    hash_count: u32,
}

impl BloomFilterTest {
    fn new(size: usize, hash_count: u32) -> Self {
        // We need (size/8) bytes to store 'size' bits
        let byte_count = (size + 7) / 8;
        BloomFilterTest {
            bits: vec![0; byte_count],
            size,
            hash_count,
        }
    }

    fn get_hash(&self, item: &str, seed: u32) -> usize {
        // A simple hash: Fowler-Noll-Vo (FNV) style logic
        let mut h: u64 = 0x811c9dc5 ^ (seed as u64);
        for byte in item.bytes() {
            h = h.wrapping_mul(0x01000193);
            h ^= byte as u64;
        }
        (h as usize) % self.size
    }

    fn add(&mut self, item: &str) {
        for i in 0..self.hash_count {
            let idx = self.get_hash(item, i);
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            // Set the specific bit to 1 using bitwise OR
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    fn exists(&self, item: &str) -> bool {
        for i in 0..self.hash_count {
            let idx = self.get_hash(item, i);
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            // Check if the bit is 0 using bitwise AND
            if (self.bits[byte_idx] & (1 << bit_idx)) == 0 {
                return false; // Definitely not there
            }
        }
        true // Might be there
    }
}

fn main() {
    let mut bf = BloomFilterTest::new(100, 3);

    // Seeding with New Testament items
    let additions = vec!["Fisher-Net", "Alabaster-Jar", "Thorns-Crown", "Seamless-Robe"];
    
    println!("--- Seeding Rust Bloom Filter ---");
    for item in additions {
        bf.add(item);
        println!("Added: {}", item);
    }

    println!("\n--- Testing Membership ---");
    let tests = vec!["Fisher-Net", "Thorns-Crown", "Golden-Calf", "Centurion-Spear", "Fisher-Boat"];

    for test in tests {
        let status = if bf.exists(test) {
            "PROBABLY PRESENT"
        } else {
            "DEFINITELY ABSENT"
        };
        println!("{:<18} : {}", test, status);
    }
}