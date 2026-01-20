// node bloom_filter_test.js

const crypto = require('crypto');

class BloomFilterTest {
    constructor(m, k) {
        this.m = m; // Size in bits
        this.k = k; // Number of hashes
        // Buffer.alloc creates a zero-filled buffer. 
        // We need ceil(m/8) bytes to store m bits.
        this.buffer = Buffer.alloc(Math.ceil(m / 8));
    }

    // Helper to generate a hash index using a seed
    _getHash(item, seed) {
        const hash = crypto.createHash('md5')
            .update(item + seed)
            .digest();

        // Take the first 4 bytes of the MD5 and convert to an unsigned 32-bit int
        const hexHash = hash.readUInt32BE(0);
        return hexHash % this.m;
    }

    add(item) {
        for (let i = 0; i < this.k; i++) {
            const bitIndex = this._getHash(item, i);
            const byteIndex = Math.floor(bitIndex / 8);
            const bitWithinByte = bitIndex % 8;

            // Use bitwise OR to set the specific bit to 1
            this.buffer[byteIndex] |= (1 << bitWithinByte);
        }
    }

    exists(item) {
        for (let i = 0; i < this.k; i++) {
            const bitIndex = this._getHash(item, i);
            const byteIndex = Math.floor(bitIndex / 8);
            const bitWithinByte = bitIndex % 8;

            // Use bitwise AND to check if the bit is 0
            if ((this.buffer[byteIndex] & (1 << bitWithinByte)) === 0) {
                return false; // Definitely not present
            }
        }
        return true; // Probably present
    }
}

// --- Execution ---

const bloomFilterTest = new BloomFilterTest(128, 4);

// Imagery from the Song of Solomon
const additions = [
    "Rose-of-Sharon",   // 2:1
    "Lily-of-the-Valley",// 2:1
    "Cedar-Beams",       // 1:17
    "Tower-of-David",    // 4:4
    "Pomegranate",       // 4:3
    "Saffron"            // 4:14
];

console.log("--- Seeding Node.js Bloom Filter (Song of Solomon) ---");
additions.forEach(item => {
    bloomFilterTest.add(item);
    console.log(`Added: ${item}`);
});

console.log("\n--- Testing Membership ---");
const tests = ["Rose-of-Sharon", "Pomegranate", "Wild-Honey", "Vineyard", "Lily-of-the-Valley"];

tests.forEach(test => {
    const result = bloomFilterTest.exists(test) ? "MAYBE PRESENT" : "DEFINITELY ABSENT";
    console.log(`${test.padEnd(20)} : ${result}`);
});