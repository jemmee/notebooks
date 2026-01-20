// go run BloomFilterTest.go

package main

import (
	"fmt"
	"hash/fnv"
)

// BloomFilterTest struct
type BloomFilterTest struct {
	bits      []byte
	m         uint32 // Total bits
	hashCount uint32 // Number of hashes (k)
}

// NewBloomFilterTest initializes a filter with m bits and k hashes
func NewBloomFilterTest(m, k uint32) *BloomFilterTest {
	// Size the slice to hold m bits (rounded up to nearest byte)
	return &BloomFilterTest{
		bits:      make([]byte, (m+7)/8),
		m:         m,
		hashCount: k,
	}
}

// getHash returns a bit index for a specific seed
func (bf *BloomFilterTest) getHash(item string, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(item))
	// We add the seed to the hash to simulate multiple hash functions
	return (h.Sum32() + seed*0x9e3779b9) % bf.m
}

// Add an item to the filter
func (bf *BloomFilterTest) Add(item string) {
	for i := uint32(0); i < bf.hashCount; i++ {
		idx := bf.getHash(item, i)
		// Locate the byte and the specific bit inside it
		bf.bits[idx/8] |= (1 << (idx % 8))
	}
}

// Exists checks if an item might be in the filter
func (bf *BloomFilterTest) Exists(item string) bool {
	for i := uint32(0); i < bf.hashCount; i++ {
		idx := bf.getHash(item, i)
		if bf.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false // Definitely not there
		}
	}
	return true // Probably there
}

func main() {
	// 100 bits, 3 hashes
	bf := NewBloomFilterTest(100, 3)

	// New Testament Data
	additions := []string{"Fisher-Net", "Alabaster-Jar", "Thorns-Crown", "Seamless-Robe"}

	fmt.Println("--- Seeding Go Bloom Filter ---")
	for _, item := range additions {
		bf.Add(item)
		fmt.Printf("Added: %s\n", item)
	}

	fmt.Println("\n--- Testing Membership ---")
	tests := []string{"Fisher-Net", "Thorns-Crown", "Golden-Calf", "Centurion-Spear", "Fisher-Boat"}

	for _, test := range tests {
		status := "DEFINITELY ABSENT"
		if bf.Exists(test) {
			status = "PROBABLY PRESENT"
		}
		fmt.Printf("%-18s : %s\n", test, status)
	}
}
