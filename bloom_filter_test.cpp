// clang++ -std=c++17 -o bloom_filter_test bloom_filter_test.cpp
//
// ./bloom_filter_test

#include <bitset>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

class RuthBloomFilter {
public:
  // 128 bits total, managed as a single bitset
  static constexpr size_t M = 128;
  static constexpr size_t K = 3;

  void add(std::string_view item) {
    for (size_t i = 0; i < K; ++i) {
      bits.set(generate_hash(item, i));
    }
  }

  bool exists(std::string_view item) const {
    for (size_t i = 0; i < K; ++i) {
      if (!bits.test(generate_hash(item, i)))
        return false;
    }
    return true;
  }

private:
  std::bitset<M> bits;

  // Simple Jenkins-style hash with seed to create k-functions
  size_t generate_hash(std::string_view item, size_t seed) const {
    size_t h = seed + 0x9e3779b9;
    for (char c : item) {
      h ^= static_cast<size_t>(c) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h % M;
  }
};

int main() {
  RuthBloomFilter harvest;

  // Items and characters added to the harvest set
  std::vector<std::string> additions = {
      "Naomi",           // The mother-in-law
      "Boaz",            // The kinsman redeemer
      "Barley-Sheaves",  // The crop being harvested
      "Winnowing-Floor", // Where the grain is separated
      "Ephah-of-Barley", // The measure Ruth gathered
      "Ten-Elders"       // Witnesses at the gate
  };

  std::cout << "--- Seeding the Ruth Bloom Filter (The Harvest) ---\n";
  for (const auto &item : additions) {
    harvest.add(item);
    std::cout << "Gleaning: " << item << "\n";
  }

  std::cout << "\n--- Testing Membership in the Harvest ---\n";
  std::vector<std::string> tests = {
      "Boaz",          // Should be present
      "Naomi",         // Should be present
      "Orpah",         // Absent (she stayed in Moab)
      "Moab",          // Absent (the land they left)
      "Barley-Sheaves" // Should be present
  };

  for (const auto &test : tests) {
    bool result = harvest.exists(test);
    std::cout << std::left << std::setw(18) << test << " : "
              << (result ? "[MAYBE PRESENT]" : "[DEFINITELY ABSENT]") << "\n";
  }

  return 0;
}