// gcc -o bloom_filter_test bloom_filter_test.c
//
// ./bloom_filter_test

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FILTER_SIZE 1024 // Number of bits

typedef struct {
  unsigned char *bits;
  int size;
} BloomFilter;

// Hash Function 1: DJB2
unsigned long hash_djb2(const char *str) {
  unsigned long hash = 5381;
  int c;
  while ((c = *str++))
    hash = ((hash << 5) + hash) + c;
  return hash;
}

// Hash Function 2: SDBM
unsigned long hash_sdbm(const char *str) {
  unsigned long hash = 0;
  int c;
  while ((c = *str++))
    hash = c + (hash << 6) + (hash << 16) - hash;
  return hash;
}

// Initialize the filter
BloomFilter *create_filter(int size) {
  BloomFilter *filter = malloc(sizeof(BloomFilter));
  filter->size = size;
  filter->bits = calloc((size / 8) + 1, sizeof(unsigned char));
  return filter;
}

// Set a bit in the array
void set_bit(BloomFilter *f, unsigned long hash) {
  int pos = hash % f->size;
  f->bits[pos / 8] |= (1 << (pos % 8));
}

// Check if a bit is set
bool check_bit(BloomFilter *f, unsigned long hash) {
  int pos = hash % f->size;
  return f->bits[pos / 8] & (1 << (pos % 8));
}

// Add an item (uses both hashes)
void add_item(BloomFilter *f, const char *item) {
  set_bit(f, hash_djb2(item));
  set_bit(f, hash_sdbm(item));
}

// Query an item
bool is_in_filter(BloomFilter *f, const char *item) {
  return check_bit(f, hash_djb2(item)) && check_bit(f, hash_sdbm(item));
}

int main() {
  BloomFilter *my_filter = create_filter(FILTER_SIZE);

  // 1. Add items
  add_item(my_filter, "Gaia-BH1");
  add_item(my_filter, "Encryption");
  add_item(my_filter, "PIPEDA");

  // 2. Test items
  const char *test_cases[] = {"Gaia-BH1", "PIPEDA", "Black Hole",
                              "Data Breach"};

  for (int i = 0; i < 4; i++) {
    if (is_in_filter(my_filter, test_cases[i])) {
      printf("Checking '%s': POSSIBLY PRESENT\n", test_cases[i]);
    } else {
      printf("Checking '%s': DEFINITELY NOT PRESENT\n", test_cases[i]);
    }
  }

  free(my_filter->bits);
  free(my_filter);
  return 0;
}