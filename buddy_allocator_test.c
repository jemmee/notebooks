// gcc buddy_allocator_test.c -o buddy_allocator_test
//
// ./buddy_allocator_test

#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#define MAX_MEM 128 // Total simulated memory units (e.g., KB)
#define MIN_LEAF 16 // Smallest possible block size

typedef struct {
  int size;
  int is_allocated;
} Block;

// Function to find the smallest power of 2 greater than or equal to n
int next_power_of_2(int n) {
  int p = 1;
  if (n && !(n & (n - 1)))
    return n;
  while (p < n)
    p <<= 1;
  return p;
}

void allocate_buddy(int total_size, int request_size) {
  int target = next_power_of_2(request_size);

  // Ensure we don't request smaller than our minimum leaf size
  if (target < MIN_LEAF)
    target = MIN_LEAF;

  printf("--- Requesting %d units (Target block size: %d) ---\n", request_size,
         target);

  int current = total_size;
  while (current > target) {
    printf("Block size %d is too large. Splitting into two %d units...\n",
           current, current / 2);
    current /= 2;
  }

  printf("SUCCESS: Allocated a block of size %d.\n", current);
  printf("Internal Fragmentation: %d units.\n\n", current - request_size);
}

int main() {
  printf("Buddy Allocator Simulation (Total Memory: %d units)\n", MAX_MEM);
  printf("Minimum Block Size: %d units\n\n", MIN_LEAF);

  // Scenario 1: Requesting 20 units (Should get a 32 block)
  allocate_buddy(MAX_MEM, 20);

  // Scenario 2: Requesting 60 units (Should get a 64 block)
  allocate_buddy(MAX_MEM, 60);

  // Scenario 3: Requesting 10 units (Should get a 16 block - the MIN_LEAF)
  allocate_buddy(MAX_MEM, 10);

  return 0;
}