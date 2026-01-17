// gcc slab_allocator_test.c -o slab_allocator_test
//
// ./slab_allocator_test

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define SLAB_SIZE 1024 // Total size of one slab (1KB)
#define OBJ_SIZE 128   // Size of each object in the slab
#define OBJS_PER_SLAB (SLAB_SIZE / OBJ_SIZE)

// A simple structure to represent an object in our slab
typedef struct {
  uint8_t data[OBJ_SIZE];
} NetworkPacket;

// The Slab structure
typedef struct {
  NetworkPacket *memory; // The actual block of memory (from Buddy Allocator)
  int free_list[OBJS_PER_SLAB]; // Indices of available objects
  int top;                      // Pointer to the next available index
} Slab;

// Initialize a slab (Simulates kmem_cache_create)
Slab *create_slab() {
  Slab *s = (Slab *)malloc(sizeof(Slab));
  s->memory = (NetworkPacket *)malloc(SLAB_SIZE);
  s->top = OBJS_PER_SLAB - 1;

  // Fill the free list: initially, all slots are free
  for (int i = 0; i < OBJS_PER_SLAB; i++) {
    s->free_list[i] = i;
  }
  printf("Slab created: %d slots of %d bytes available.\n", OBJS_PER_SLAB,
         OBJ_SIZE);
  return s;
}

// Allocate from slab (Simulates kmem_cache_alloc)
NetworkPacket *slab_alloc(Slab *s) {
  if (s->top < 0) {
    printf("Error: Slab is full! (In reality, we would request a new slab from "
           "Buddy)\n");
    return NULL;
  }
  int index = s->free_list[s->top--];
  printf("Allocated object at slot index: %d\n", index);
  return &s->memory[index];
}

// Free back to slab (Simulates kmem_cache_free)
void slab_free(Slab *s, int index) {
  if (s->top >= OBJS_PER_SLAB - 1)
    return;
  s->free_list[++s->top] = index;
  printf("Freed object at slot index: %d. Slot is now reused.\n", index);
}

int main() {
  printf("--- Slab Allocator Simulation ---\n");
  Slab *my_cache = create_slab();

  // 1. Allocate a few packets
  NetworkPacket *p1 = slab_alloc(my_cache);
  NetworkPacket *p2 = slab_alloc(my_cache);
  NetworkPacket *p3 = slab_alloc(my_cache);

  // 2. Free one packet
  slab_free(my_cache, 1); // Freeing p2 (index 1)

  // 3. Re-allocate
  // The allocator is efficient: it immediately reuses the slot we just freed
  NetworkPacket *p4 = slab_alloc(my_cache);

  return 0;
}