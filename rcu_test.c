// gcc rcu_test.c -o rcu_test -lpthread
//
// ./rcu_test

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct {
  int version;
  int data[5];
} Config;

// The global "RCU-protected" pointer
_Atomic(Config *) global_config;

// 1. READER: Accesses data without any locks
void *reader_thread(void *arg) {
  long id = (long)arg;
  for (int i = 0; i < 5; i++) {
    // RCU Read-Side: Just load the pointer atomically
    Config *current = atomic_load(&global_config);

    printf("Reader %ld: Reading Version %d (Value: %d)\n", id, current->version,
           current->data[0]);

    usleep(100000); // Simulate processing
  }
  return NULL;
}

// 2. WRITER: Copy -> Update -> Swap
void *writer_thread(void *arg) {
  for (int v = 1; v <= 2; v++) {
    usleep(150000); // Wait a bit before updating

    // A. Copy: Allocate new memory
    Config *old_config = atomic_load(&global_config);
    Config *new_config = malloc(sizeof(Config));

    // B. Update: Modify the new copy
    new_config->version = v;
    for (int i = 0; i < 5; i++)
      new_config->data[i] = v * 10;

    printf("\n*** Writer: Swapping to Version %d ***\n\n", v);

    // C. Swap: Atomically update the pointer
    atomic_store(&global_config, new_config);

    // D. Grace Period / Reclaim:
    // In a real kernel, we'd use synchronize_rcu().
    // Here, we just wait to ensure readers are done before freeing.
    sleep(1);
    free(old_config);
  }
  return NULL;
}

int main() {
  // Initialize initial config
  Config *initial = malloc(sizeof(Config));
  initial->version = 0;
  for (int i = 0; i < 5; i++)
    initial->data[i] = 0;
  atomic_store(&global_config, initial);

  pthread_t r1, r2, w1;

  pthread_create(&r1, NULL, reader_thread, (void *)1);
  pthread_create(&r2, NULL, reader_thread, (void *)2);
  pthread_create(&w1, NULL, writer_thread, NULL);

  pthread_join(r1, NULL);
  pthread_join(r2, NULL);
  pthread_join(w1, NULL);

  free(atomic_load(&global_config));
  return 0;
}