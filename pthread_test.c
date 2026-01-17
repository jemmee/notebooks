// gcc -o pthread_test pthread_test.c -lpthread
//
// ./pthread_test

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NUM_THREADS 4

// Shared data
int global_counter = 0;
pthread_mutex_t counter_mutex;

// The function the threads will execute
void *perform_work(void *thread_id) {
  long id = (long)thread_id;

  for (int i = 0; i < 3; i++) {
    // Lock the mutex before touching shared data
    pthread_mutex_lock(&counter_mutex);

    global_counter++;
    printf("Thread #%ld updated counter to: %d\n", id, global_counter);

    // Unlock so other threads can use it
    pthread_mutex_unlock(&counter_mutex);

    // Sleep briefly to simulate work and allow thread interleaving
    usleep(100000);
  }

  pthread_exit(NULL);
}

int main() {
  pthread_t threads[NUM_THREADS];
  pthread_mutex_init(&counter_mutex, NULL);

  printf("Starting %d threads...\n", NUM_THREADS);

  // Create threads
  for (long t = 0; t < NUM_THREADS; t++) {
    int rc = pthread_create(&threads[t], NULL, perform_work, (void *)t);
    if (rc) {
      printf("Error: return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }

  // Wait for all threads to finish (joining)
  for (int t = 0; t < NUM_THREADS; t++) {
    pthread_join(threads[t], NULL);
  }

  printf("All threads complete. Final counter value: %d\n", global_counter);

  // Clean up
  pthread_mutex_destroy(&counter_mutex);
  return 0;
}