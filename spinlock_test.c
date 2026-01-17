// gcc spinlock_test.c -o spinlock_test -lpthread
//
// ./spinlock_test

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

// Shared resources
long shared_counter = 0;
pthread_spinlock_t my_spinlock;

#define ITERATIONS 1000000

void *increment_task(void *arg) {
  char *thread_name = (char *)arg;

  for (int i = 0; i < ITERATIONS; i++) {
    // "Spin" here until the lock is acquired
    pthread_spin_lock(&my_spinlock);

    // Critical Section
    shared_counter++;

    pthread_spin_unlock(&my_spinlock);
  }

  printf("%s finished.\n", thread_name);
  return NULL;
}

int main() {
  pthread_t t1, t2;

  // Initialize the spinlock
  // PTHREAD_PROCESS_PRIVATE means it's only shared between threads of this
  // process
  pthread_spin_init(&my_spinlock, PTHREAD_PROCESS_PRIVATE);

  printf("Starting threads. Each will increment the counter %d times...\n",
         ITERATIONS);

  pthread_create(&t1, NULL, increment_task, "Thread A");
  pthread_create(&t2, NULL, increment_task, "Thread B");

  pthread_join(t1, NULL);
  pthread_join(t2, NULL);

  printf("Final Counter Value: %ld\n", shared_counter);
  printf("Expected Value:      %d\n", ITERATIONS * 2);

  // Clean up
  pthread_spin_destroy(&my_spinlock);

  return 0;
}