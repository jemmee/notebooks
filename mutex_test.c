// gcc mutex_test.c -o mutex_test -lpthread
//
// ./mutex_test

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

// Shared resources
int balance = 0;
pthread_mutex_t lock; // The Mutex

#define DEPOSITS_PER_THREAD 1000000

void *deposit_money(void *arg) {
  for (int i = 0; i < DEPOSITS_PER_THREAD; i++) {
    // --- START CRITICAL SECTION ---
    pthread_mutex_lock(&lock);

    balance++; // Only one thread can be here at a time

    pthread_mutex_unlock(&lock);
    // --- END CRITICAL SECTION ---
  }
  return NULL;
}

int main() {
  pthread_t thread1, thread2;

  // Initialize the mutex
  if (pthread_mutex_init(&lock, NULL) != 0) {
    printf("Mutex init failed\n");
    return 1;
  }

  printf("Starting balance: %d\n", balance);

  // Create two threads performing deposits
  pthread_create(&thread1, NULL, deposit_money, NULL);
  pthread_create(&thread2, NULL, deposit_money, NULL);

  // Wait for both to finish
  pthread_join(thread1, NULL);
  pthread_join(thread2, NULL);

  printf("Final balance:    %d\n", balance);
  printf("Expected balance: %d\n", DEPOSITS_PER_THREAD * 2);

  // Clean up
  pthread_mutex_destroy(&lock);

  return 0;
}