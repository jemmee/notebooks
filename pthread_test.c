// gcc -o pthread_test pthread_test.c -lpthread
//
// ./pthread_test

#include <pthread.h>
#include <stdio.h>

void *print_message(void *ptr) {
  printf("Hello from the thread!\n");
  return NULL;
}

int main() {
  pthread_t thread1;

  // Create the thread
  pthread_create(&thread1, NULL, print_message, NULL);

  // Wait for the thread to finish
  pthread_join(thread1, NULL);

  printf("Thread finished. Back in main.\n");
  return 0;
}