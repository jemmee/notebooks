// gcc -Wall -O2 -o interrupt_test interrupt_test.c
//
// ./interrupt_test
//
// In another terminal you can send signals:
//     kill -USR1 <pid>
//     kill -USR2 <pid>
//     kill -TERM <pid>   (to exit)

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

volatile sig_atomic_t usr1_count = 0;
volatile sig_atomic_t usr2_count = 0;
volatile sig_atomic_t running = 1;

static const char *timestamp(void) {
  static char buf[32];
  time_t now = time(NULL);
  struct tm *t = localtime(&now);
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);
  return buf;
}

void handler_usr1(int sig) {
  (void)sig;
  usr1_count++;
  printf("[%s] SIGUSR1 received (#%d)\n", timestamp(), usr1_count);
}

void handler_usr2(int sig) {
  (void)sig;
  usr2_count++;
  printf("[%s] SIGUSR2 received (#%d)\n", timestamp(), usr2_count);
}

void handler_term(int sig) {
  (void)sig;
  printf("\n[%s] SIGTERM received → shutting down...\n", timestamp());
  running = 0;
}

int main(void) {
  struct sigaction sa;

  printf("Process PID: %d\n", getpid());
  printf("Send signals:\n");
  printf("  kill -USR1 %d     → count SIGUSR1\n", getpid());
  printf("  kill -USR2 %d     → count SIGUSR2\n", getpid());
  printf("  kill -TERM %d     → exit gracefully\n\n", getpid());

  // Prepare signal action structure
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = handler_usr1;
  sa.sa_flags = SA_RESTART; // important: restart interrupted syscalls

  if (sigaction(SIGUSR1, &sa, NULL) == -1) {
    perror("sigaction(SIGUSR1)");
    return EXIT_FAILURE;
  }

  sa.sa_handler = handler_usr2;
  if (sigaction(SIGUSR2, &sa, NULL) == -1) {
    perror("sigaction(SIGUSR2)");
    return EXIT_FAILURE;
  }

  sa.sa_handler = handler_term;
  if (sigaction(SIGTERM, &sa, NULL) == -1) {
    perror("sigaction(SIGTERM)");
    return EXIT_FAILURE;
  }

  printf("Waiting for signals... (press Ctrl+C or send SIGTERM to exit)\n");
  printf("------------------------------------------------------------\n");

  while (running) {
    printf("[%s] Still alive...   SIGUSR1: %d   SIGUSR2: %d\r", timestamp(),
           usr1_count, usr2_count);
    fflush(stdout);
    sleep(1);
  }

  printf("\nFinal counters:\n");
  printf("  SIGUSR1: %d times\n", usr1_count);
  printf("  SIGUSR2: %d times\n", usr2_count);
  printf("Goodbye!\n");

  return EXIT_SUCCESS;
}