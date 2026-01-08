// gcc -o pi_test pi_test.c
//
// ./pi_test

#include <stdio.h>

int main() {
  long int terms = 10000000000; // More terms = higher accuracy
  double pi = 0.0;
  double denominator = 1.0;

  for (long int i = 0; i < terms; i++) {
    if (i % 2 == 0) {
      pi += 4.0 / denominator;
    } else {
      pi -= 4.0 / denominator;
    }
    denominator += 2.0;
  }

  printf("Approximated Pi over %d terms: %.10f\n", terms, pi);
  return 0;
}
