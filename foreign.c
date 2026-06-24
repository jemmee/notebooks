// gcc -shared -o libforeign.dylib -fPIC foreign.c

#include <stdio.h>

void function_call(int a, int b) {
  printf("Hello from C! The sum of %d and %d is: %d\n", a, b, (a + b));
}