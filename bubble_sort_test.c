// gcc bubble_sort_test.c -o bubble_sort_test
//
// ./bubble_sort_test

#include <stdio.h>

void bubble_sort(int array[], int size) {
  for (int i = 0; i < size - 1; i++) {
    for (int j = 0; j < size - i - 1; j++) {
      if (array[j] > array[j + 1]) {
        int temp = array[j];
        array[j] = array[j + 1];
        array[j + 1] = temp;
      }
    }
  }
}

void print_array(int array[], int size) {
  printf("[");
  for (int i = 0; i < size; i++) {
    printf("%d", array[i]);
    if (i < size - 1) {
      printf(", ");
    }
  }
  printf("]");
}

int main() {
  int array[] = {64, 34, 25, 12, 22, 11, 90};

  printf("Original array:\n");
  print_array(array, sizeof(array) / sizeof(array[0]));
  printf("\n");

  bubble_sort(array, sizeof(array) / sizeof(array[0]));

  printf("\n");
  printf("Sorted array:\n");
  print_array(array, sizeof(array) / sizeof(array[0]));
  printf("\n");

  return 0;
}