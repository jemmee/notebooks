// javac BubbleSortTest.java
//
// java BubbleSortTest

public class BubbleSortTest {
    public static int[] bubbleSort(int[] array) {
        if (array != null) {
            for (int i = 0; i < array.length - 1; i++) {
                for (int j = 0; j < array.length - i - 1; j++) {
                    if (array[j] > array[j + 1]) {
                        int temp = array[j];
                        array[j] = array[j + 1];
                        array[j + 1] = temp;
                    }
                }
            }
        }
        return array;
    }

    public static String toString(int[] array) {
        if (array != null) {
            StringBuffer sb = new StringBuffer("");

            sb.append("[");
            for (int i = 0; i < array.length; i++) {
                sb.append(Integer.toString(array[i]));
                if (i < array.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            return sb.toString();
        } else {
            return null;
        }

    }

    public static void main(String[] args) {
        int[] array = { 64, 34, 25, 12, 22, 11, 90 };

        System.out.println("Original array:");
        System.out.println(toString(array));

        bubbleSort(array);

        System.out.println();
        System.out.println("Sorted array:");
        System.out.println(toString(array));
    }
}