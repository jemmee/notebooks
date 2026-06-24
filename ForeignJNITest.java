// javac -h . ForeignJNITest.java
//
// java -Djava.library.path=. ForeignJNITest

public class ForeignJNITest {
    // Tell the JVM a native implementation exists elsewhere
    public native void functionCall(int a, int b);

    static {
        // Loads libhelloJni.so/dylib/dll
        System.loadLibrary("foreignjni");
    }

    public static void main(String[] args) {
        new ForeignJNITest().functionCall(40, 2);
    }
}
