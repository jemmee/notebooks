// javac ForeignModernTest.java
//
// java --enable-native-access=ALL-UNNAMED ForeignModernTest

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

public class ForeignModernTest {
    public static void main(String[] args) throws Throwable {
        // 1. Locate and load the compiled library
        SymbolLookup lib = SymbolLookup.libraryLookup(
                Path.of(System.getProperty("user.dir") + "/libforeign.dylib"),
                Arena.global());

        // 2. Find the address of the "function_call" function symbol
        MemorySegment functionAddress = lib.find("function_call")
                .orElseThrow(() -> new RuntimeException("Function not found"));

        // 3. Define the C function signature (Returns void, accepts two ints)
        FunctionDescriptor descriptor = FunctionDescriptor.ofVoid(
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT);

        // 4. Create a Java method handle linking to the native function
        Linker linker = Linker.nativeLinker();
        MethodHandle printSum = linker.downcallHandle(functionAddress, descriptor);

        // 5. Invoke the native C function natively
        System.out.println("Java: Invoking native function...");
        printSum.invokeExact(15, 27);
    }
}