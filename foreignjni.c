// gcc -shared -fPIC -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/darwin" -o
// libforeignjni.dylib foreignjni.c

#include "ForeignJNITest.h"
#include <jni.h>
#include <stdio.h>

// The method name maps directly back to the Java package/class layout
JNIEXPORT void JNICALL Java_ForeignJNITest_functionCall(JNIEnv *env,
                                                        jobject obj, jint a,
                                                        jint b) {
  printf("Hello from JNI C! The sum is: %d\n", (a + b));
}