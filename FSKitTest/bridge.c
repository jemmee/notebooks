// clang -dynamiclib -arch arm64 bridge.c -o libFSKitBridge.dylib
//
// cp libFSKitBridge.dylib bin/Debug/net10.0/

#include <stdio.h>

void start_fskit_demo() { printf("Hello from the Native Mac Bridge!\n"); }