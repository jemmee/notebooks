// https://dotnet.microsoft.com/en-us/download
//
// dotnet --version
//
// dotnet build
// dotnet run
//
// dotnet publish -r osx-arm64 -c Release
//
// mkdir -p FSKitTest.app/Contents/MacOS
// cp bin/Release/net10.0/osx-arm64/publish/FSKitTest FSKitTest.app/Contents/MacOS/
// cp libFSKitBridge.dylib FSKitTest.app/Contents/MacOS/
//
// ./FSKitTest.app/Contents/MacOS/FSKitTest

using System;
using System.Runtime.InteropServices;

namespace FSKitDemo
{
    internal partial class Program
    {
        // P/Invoke to our compiled Swift bridge
        [LibraryImport("libFSKitBridge.dylib")]
        private static partial void start_fskit_demo();

        [LibraryImport("libFSKitBridge.dylib")]
        private static partial int get_macos_version();

        [LibraryImport("libFSKitBridge.dylib", StringMarshalling = StringMarshalling.Utf8)]
        private static partial void mount_drive_with_name(string name);

        [LibraryImport("libFSKitBridge.dylib")]
        private static partial void force_mount_signal();

        static void Main(string[] args)
        {
            Console.WriteLine(".NET 10 FSKit Demo (Apple Silicon)");
            
            try 
            {
                // This starts the Apple FSKit service loop
                start_fskit_demo();
            }
            catch (DllNotFoundException)
            {
                Console.WriteLine("Error: libFSKitBridge.dylib not found. Ensure it's in the bin folder.");
            }

            // Comment out below to test with bridge.c
            Console.WriteLine($"macOS Version: {get_macos_version()}");

            mount_drive_with_name("MyMagicDrive");

            Console.WriteLine("C# is starting the mount process...");
        
            // HERE IS THE CALL:
            force_mount_signal();
        
            Console.WriteLine("Signal sent! Check your Mac notifications.");
        
            // Keep the app alive so the notification has time to pop up
            Console.ReadLine();
        }
    }
}