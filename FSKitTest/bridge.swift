// swiftc -emit-library -o libFSKitBridge.dylib bridge.swift -target arm64-apple-macosx15.4
//
// cp libFSKitBridge.dylib bin/Debug/net10.0/

import Foundation
import FSKit
import UserNotifications

// The @_cdecl attribute makes this visible to C#
@_cdecl("start_fskit_demo")
public func startFskitDemo() {
    print("🚀 Swift Bridge: Initializing FSKit...")
    
    // In a real app, you would define an FSUnaryFileSystem here.
    // For now, we prove we can access Apple's FSKit framework.
    let fileName = FSFileName(data: "hello.txt".data(using: .utf8)!)
    print("✅ Successfully created an FSKit FSFileName: \(fileName)")
}

@_cdecl("get_macos_version")
public func getMacVersion() -> Int32 {
    return Int32(ProcessInfo.processInfo.operatingSystemVersion.majorVersion)
}

@_cdecl("mount_drive_with_name")
public func mountDrive(name: UnsafePointer<Int8>) {
    let driveName = String(cString: name)
    print("💎 C# requested a drive named: \(driveName)")
}

@_cdecl("force_mount_signal")
public func forceMountSignal() {
    // Check if we are running in a bundle
    if Bundle.main.bundleIdentifier == nil {
        print("⚠️ Warning: Running without an App Bundle. Notifications will be suppressed by macOS.")
        print("💎 C# requested the mount, but I can't show a popup yet.")
        return 
    }
    
    let center = UNUserNotificationCenter.current()
    
    // 1. Request Permission (Modern macOS requirement)
    center.requestAuthorization(options: [.alert, .sound]) { granted, error in
        if granted {
            print("✅ Notification permission granted.")
            
            // 2. Create the content
            let content = UNMutableNotificationContent()
            content.title = "FSKit Drive"
            content.body = "MyMagicDrive is attempting to mount."
            content.sound = .default
            
            // 3. Create a trigger (deliver immediately)
            let trigger = UNTimeIntervalNotificationTrigger(timeInterval: 1, repeats: false)
            
            // 4. Create the request
            let request = UNNotificationRequest(identifier: "FSKitMount", content: content, trigger: trigger)
            
            // 5. Schedule the notification
            center.add(request) { error in
                if let error = error {
                    print("❌ Error scheduling notification: \(error)")
                }
            }
        } else {
            print("⚠️ Permission denied, but the Bridge is still alive. Continuing to FSKit logic...")
            // We can still try to mount the drive here!
        }
    }
}