// https://github.com/dokan-dev/dokany/releases
// https://github.com/dokan-dev/dokany/releases/download/v2.3.1.1000/Dokan_x64.msi
//
// https://dotnet.microsoft.com
//
// dotnet --version
//
// dotnet add package DokanNet
//
// dotnet run

using DokanNet;
using DokanNet.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;

class DokanTest : IDokanOperations
{
    private const string HelloFile = @"\hello.txt";
    private static readonly byte[] HelloContent = System.Text.Encoding.UTF8.GetBytes(
            "Hello from Dokan on Windows!\r\n" +
            "Time:     2019\r\n" +
            "Location: Redwood City\r\n");

    private readonly ConsoleLogger logger = new ConsoleLogger();

    public void Cleanup(string fileName, IDokanFileInfo info) { }

    public void CloseFile(string fileName, IDokanFileInfo info) { }

    public NtStatus CreateDirectory(string fileName, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus CreateFile(string fileName, DokanNet.FileAccess access, FileShare share,
            FileMode mode, FileOptions options, FileAttributes attributes,
            IDokanFileInfo info)
        {
            if (fileName == "\\" || fileName == "")
            {
                // Root directory - always exists
                return NtStatus.Success;
            }

            if (fileName.Equals(HelloFile, StringComparison.OrdinalIgnoreCase))
            {
                if (mode == FileMode.CreateNew || mode == FileMode.Truncate)
                    return NtStatus.AccessDenied; // read-only demo

                if ((access & DokanNet.FileAccess.ReadData) != 0)
                    return NtStatus.Success;

                if (mode == FileMode.Open || mode == FileMode.OpenOrCreate)
                {
                    return NtStatus.Success;
                }
                return NtStatus.AccessDenied;
            }

            return NtStatus.ObjectNameNotFound;
        }

    public NtStatus DeleteDirectory(string fileName, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus DeleteFile(string fileName, IDokanFileInfo info)
        => NtStatus.AccessDenied;

        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            files = new List<FileInformation>();  // Always initialize!

            if (string.IsNullOrEmpty(fileName) || fileName == "\\")
            {
                files.Add(new FileInformation
                {
                    FileName = "hello.txt",
                    Attributes = FileAttributes.ReadOnly | FileAttributes.Archive,
                    CreationTime = DateTime.UtcNow,
                    LastAccessTime = DateTime.UtcNow,
                    LastWriteTime = DateTime.UtcNow,
                    Length = HelloContent.LongLength  // Use LongLength for safety
                });
            }

            return NtStatus.Success;
        }

    public NtStatus FindFilesWithPattern(string fileName, string searchPattern,
            out IList<FileInformation> files, IDokanFileInfo info)
        {
            files = new List<FileInformation>();
            return FindFiles(fileName, out files, info); // simple fallback
        }

    public  NtStatus FindStreams(string fileName, out IList<FileInformation> streams,
        IDokanFileInfo info)
    {
        streams = new List<FileInformation>();
        return NtStatus.NotImplemented;
    }

    public NtStatus FlushFileBuffers(string fileName, IDokanFileInfo info)
        => NtStatus.Success;

    public NtStatus GetDiskFreeSpace(out long freeBytesAvailable,
        out long totalNumberOfBytes, out long totalNumberOfFreeBytes,
        IDokanFileInfo info)
    {
        freeBytesAvailable = 512L * 1024 * 1024 * 1024;     // fake 512 GB free
        totalNumberOfBytes = 1024L * 1024 * 1024 * 1024;    // fake 1 TB total
        totalNumberOfFreeBytes = freeBytesAvailable;
        return NtStatus.Success;
    }

    public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo,
        IDokanFileInfo info)
    {
        fileInfo = new FileInformation();

        if (fileName == "\\" || fileName == "")
        {
            fileInfo.Attributes = FileAttributes.Directory;
            fileInfo.CreationTime = fileInfo.LastAccessTime = fileInfo.LastWriteTime = DateTime.Now;
            fileInfo.Length = 0;
            return NtStatus.Success;
        }

        if (fileName.Equals(HelloFile, StringComparison.OrdinalIgnoreCase))
        {
            fileInfo.Attributes = FileAttributes.ReadOnly | FileAttributes.Archive;
            fileInfo.CreationTime = fileInfo.LastAccessTime = fileInfo.LastWriteTime = DateTime.Now;
            fileInfo.Length = HelloContent.Length;
            return NtStatus.Success;
        }

        return NtStatus.ObjectNameNotFound;
    }

    public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security,
        AccessControlSections sections, IDokanFileInfo info)
    {
        security = null;
        return DokanResult.Success;
    }

    public NtStatus GetVolumeInformation(out string volumeName, out FileSystemFeatures features,
        out string fileSystemName, out uint maximumComponentLength,
        IDokanFileInfo info)
    {
        volumeName = "DokanTest FS";
        features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch
                    | FileSystemFeatures.PersistentAcls | FileSystemFeatures.SupportsRemoteStorage;
        fileSystemName = "DOKAN";
        maximumComponentLength = 256;
        return NtStatus.Success;
    }

    public NtStatus LockFile(string fileName, long byteOffset, long length,
        IDokanFileInfo info)
        => NtStatus.Success;

    public NtStatus Mounted(string mountPoint, IDokanFileInfo info)
    {
        logger.Info($"Hello filesystem mounted at {mountPoint}");
        return NtStatus.Success;
    }

    public NtStatus MoveFile(string oldName, string newName, bool replace,
        IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead,
        long offset, IDokanFileInfo info)
    {
        bytesRead = 0;

        if (!fileName.Equals(HelloFile, StringComparison.OrdinalIgnoreCase))
            return NtStatus.ObjectNameNotFound;

        if (offset >= HelloContent.Length)
            return NtStatus.EndOfFile;

        int toRead = Math.Min(buffer.Length, HelloContent.Length - (int)offset);
        Array.Copy(HelloContent, offset, buffer, 0, toRead);
        bytesRead = toRead;

        return NtStatus.Success;
    }

    public NtStatus SetAllocationSize(string fileName, long allocSize, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus SetEndOfFile(string fileName, long byteOffset, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus SetFileAttributes(string fileName, FileAttributes attributes,
        IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security,
        AccessControlSections sections, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus SetFileTime(string fileName, DateTime? creationTime,
        DateTime? lastAccessTime, DateTime? lastWriteTime, IDokanFileInfo info)
        => NtStatus.AccessDenied;

    public NtStatus UnlockFile(string fileName, long byteOffset, long length,
        IDokanFileInfo info)
        => NtStatus.Success;

    public NtStatus Unmounted(IDokanFileInfo info)
    {
        logger.Info("Hello filesystem unmounted");
        return NtStatus.Success;
    }

    public NtStatus Unmounted(string mountPoint, IDokanFileInfo info)
    {
        logger.Info($"Hello filesystem unmounted from {mountPoint}");
        return NtStatus.Success;
    }

    public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten,
        long offset, IDokanFileInfo info)
    {
        bytesWritten = 0;
        return NtStatus.AccessDenied; // read-only
    }
}

class Program
{
    static void Main(string[] args)
    {
        var dokanTest = new DokanTest();
        var logger = new NullLogger(); // Resolves the null warning

        // We use the Dokan class as a factory to create a DokanInstance
        // This is the most explicit way to mount in DokanNet 2.0+
        using (var dokan = new Dokan(logger))
        {
            // Create the builder
            var builder = new DokanInstanceBuilder(dokan)
                .ConfigureOptions(options =>
                {
                    options.Options = DokanOptions.DebugMode;
                    options.MountPoint = "M:\\";
                });

            // Build and run the instance
            // This starts the mount and blocks until the drive is unmounted
            using (var instance = builder.Build(dokanTest))
            {
                Console.WriteLine("Drive M:\\ is now live.");
                Console.WriteLine("Press any key to unmount and exit...");
                Console.ReadKey();
            }
        }
    }
}