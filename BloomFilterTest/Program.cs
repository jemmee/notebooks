// https://github.com/dokan-dev/dokany/releases
// https://github.com/dokan-dev/dokany/releases/download/v2.3.1.1000/Dokan_x64.msi
//
// https://dotnet.microsoft.com
//
// dotnet --version
//
// dotnet run

using System;
using System.Collections;
using System.Security.Cryptography;
using System.Text;

class BloomFilter {
    private readonly BitArray _bitArray;
    private readonly int _hashCount;

    public BloomFilter(int size, int hashCount) {
        _bitArray = new BitArray(size);
        _hashCount = hashCount;
    }

    // A simple way to get multiple hashes for one string
    private int GetHash(string item, int seed) {
        using (var md5 = MD5.Create()) {
            byte[] inputBytes = Encoding.UTF8.GetBytes(item + seed);
            byte[] hashBytes = md5.ComputeHash(inputBytes);
            int hash = BitConverter.ToInt32(hashBytes, 0);
            return Math.Abs(hash % _bitArray.Count);
        }
    }

    public void Add(string item) {
        for (int i = 0; i < _hashCount; i++) {
            int index = GetHash(item, i);
            _bitArray.Set(index, true);
        }
    }

    public bool Contains(string item) {
        for (int i = 0; i < _hashCount; i++) {
            int index = GetHash(item, i);
            if (!_bitArray.Get(index)) return false; // Definitely not there
        }
        return true; // Might be there
    }
}

class BloomFilterTest {
    static void Main() {
        var filter = new BloomFilter(size: 100, hashCount: 3);

        // Items from the Gospels
        string[] gospelItems = { "Frankincense", "Myrrh", "Gold", "Bread", "Fish", "Hyssop" };

        Console.WriteLine("--- Adding items to the Bloom Filter ---");
        foreach (var item in gospelItems) {
            filter.Add(item);
            Console.WriteLine($"Added: {item}");
        }

        Console.WriteLine("\n--- Testing the Filter ---");
        string[] testItems = { "Bread", "Gold", "Computer", "Tesla" };

        foreach (var test in testItems) {
            bool exists = filter.Contains(test);
            string result = exists ? "MAYBE in the Gospels" : "DEFINITELY NOT in the Gospels";
            Console.WriteLine($"{test}: {result}");
        }
    }
}