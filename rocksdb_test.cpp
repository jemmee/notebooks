// sudo dnf install rocksdb-devel bzip2-devel lz4-devel snappy-devel -y
//
// g++ -std=c++11 -l rocksdb -lsnappy -llz4 -lbz2 -lz -lpthread rocksdb_test.cpp
// -o rocksdb_test
//
// ./rocksdb_test

#include "rocksdb/db.h"
#include <iostream>
#include <string>

using namespace std;
using namespace rocksdb;

int main() {
  DB *db;
  Options options;
  // Optimize RocksDB for the current system
  options.create_if_missing = true;

  // 1. Open the database
  Status s = DB::Open(options, "./rocksdb_test.db", &db);
  if (!s.ok())
    cerr << s.ToString() << endl;

  // 2. Put a key-value pair
  string key = "1/JHN.3.16";
  string value =
      "For God so loved the world, that he gave his only begotten Son, "
      "that whosoever believeth in him should not perish, but have "
      "everlasting life.";
  s = db->Put(WriteOptions(), key, value);
  if (s.ok())
    cout << "Stored: [" << key << " -> " << value << "]" << endl;

  key = "111/JHN.3.16";
  value = "For God so loved the world that he gave his one and only Son, that "
          "whoever believes in him shall not perish but have eternal life.";
  s = db->Put(WriteOptions(), key, value);
  if (s.ok())
    cout << "Stored: [" << key << " -> " << value << "]" << endl;

  key = "83/JHN.3.16";
  value = "実に神は、ひとり子をさえ惜しまず与えるほどに、この世界を愛してくださ"
          "いました。それは、神の御子を信じる者が、だれ一人滅びず、永遠のいのち"
          "を得るためです。";
  s = db->Put(WriteOptions(), key, value);
  if (s.ok())
    cout << "Stored: [" << key << " -> " << value << "]" << endl;

  // 3. Get the value back
  string returned_value;
  s = db->Get(ReadOptions(), "111/JHN.3.16", &returned_value);
  if (s.ok()) {
    cout << "Retrieved: " << returned_value << endl;
  } else {
    cerr << "Error retrieving key!" << endl;
  }

  // 4. Close the database
  delete db;
  return 0;
}