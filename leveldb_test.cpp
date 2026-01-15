// sudo dnf install leveldb leveldb-devel -y
//
// g++ -O3 leveldb_test.cpp -o leveldb_test -lleveldb -lpthread
//
// ./leveldb_test

#include "leveldb/db.h"
#include <iostream>
#include <map>
#include <string>

int main() {
  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing = true;

  // 1. Open the database
  leveldb::Status s = leveldb::DB::Open(options, "./leveldb_test.db", &db);
  if (!s.ok()) {
    std::cerr << "Open failed: " << s.ToString() << std::endl;
    return 1;
  }

  // 2. Prepare the data
  std::map<std::string, std::string> biblical_figures = {
      {"Othniel",
       "The first judge; he delivered Israel from the king of Mesopotamia."},
      {"Deborah",
       "A prophetess who, with Barak, defeated the Canaanite general Sisera."},
      {"Gideon", "Destroyed the altar of Baal and led 300 men to victory over "
                 "the Midianites."},
      {"Jephthah", "The son of a harlot who led Israel against the Ammonites "
                   "after a rash vow."},
      {"Samson", "Known for his immense strength and his struggle against the "
                 "Philistines."},
      {"Sarah", "The wife of Abraham and mother of Isaac in her old age."},
      {"Ruth",
       "A Moabite woman who showed great loyalty to her mother-in-law Naomi."},
      {"Esther",
       "A Jewish queen of Persia who saved her people from a massacre."},
      {"Hannah",
       "The mother of the prophet Samuel, known for her persistent prayer."},
      {"Rahab", "A woman of Jericho who assisted the Israelite spies."}};

  // 3. Insert data into LevelDB
  for (auto const &[name, bio] : biblical_figures) {
    s = db->Put(leveldb::WriteOptions(), name, bio);
    if (!s.ok())
      std::cerr << "Error writing " << name << std::endl;
  }

  // 4. Look up a specific figure
  std::string value;
  std::string search_key = "Deborah";
  s = db->Get(leveldb::ReadOptions(), search_key, &value);

  if (s.ok()) {
    std::cout << "--- Single Lookup ---" << std::endl;
    std::cout << search_key << ": " << value << "\n" << std::endl;
  }

  // 5. Iterate through all figures (Sorted Order)
  std::cout << "--- All figures (Sorted by Key) ---" << std::endl;
  leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << "Figure: " << it->key().ToString() << "\n";
    std::cout << "Bio:   " << it->value().ToString() << "\n" << std::endl;
  }

  // 6. Cleanup
  delete it;
  delete db;
  return 0;
}