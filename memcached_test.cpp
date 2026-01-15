// sudo dnf install memcached libmemcached libmemcached-devel -y
//
// sudo systemctl enable --now memcached
//
// g++ -O3 memcached_test.cpp -o memcached_test -lmemcached
//
// ./memcached_test

#include <iostream>
#include <libmemcached/memcached.h>
#include <map>
#include <string>
#include <vector>

int main() {
  memcached_st *memc = memcached_create(NULL);
  memcached_server_st *servers =
      memcached_server_list_append(NULL, "127.0.0.1", 11211, NULL);
  memcached_server_push(memc, servers);
  memcached_server_list_free(servers);

  // 1. Data Set: 20+ Kings and Queens
  std::map<std::string, std::string> monarchy = {
      {"Rehoboam", "Son of Solomon; first king of Judah after the split."},
      {"Jeroboam", "First king of the northern kingdom of Israel."},
      {"Asa", "A king of Judah who removed idols from the land."},
      {"Ahab",
       "King of Israel known for his conflict with the prophet Elijah."},
      {"Jezebel", "Queen of Israel; wife of Ahab who promoted Baal worship."},
      {"Jehoshaphat",
       "King of Judah known for his military and judicial reforms."},
      {"Jehu", "Anointed to destroy the house of Ahab; king of Israel."},
      {"Athaliah", "The only reigning Queen of Judah; daughter of Ahab."},
      {"Joash", "Hidden in the Temple as a child to escape Athaliah."},
      {"Uzziah", "A powerful king of Judah who became proud and was struck "
                 "with leprosy."},
      {"Ahaz", "King of Judah who sought help from Assyria against Israel."},
      {"Hoshea", "The final king of the northern kingdom of Israel."},
      {"Manasseh", "The longest-reigning king of Judah; known for great "
                   "wickedness then repentance."},
      {"Amon", "Son of Manasseh; reigned briefly and continued in idolatry."},
      {"Zedekiah", "The last king of Judah before the Babylonian captivity."},
      {"Omri", "Powerful king of Israel who built the city of Samaria."},
      {"Joram", "King of Judah; married the daughter of Ahab."},
      {"Azariah", "Also known as Uzziah; reigned 52 years in Jerusalem."},
      {"Pekah", "King of Israel who conspired against Judah."},
      {"Shallum",
       "Reigned only one month in Samaria before being assassinated."},
      {"Queen_of_Sheba",
       "Visited Solomon to test his wisdom with hard questions."}};

  // 2. Batch Store (SET)
  std::cout << "--- Caching 20+ Royal Figures ---" << std::endl;
  for (auto const &[name, bio] : monarchy) {
    memcached_set(memc, name.c_str(), name.length(), bio.c_str(), bio.length(),
                  600, 0);
  }

  // 3. Multi-Get (Fetching multiple keys in one network round-trip)
  std::vector<const char *> keys = {"David", "Solomon", "Jezebel", "Athaliah",
                                    "Zedekiah"};
  std::vector<size_t> key_lens = {5, 7, 7, 8, 8};

  std::cout << "\n--- Performing Multi-Get for specific Royals ---"
            << std::endl;
  memcached_return_t rc =
      memcached_mget(memc, keys.data(), key_lens.data(), keys.size());

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t return_value_length;
  uint32_t flags;

  while ((return_value = memcached_fetch(memc, return_key, &return_key_length,
                                         &return_value_length, &flags, &rc)) !=
         NULL) {
    std::cout << "Found [" << std::string(return_key, return_key_length)
              << "]: " << std::string(return_value, return_value_length)
              << std::endl;
    free(return_value);
  }

  memcached_free(memc);
  return 0;
}