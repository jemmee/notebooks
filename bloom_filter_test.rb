# ruby bloom_filter_test.rb

require 'zlib'

class BloomFilterTest
  def initialize(size, hash_count)
    @size = size
    @hash_count = hash_count
    # Create a string of zeros to act as our bit-array
    @bits = 0
  end

  # Create k different hashes by salting the item
  def get_hashes(item)
    (0...@hash_count).map do |seed|
      # Salt the item with the seed and use CRC32 for the hash
      Zlib.crc32("#{seed}#{item}") % @size
    end
  end

  def add(item)
    get_hashes(item).each do |idx|
      # Set the bit at idx to 1 using bitwise OR
      @bits |= (1 << idx)
    end
  end

  def exists?(item)
    get_hashes(item).all? do |idx|
      # Check if the bit at idx is 1 using bitwise AND
      (@bits & (1 << idx)) != 0
    end
  end
end

# --- Execution Script ---

bf = BloomFilterTest.new(100, 3)

# New Testament additions
additions = ["Fisher-Net", "Alabaster-Jar", "Thorns-Crown", "Seamless-Robe"]

puts "--- Seeding Ruby Bloom Filter ---"
additions.each do |item|
  bf.add(item)
  puts "Added: #{item}"
end

puts "\n--- Testing Membership ---"
tests = ["Fisher-Net", "Thorns-Crown", "Golden-Calf", "Centurion-Spear", "Fisher-Boat"]

tests.each do |test|
  status = bf.exists?(test) ? "PROBABLY PRESENT" : "DEFINITELY ABSENT"
  puts "#{test.ljust(18)} : #{status}"
end