# bloom_filter_test.jl
#
# curl -fsSL https://install.julialang.org | sh
#
# julia bloom_filter_test.jl

using SHA  # Standard library for cryptographic hashing

struct BloomFilter
    bit_array::BitVector
    num_hashes::Int
    size::Int
end

# Constructor to initialize our filter with zeros
function BloomFilter(size::Int, num_hashes::Int)
    return BloomFilter(falses(size), num_hashes, size)
end

"""
    get_hashes(item::String, k::Int, max_val::Int)

Generates `k` distinct bit-array indices for a given string using 
salting and SHA-256 to simulate multiple independent hash functions.
"""
function get_hashes(item::String, k::Int, max_val::Int)
    indices = Int[]
    for i in 1:k
        # Salt the string with the current index loop to get diverse hashes
        salted_bytes = sha256(item * string(i))
        
        # Take the first 8 bytes of the hash and convert to an integer
        raw_int = reinterpret(UInt64, salted_bytes[1:8])[1]
        
        # Map it to our bit array boundaries (1-indexed for Julia arrays)
        push!(indices, (raw_int % max_val) + 1)
    end
    return indices
end

"""
    insert!(bf::BloomFilter, item::String)

Flips the calculated hash bits to true.
"""
function insert!(bf::BloomFilter, item::String)
    for idx in get_hashes(item, bf.num_hashes, bf.size)
        bf.bit_array[idx] = true
    end
end

"""
    check(bf::BloomFilter, item::String)::Bool

Returns true if the item MIGHT be in the set, false if it DEFINITELY is not.
"""
function check(bf::BloomFilter, item::String)::Bool
    for idx in get_hashes(item, bf.num_hashes, bf.size)
        if !bf.bit_array[idx]
            return false # Found a 0, so it absolutely never entered the set
        end
    end
    return true # All bits were 1, might be a false positive!
end

# Run the test simulation
function main()
    # Create a small 1000-bit array utilizing 4 hash passes
    bf = BloomFilter(1000, 4)
    
    # 1. Populate the filter with Old Testament names
    println("--- Inserting Old Testament Records into Bloom Filter ---")
    old_testament_records = ["abraham_patriarch", "moses_prophet", "david_king", "esther_queen"]
    for person in old_testament_records
        insert!(bf, person)
        println("Registered: ", person)
    end
    
    println("\n--- Querying the Registry ---")
    
    # Test an item that definitely exists in the filter
    test_present = "moses_prophet"
    println("Checking '$(test_present)' (Should be true): ", check(bf, test_present))
    
    # Test an item that belongs to a completely different era/text (New Testament)
    test_absent = "paul_apostle"
    println("Checking '$(test_absent)' (Should be false): ", check(bf, test_absent))
    
    # Let's inspect the underlying BitVector optimization
    active_bits = sum(bf.bit_array)
    println("\nFilter Density: ", active_bits, " out of ", bf.size, " bits flipped to true.")
end

main()