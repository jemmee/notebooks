-- brew install lua
--
-- lua -v
--
-- lua bloom_filter_test.lua

local BloomFilter = {}
BloomFilter.__index = BloomFilter

-- Pure Lua FNV-1a 32-bit hash implementation
local function fnv1a_32(data, seed)
    local prime = 16777619
    local hash = seed or 2166136261
    
    for i = 1, #data do
        local byte = string.byte(data, i)
        -- XOR the lowest 8 bits of the hash with the current byte
        hash = (hash ~ byte) & 0xFFFFFFFF
        -- Multiply by FNV prime and maintain 32-bit integer limits
        hash = (hash * prime) & 0xFFFFFFFF
    end
    return hash
end

-- Constructor
-- @param size: The total number of bits in the bit array (m)
-- @param num_hashes: The number of hash functions to use (k)
function BloomFilter.new(size, num_hashes)
    local self = setmetatable({}, BloomFilter)
    self.size = size
    self.num_hashes = num_hashes
    
    -- Initialize the bit array. 
    -- In Lua, tables act as sparse arrays, so initializing to false is highly memory efficient.
    self.bit_array = {}
    for i = 0, size - 1 do
        self.bit_array[i] = false
    end
    
    return self
end

-- Add an item to the Bloom Filter
function BloomFilter:add(item)
    item = tostring(item)
    for i = 1, self.num_hashes do
        -- Use the loop index as a unique seed to simulate independent hash functions
        local hash = fnv1a_32(item, i * 1103515245)
        local index = hash % self.size
        self.bit_array[index] = true
    end
end

-- Check if an item is likely in the Bloom Filter
-- @return boolean: false means "Definitely Not", true means "Probably"
function BloomFilter:check(item)
    item = tostring(item)
    for i = 1, self.num_hashes do
        local hash = fnv1a_32(item, i * 1103515245)
        local index = hash % self.size
        
        -- If any single bit is false, the item has definitively never been added
        if not self.bit_array[index] then
            return false
        end
    end
    return true
end

--- ========================================================================
--- DEMO EXECUTION
--- ========================================================================

print("--- Initializing Bloom Filter (Size: 1000 bits, Hashes: 5) ---")
-- Allocating a small bit-array room for demo visibility
local bf = BloomFilter.new(1000, 5)

-- 1. Populate the filter with a known allowed-list (e.g., trusted IP addresses or safe URLs)
local trusted_ips = {
    "192.168.1.1",
    "10.0.0.50",
    "172.16.254.1",
    "8.8.8.8"
}

print("\n[+] Adding trusted IPs to the filter...")
for _, ip in ipairs(trusted_ips) do
    bf:add(ip)
    print("    Added: " .. ip)
end

-- 2. Test Membership Verification
print("\n--- Testing Membership (Definitive Negatives & Probable Positives) ---")

local test_cases = {
    "192.168.1.1",   -- Should be True (Was added)
    "8.8.8.8",       -- Should be True (Was added)
    "192.168.1.99",  -- Should be False (Never added)
    "10.0.0.1",      -- Should be False (Never added)
    "200.100.50.25"  -- Should be False (Never added)
}

for _, ip in ipairs(test_cases) do
    local result = bf:check(ip)
    if result then
        print(string.format("--> Is '%s' trusted? PROBABLY YES", ip))
    else
        print(string.format("--> Is '%s' trusted? DEFINITELY NO", ip))
    end
end