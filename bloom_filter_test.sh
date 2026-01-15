#!/bin/bash

# chmod +x bloom_filter_test.sh
#
# ./bloom_filter_test.sh

# --- Configuration ---
# For 1,000 items with low collisions, we use 10,000 bits.
BIT_SIZE=10000
declare -a FILTER

# --- Function: Quick Hash ---
# Returns a number 0-9999 using the POSIX cksum algorithm
get_hash() {
    local input="$1$2"
    # Use cksum for speed; it returns 'hash size'
    local h=$(echo "$input" | cksum | cut -d' ' -f1)
    echo $(( h % BIT_SIZE ))
}

# --- Function: Add Item ---
add_item() {
    local h1=$(get_hash "$1" "A")
    local h2=$(get_hash "$1" "B")
    FILTER[$h1]=1
    FILTER[$h2]=1
}

# --- Function: Check Item ---
check_item() {
    local h1=$(get_hash "$1" "A")
    local h2=$(get_hash "$1" "B")
    if [[ ${FILTER[$h1]} -eq 1 && ${FILTER[$h2]} -eq 1 ]]; then
        return 0 # Probable match
    fi
    return 1 # Definitive NO
}

# --- Demonstration ---
echo "Ingesting 1,000 items into the filter..."
for i in {1..1000}; do
    add_item "King_$i"
done

echo "---------------------------------------"
# Test cases
for test_val in "King_500" "Shallum_The_Usurper"; do
    if check_item "$test_val"; then
        echo "[MATCH] $test_val is PROBABLY in the system."
    else
        echo "[MISS]  $test_val is DEFINITELY NOT there."
    fi
done