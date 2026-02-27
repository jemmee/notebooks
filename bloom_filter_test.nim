# brew install nim
#
# nim c -r bloom_filter_test.nim

import std/[hashes, math, strutils]

type
  CensusFilter* = object
    bits: seq[uint64]
    m: int 
    k: int 

# ... [Internal logic: nextPowerOfTwo and newBloomFilter remain the same as before] ...

proc nextPowerOfTwo(n: int): int =
  if n <= 0: return 1
  result = 1
  while result < n: result = result shl 1

proc newCensusFilter*(expectedItems: int, p: float): CensusFilter =
  let m = ceil(-(expectedItems.float * ln(p)) / pow(ln(2.0), 2.0)).int
  let k = round((m.float / expectedItems.float) * ln(2.0)).int
  let bucketCount = nextPowerOfTwo((m + 63) div 64)
  result = CensusFilter(bits: newSeq[uint64](bucketCount), m: bucketCount * 64, k: k)

proc getIndices(bf: CensusFilter, item: string): seq[int] =
  # Cast signed hashes to unsigned to allow wrapping arithmetic
  let h1 = hash(item).uint
  let h2 = (hash(item & "sinai")).uint
  
  for i in 0 ..< bf.k:
    # Use uint for the calculation so it wraps instead of overflowing
    # Then convert back to int for the final index
    let combinedHash = h1 + i.uint * h2
    result.add((combinedHash mod bf.m.uint).int)
    
proc register*(bf: var CensusFilter, name: string) =
  for idx in bf.getIndices(name):
    bf.bits[idx div 64] = bf.bits[idx div 64] or (1.uint64 shl (idx mod 64))

proc isAccountedFor*(bf: CensusFilter, name: string): bool =
  result = true
  for idx in bf.getIndices(name):
    if (bf.bits[idx div 64] and (1.uint64 shl (idx mod 64))) == 0:
      return false 

# --- The Census of Israel (Numbers 1-2) ---
var sinaiCensus = newCensusFilter(603550, 0.05) # Large capacity for the 603,550 men

let tribes = ["Judah", "Reuben", "Gad", "Asher", "Naphtali", "Ephraim"]
for tribe in tribes:
  sinaiCensus.register(tribe)

# Testing our Filter
echo "--- Testing the Census Filter ---"
echo "Is Judah accounted for?     ", sinaiCensus.isAccountedFor("Judah")    # True
echo "Is Ephraim accounted for?   ", sinaiCensus.isAccountedFor("Ephraim")  # True

# These tribes are not in the 'tribes' list above
echo "Is Amalek accounted for?    ", sinaiCensus.isAccountedFor("Amalek")   # False
echo "Is Philistia accounted for? ", sinaiCensus.isAccountedFor("Philistia")# False