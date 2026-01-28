-- brew install ghc
--
-- runhaskell bloom_filter_test.hs

import Data.Array (Array, accum, (!), listArray)
import Data.Char (ord)

-- | Standard Bloom Filter Structure
data BloomFilter = BloomFilter {
    size :: Int,
    array :: Array Int Bool
}

-- | Custom Hash (djb2) to avoid external dependencies
hashWithSalt :: Int -> String -> Int
hashWithSalt salt str = foldl (\h c -> (h * 33) + ord c + salt) 5381 str

emptyFilter :: Int -> BloomFilter
emptyFilter m = BloomFilter m (listArray (0, m - 1) (replicate m False))

getHashes :: Int -> Int -> String -> [Int]
getHashes m k item = [ abs (hashWithSalt salt item) `mod` m | salt <- [1..k] ]

insert :: Int -> String -> BloomFilter -> BloomFilter
insert k item bf = 
    let indices = getHashes (size bf) k item
        newArray = accum (||) (array bf) [ (i, True) | i <- indices ]
    in bf { array = newArray }

contains :: Int -> String -> BloomFilter -> Bool
contains k item bf = 
    let indices = getHashes (size bf) k item
    in all (array bf !) indices

-- --- Main Execution ---
main :: IO ()
main = do
    let k = 3   -- 3 hash functions
    let m = 100 -- Large enough to avoid too many collisions
    
    -- Leviticus 11 "Clean" Items (Examples)
    let cleanItems = ["Cow", "Sheep", "Goat", "Locust", "Salmon"]
    
    -- Build the filter by folding the clean items list
    let leviticusFilter = foldl (\bf item -> insert k item bf) (emptyFilter m) cleanItems

    putStrLn "--- Leviticus 11: Probabilistic Classification Demo ---"
    
    let testItems = ["Cow", "Pig", "Salmon", "Camel", "Sheep", "Eagle"]

    mapM_ (\item -> do
        let result = if contains k item leviticusFilter
                     then "Probably Clean (In Filter)"
                     else "Definitely Unclean (Not in Filter)"
        putStrLn $ "Item: " ++ item ++ " -> " ++ result
        ) testItems

    putStrLn "\n[Theology meets Tech] If the filter says 'Unclean', it is 100% correct."
    putStrLn "If it says 'Clean', there is a small mathematical chance of a false positive."