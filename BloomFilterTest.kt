// kotlinc BloomFilterTest.kt -include-runtime -d BloomFilterTest.jar
//
// java -jar BloomFilterTest.jar

import java.util.BitSet
import kotlin.math.abs

class BloomFilterTest(private val m: Int, private val k: Int) {
    private val bits = BitSet(m)

    /**
     * Generates k different hash indices for a given item.
     */
    private fun getIndices(item: String): List<Int> {
        return (0 until k).map { seed ->
            // Use a simple salted hash
            abs("${item}${seed}".hashCode()) % m
        }
    }

    /**
     * Adds an item to the Bloom Filter.
     */
    fun add(item: String) {
        getIndices(item).forEach { index ->
            bits.set(index)
        }
    }

    /**
     * Returns true if the item might be present, false if definitely absent.
     */
    fun contains(item: String): Boolean {
        return getIndices(item).all { index ->
            bits.get(index)
        }
    }
}

fun main() {
    val bloomFilterTest = BloomFilterTest(m = 100, k = 3)

    // New Testament Data
    val additions = listOf("Seraphim", "Live-Coal", "Plowshare", "Pruning-Hook", "Wolf-and-Lamb", "Standard-to-Gentiles")

    println("--- Seeding Kotlin Bloom Filter ---")
    additions.forEach { item ->
        bloomFilterTest.add(item)
        println("Added: $item")
    }

    println("\n--- Testing Membership ---")
    val tests = listOf("Seraphim", "Plowshare", "Spear", "Great-Whale", "Live-Coal")

    tests.forEach { test ->
        val result = if (bloomFilterTest.contains(test)) "PROBABLY PRESENT" else "DEFINITELY ABSENT"
        println("${test.padEnd(18)} : $result")
    }
}