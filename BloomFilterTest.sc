// scala-cli run --server=false BloomFilterTest.sc

// BloomFilterTest.sc
import scala.util.hashing.MurmurHash3

case class RitualItem(name: String, isClean: Boolean)

class LeviticalValidator(size: Int) {
  private val cleanRegistry = new Array[Boolean](size)

  private def getHashes(itemName: String): Seq[Int] = {
    Seq(
      Math.abs(MurmurHash3.stringHash(itemName, 1) % size),
      Math.abs(MurmurHash3.stringHash(itemName, 2) % size)
    )
  }

  def register(name: String): Unit = 
    getHashes(name).foreach(cleanRegistry(_) = true)

  def isClean(name: String): Boolean = 
    getHashes(name).forall(cleanRegistry(_))
}

// Action Section - We add println here to see the results
val tabernacle = new LeviticalValidator(64)

println("--- Levitical Audit Starting ---")

tabernacle.register("Lamb")
println(s"Is Lamb clean? ${tabernacle.isClean("Lamb")}")

tabernacle.register("Ox")
println(s"Is Ox clean? ${tabernacle.isClean("Ox")}")

println(s"Is Swine clean? ${tabernacle.isClean("Swine")}")
println("--- Audit Complete ---")