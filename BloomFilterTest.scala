// brew install Virtuslab/scala-cli/scala-cli
//
// scala-cli run --server=false BloomFilterTest.scala

import scala.util.hashing.MurmurHash3

// 1. Keep your class and logic at the top
case class RitualItem(name: String, isClean: Boolean, weightInShekels: Double)

class LeviticalValidator(size: Int) {
  private val cleanRegistry = new Array[Boolean](size)

  private def getHashes(itemName: String): Seq[Int] = {
    Seq(
      Math.abs(MurmurHash3.stringHash(itemName, 11) % size),
      Math.abs(MurmurHash3.stringHash(itemName, 22) % size)
    )
  }

  def registerCleanItem(name: String): Unit = {
    getHashes(name).foreach(idx => cleanRegistry(idx) = true)
    println(s"📜 Statute Updated: '$name' is marked CLEAN.")
  }

  def validate(item: RitualItem): Boolean = {
    val maybeClean = getHashes(item.name).forall(idx => cleanRegistry(idx))
    if (!maybeClean) {
      println(s"⚠️ REJECTED: ${item.name} is NOT in the clean registry.")
      false
    } else {
      println(s"✅ VALIDATED: ${item.name} is acceptable for the altar.")
      true
    }
  }
}

// 2. Wrap your execution code in a @main function
@main def runLeviticus(): Unit = {
  val validator = new LeviticalValidator(128)

  // Now these calls are legal because they are inside a function!
  validator.registerCleanItem("Ox")
  validator.registerCleanItem("Sheep")
  validator.registerCleanItem("Goat")

  val offering1 = RitualItem("Ox", isClean = true, 50.5)
  val offering2 = RitualItem("Camel", isClean = false, 120.0)

  validator.validate(offering1)
  validator.validate(offering2)
}