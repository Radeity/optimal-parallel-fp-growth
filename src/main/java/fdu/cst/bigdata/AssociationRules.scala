package fdu.cst.bigdata

import fdu.cst.bigdata.PMAssociationRules.Rule
import fdu.cst.bigdata.FPGrowthCore.FreqItemset
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * @author 王维饶
 * @date 2022/6/10 1:38 PM
 * @version 1.0
 */

/**
 * Generates association rules from a `RDD[FreqItemset[Item]]`. This method only generates
 * association rules which have a single item as the consequent.
 *
 */
class PMAssociationRules(
                        var minConfidence: Double) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minConfidence = 0.8}.
   */
  def this() = this(0.8)

  /**
   * Sets the minimal confidence (default: `0.8`).
   */
  def setMinConfidence(minConfidence: Double): this.type = {
    require(minConfidence >= 0.0 && minConfidence <= 1.0,
      s"Minimal confidence must be in range [0, 1] but got ${minConfidence}")
    this.minConfidence = minConfidence
    this
  }

  /**
   * Computes the association rules with confidence above `minConfidence`.
   *
   * @param freqItemsets frequent itemset model obtained from [[FPGrowthCore]]
   * @return a `RDD[Rule[Item]]` containing the association rules.
   *
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    run(freqItemsets, Map.empty[Item, Double])
  }

  /**
   * Computes the association rules with confidence above `minConfidence`.
   *
   * @param freqItemsets frequent itemset model obtained from [[FPGrowthCore]]
   * @param itemSupport  map containing an item and its support
   * @return a `RDD[Rule[Item]]` containing the association rules. The rules will be able to
   *         compute also the lift metric.
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]],
                          itemSupport: scala.collection.Map[Item, Double]): RDD[Rule[Item]] = {
    // For candidate rule X => Y, generate (X, (Y, freq(X union Y)))
    val candidates = freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
          case _ => None
        }
      }
    }

    // Join to get (X, ((Y, freq(X union Y)), freq(X))), generate rules, and filter by confidence
    candidates.join(freqItemsets.map((x: FreqItemset[Item]) => (x.items.toSeq, x.freq)))
      .map { case (antecedent, ((consequent, freqUnion), freqAntecedent)) =>
        new Rule(antecedent.toArray,
          consequent.toArray,
          freqUnion,
          freqAntecedent,
          // the consequent contains always only one element
          itemSupport.get(consequent.head))
      }.filter(_.confidence >= minConfidence)
  }

}

object PMAssociationRules {

  /**
   * An association rule between sets of items.
   *
   * @param antecedent hypotheses of the rule. Java users should call [[Rule#javaAntecedent]]
   *                   instead.
   * @param consequent conclusion of the rule. Java users should call [[Rule#javaConsequent]]
   *                   instead.
   * @tparam Item item type
   *
   */
  class Rule[Item](
                    val antecedent: Array[Item],
                    val consequent: Array[Item],
                    val freqUnion: Double,
                    freqAntecedent: Double,
                    freqConsequent: Option[Double]) extends Serializable {

    /**
     * Returns the confidence of the rule.
     *
     */
    def confidence: Double = freqUnion / freqAntecedent

    /**
     * Returns the lift of the rule.
     */
    def lift: Option[Double] = freqConsequent.map(fCons => confidence / fCons)

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })

    override def toString: String = {
      s"${antecedent.mkString("{", ",", "}")} => " +
        s"${consequent.mkString("{", ",", "}")}: (confidence: $confidence; lift: $lift)"
    }
  }
}

