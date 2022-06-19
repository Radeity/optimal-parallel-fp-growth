package fdu.cst.bigdata.pfp

import fdu.cst.bigdata.pfp.FPGrowthCore.FreqItemset
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag


/**
 * @author 王维饶
 * @date 2022/6/10 11:16 AM
 * @version 1.0
 */
class FPGrowthModel1[Item: ClassTag](val freqItemsets: RDD[FreqItemset[Item]],
                                    val itemSupport: Map[Item, Double]) extends Serializable {
  def this(freqItemsets: RDD[FreqItemset[Item]]) = this(freqItemsets, Map.empty)
}

class FPGrowthCore(private var minSupport: Double,
                   private var numPartitions: Int) extends Serializable {

  def this() = this(0.3, -1)

  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got $minSupport")
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got $numPartitions")
    this.numPartitions = numPartitions
    this
  }

  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   *
   */
  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel1[Item] = {
//    if (data.getStorageLevel == StorageLevel.NONE) {
//      logWarning("Input data is not cached.")
//    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItemsCount = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItemsCount.map(_._1), partitioner)
    val itemSupport = freqItemsCount.map {
      case (item, cnt) => item -> cnt.toDouble / count
    }.toMap
    new FPGrowthModel1(freqItemsets, itemSupport)
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount minimum count for frequent itemsets
   * @param partitioner partitioner used to distribute items
   * @return array of frequent patterns and their frequencies ordered by their frequencies
   */
  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            minCount: Long,
                                            partitioner: Partitioner): Array[(Item, Long)] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param freqItems frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets[Item: ClassTag](
                                               data: RDD[Array[Item]],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner): RDD[FreqItemset[Item]] = {
    val itemToRank: Map[Item, Int] = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
   * Generates conditional transactions.
   * @param transaction a transaction
   * @param itemToRank map from item to their rank
   * @param partitioner partitioner used to distribute transactions
   * @return a map of (target partition, conditional transaction)
   */
  private def genCondTransactions[Item: ClassTag](
                                                   transaction: Array[Item],
                                                   itemToRank: Map[Item, Int],
                                                   partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

object FPGrowthCore {

  /**
   * Frequent itemset.
   *
   * @param items items in this itemset.
   * @param freq  frequency
   * @tparam Item item type
   *
   */
  class FreqItemset[Item](val items: Array[Item],
                          val freq: Long) extends Serializable {

    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}