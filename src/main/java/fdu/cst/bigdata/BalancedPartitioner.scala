package fdu.cst.bigdata

import org.apache.spark.Partitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log

/**
 * @author 王维饶
 * @date 2022/6/10 9:58 PM
 * @version 1.0
 */
class BalancedPartitioner() extends Partitioner {

  var partitions: Int = _

  var weightMap: mutable.Map[Int, Int] = mutable.Map[Int, Int]()

  def this(partitions: Int, dataSize: Int) {
    this()
    this.partitions = partitions
    val weightIter: Iterator[Double] = Array.range(1, dataSize + 1).map(log(_)).iterator
    //    val dataSize: Int = rankMap.size
    //    val partitionSize: Int = dataSize / partitions
    var partitionWeights = new ArrayBuffer[(Double, Int)](partitions)
    for (i <- 0 to partitions - 1) {
      partitionWeights.append((0, i))
    }
    var cnt = 0
    while(weightIter.hasNext) {
      val weight: Double = weightIter.next()
      val idx: Int = cnt % partitions
      if (idx == 0) {
        partitionWeights = partitionWeights.sortBy(_._1)(Ordering.Double.reverse)
      }
      weightMap.put(cnt, partitionWeights(idx)._2)
      partitionWeights(idx) = (partitionWeights(idx)._1 + weight, partitionWeights(idx)._2)
      cnt += 1
    }
  }

  def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = weightMap(key.toString.toInt)
}