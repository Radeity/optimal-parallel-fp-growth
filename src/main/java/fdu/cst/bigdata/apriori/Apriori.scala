package fdu.cst.bigdata.apriori

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

object Apriori extends App {
  type ItemSet = Set[String]

  case class Item(set: ItemSet, support: Int)

  type Transaction = List[ItemSet]
  type FrequentItemSets = List[Item]
  type AssociationRule = (ItemSet, ItemSet, Double)

  val slides = "datas/train.txt"
  val support = 0.1
  val confidence = 0.1

  run(slides, support, confidence)

  def run(file: String, support: Double, confidence: Double): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Apriori")
    val sc = new SparkContext(sparkConf)
    getTransactions(file) match {
      case Success(set) => {
        println("Running with support: " + support + ", and confidence: " + confidence)
        val start = System.currentTimeMillis
        val frequencyMap = getFrequencyMap(set)
        val frequentItems = getFrequentItemSets(frequencyMap, set, support)
        val twoSizeRules = generateAssociationRules(sc, frequencyMap, frequentItems, confidence)
        val totalTime = System.currentTimeMillis - start
        println("Elapsed time: %1d ms".format(totalTime))
        twoSizeRules.foreach(printRule)
      }
      case Failure(e) => println(e.getMessage)
    }
    sc.stop()
  }

  def printRule(rule: (String, List[(String, String, Double)])): Unit = {
    print("[" + rule._1 + "] ")
    for (i <- rule._2.indices) {
      print("[" + rule._2(i)._1 + ", " + rule._2(i)._2 + "]")
      if (i != rule._2.size - 1) {
        print(", ")
      } else {
        println()
      }
    }
  }

  def getTransactions(file: String): Try[Transaction] = Try {
    Source.fromFile(file)
      .getLines()
      .map(_.split(" ").toSet)
      .toList
  }

  def getFrequencyMap(transaction: Transaction) = {
    transaction.flatten.foldLeft(Map[String, Int]() withDefaultValue 0) { (m, x) => m + (x -> (1 + m(x))) }
  }

  def getFrequentItemSets(frequencyMap: Map[String, Int], transaction: Transaction, minsup: Double): List[Item] = {
    // Map singletons with frequency

    val transactionsNeeded = (transaction.size * minsup).toInt

    // Filter Singletons
    val currentSet = frequencyMap.filter(item => item._2 >= transactionsNeeded).toList
    val items = currentSet.map(tuple => Item(Set(tuple._1), tuple._2))
    // List(Item(Set(5), support), Item(Set(..), support)

    // Current tuple size value
    var k = 2
    val result = ListBuffer(items)

    // Make this more functional?
    breakable {
      while (true) {
        val nextItemSetK = items.flatMap(_.set)
          .combinations(k)
          .map(_.toSet)
          .toList

        val supportedK = nextItemSetK.map(set => Item(set, getSupport(set, transaction)))
          .filter(_.support >= transactionsNeeded)

        // Nothing more to process..
        if (supportedK.isEmpty)
          break

        // Add new ItemSets that are supported, and increase k-tuple size
        result += supportedK
        k = k + 1
      }
    }

    result.flatten
      .toList
  }

  def generateAssociationRules(sc: SparkContext, frequencyMap: Map[String, Int], items: FrequentItemSets, conf: Double) = {
    // Just to have an easier way to access each sets support..
    val map: Map[ItemSet, Int] = items.map(item => item.set -> item.support)
      .toMap

    val rules: Seq[(Set[String], Set[String], Double)] = items.flatMap { item =>
      val set = item.set
      set.subsets().filter(x => (x.nonEmpty && x.size != set.size))
        .map { subset =>
          (subset, set diff subset, map(set).toDouble / map(subset).toDouble)
        }.toList
    }

    // Return only those confidence higher than "conf"
    //    rules.filter(rule => rule._3 >= conf)
    val filterRules: Seq[(String, List[(String, String, Double)])] = rules.filter(rule => rule._1.size == 1 && rule._2.size == 1 && rule._3 >= conf)
      .map(rule => (rule._1.head, List((rule._1.head, rule._2.head, (rule._3 / frequencyMap.getOrElse(rule._2.head, 1).toDouble)))))
    val filterRulesRDD: RDD[(String, List[(String, String, Double)])] = sc.parallelize(filterRules)
    val reduceRDD: RDD[(String, List[(String, String, Double)])] = filterRulesRDD.reduceByKey(_ ::: _)
    val sortRDD: RDD[(String, List[(String, String, Double)])] = reduceRDD.map(rule => (rule._1, rule._2.sortBy(_._3)(Ordering.Double.reverse)))
    sortRDD.collect()
  }

  def getSupport(set: ItemSet, transaction: Transaction): Int =
    transaction.count(line => set.subsetOf(line))
}
