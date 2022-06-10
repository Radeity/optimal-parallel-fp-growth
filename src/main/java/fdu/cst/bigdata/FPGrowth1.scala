package fdu.cst.bigdata

import fdu.cst.bigdata.FPGrowthCore.FreqItemset
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.util.UUID
import scala.reflect.ClassTag

/**
 * @author 王维饶
 * @date 2022/6/9 6:15 PM
 * @version 1.0
 */
class FPGrowthParams {

  var itemsCol: String = "items"

  var minSupport: Double = 0.0

  var numPartitions: Int = 1

  var minConfidence: Double = 0.0

  var predictionCol: String = "prediction"
}


class FPGrowth1(val uid: String) extends FPGrowthParams {

  def this() = this("fpgrowth_" + UUID.randomUUID().toString.takeRight(12))

  def setMinSupport(value: Double): this.type = {
    minSupport = value
    this
  }

  def setMinConfidence(value: Double): this.type = {
    minConfidence = value
    this
  }

  def setItemsCol(value: String): this.type = {
    itemsCol = value
    this
  }

  def setNumPartitions(value: Int): this.type = {
    numPartitions = value
    this
  }

  def fit(dataset: Dataset[_]): FPGrowthModel1 = {
    genericFit(dataset)
  }


  def genericFit[T: ClassTag](dataset: Dataset[_]): FPGrowthModel1 = {
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    val data = dataset.select(itemsCol)
    val items = data.where(col(itemsCol).isNotNull).rdd.map(r => r.getSeq[Any](0).toArray)
    val FP = new FPGrowthCore().setMinSupport(minSupport).setNumPartitions(numPartitions)
    if (handlePersistence) {
      items.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val inputRowCount = items.count()
    val parentModel = FP.run(items)
    val rows = parentModel.freqItemsets.map(f => Row(f.items, f.freq))
    val schema = StructType(Seq(
      StructField("items", dataset.schema(itemsCol).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val frequentItems = dataset.sparkSession.createDataFrame(rows, schema)

    if (handlePersistence) {
      items.unpersist()
    }
    new FPGrowthModel1(uid, frequentItems, parentModel.itemSupport, inputRowCount).setMinConfidence(minConfidence)
  }
}

class FPGrowthModel1(
                      val uid: String,
                      @transient val freqItemsets: DataFrame,
                      private val itemSupport: scala.collection.Map[Any, Double],
                      private val numTrainingRecords: Long) extends FPGrowthParams {

  /**
   * Cache minConfidence and associationRules to avoid redundant computation for association rules
   * during transform. The associationRules will only be re-computed when minConfidence changed.
   */
  @transient private var _cachedMinConf: Double = Double.NaN

  @transient private var _cachedRules: DataFrame = _

  @transient def associationRules: DataFrame = {
    if (minConfidence == _cachedMinConf) {
      _cachedRules
    } else {
      _cachedRules = AssociationRules
        .getAssociationRulesFromFP(freqItemsets, "items", "freq", minConfidence, itemSupport,
          numTrainingRecords)
      _cachedMinConf = minConfidence
      _cachedRules
    }
  }

  def setPredictionCol(value: String): this.type = {
    predictionCol = value
    this
  }

  def setMinConfidence(value: Double): this.type = {
    minConfidence = value
    this
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    genericTransform(dataset)
  }

  def genericTransform(dataset: Dataset[_]): DataFrame = {
    val rules: Array[(Seq[Any], Seq[Any], Double)] = associationRules.select("antecedent", "consequent", "lift")
      .rdd.map(r => (r.getSeq(0), r.getSeq(1), r.getDouble(2)))
      .collect().asInstanceOf[Array[(Seq[Any], Seq[Any], Double)]]
    val brRules = dataset.sparkSession.sparkContext.broadcast(rules)
    val dt = dataset.schema(itemsCol).dataType
    // For each rule, examine the input items and summarize the consequents
//    def predictUDF(items: Seq[Any]) = {
//      if (items != null) {
//        val itemset = items.toSet
//        brRules.value.filter(_._1.forall(itemset.contains))
//          .flatMap(_._2.filter(!itemset.contains(_))) //.distinct.sortBy(_._3)(Ordering.Double.reverse)
//      } else {
//        Seq.empty
//      }
//    }

    val udf_predict = udf((items: Seq[Any]) => {
      if (items != null) {
        val itemset = items.toSet
        brRules.value.filter(_._1.forall(itemset.contains))
          .flatMap(_._2.filter(!itemset.contains(_))).toString //.distinct.sortBy(_._3)(Ordering.Double.reverse)
      } else {
        ""
      }
    })
    dataset.withColumn(predictionCol, udf_predict(col(itemsCol)))
  }
}

object AssociationRules {

  /**
   * Computes the association rules with confidence above minConfidence.
   *
   * @param dataset            DataFrame("items"[Array], "freq"[Long]) containing frequent itemsets obtained
   *                           from algorithms like [[FPGrowthCore]].
   * @param itemsCol           column name for frequent itemsets
   * @param freqCol            column name for appearance count of the frequent itemsets
   * @param minConfidence      minimum confidence for generating the association rules
   * @param itemSupport        map containing an item and its support
   * @param numTrainingRecords count of training Dataset
   * @return a DataFrame("antecedent"[Array], "consequent"[Array], "confidence"[Double],
   *         "lift" [Double]) containing the association rules.
   */
  def getAssociationRulesFromFP[T: ClassTag](
                                              dataset: Dataset[_],
                                              itemsCol: String,
                                              freqCol: String,
                                              minConfidence: Double,
                                              itemSupport: scala.collection.Map[T, Double],
                                              numTrainingRecords: Long): DataFrame = {
    val freqItemSetRdd = dataset.select(itemsCol, freqCol).rdd
      .map(row => new FreqItemset(row.getSeq[T](0).toArray, row.getLong(1)))
    val rows = new PMAssociationRules()
      .setMinConfidence(minConfidence)
      .run(freqItemSetRdd, itemSupport)
      .map(r => Row(r.antecedent, r.consequent, r.confidence, r.lift.orNull,
        r.freqUnion / numTrainingRecords))

    val dt = dataset.schema(itemsCol).dataType
    val schema = StructType(Seq(
      StructField("antecedent", dt, nullable = false),
      StructField("consequent", dt, nullable = false),
      StructField("confidence", DoubleType, nullable = false),
      StructField("lift", DoubleType),
      StructField("support", DoubleType, nullable = false)))
    val rules = dataset.sparkSession.createDataFrame(rows, schema)
    rules
  }
}
