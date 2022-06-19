package fdu.cst.bigdata.fp

import fdu.cst.bigdata.pfp.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim}

/**
 * @author 王维饶
 * @date 2022/6/8 2:21 PM
 * @version 1.0
 */
object FP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FP")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val support = 0.1
    val confidence = 0.1
    val train_path = "datas/train.txt" //"datas/"+transactionsFile
    val test_path = "datas/test.txt" //"datas/"+testFile

    val trainset = spark.read.textFile(train_path).map(t => t.split(" ").distinct).toDF("items")
    val testset = spark.read.textFile(test_path).map(t => t.split(" ").distinct).toDF("items")

    println("Running with support: " + support + ", and confidence: " + confidence)

    val start = System.currentTimeMillis
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(support).setMinConfidence(confidence).setNumPartitions(1)
    val model = fpgrowth.fit(trainset)
    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
    val resultDF = model.transform(testset)

    //    resultDF.show()
    resultDF.rdd.map(r => (r.getSeq[String](0).mkString(", "), r(1).toString)).collect().foreach(printRule)
  }

  def printRule(rule: (String, String)): Unit = {
    print("[" + rule._1 + "] ")
    val candidates = rule._2.split(",")
    if (candidates.size != 1) {
      for (i <- candidates.indices) {
        print("["+ rule._1 + ", " + candidates(i).trim + "]")
        if (i != candidates.size - 1) {
          print(", ")
        } else {
          println()
        }
      }
    }
  }
}
