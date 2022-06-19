package fdu.cst.bigdata.epfp

import org.apache.spark.sql.SparkSession

/**
 * @author 王维饶
 * @date 2022/6/8 2:21 PM
 * @version 1.0
 */
object EPFP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EPFP")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    val support = args(1).toDouble //0.01
    val confidence = args(2).toDouble //0.01

    val train_path = "datas/sougou_sample.txt"
    val test_path = "datas/sougou_test.txt"

    val trainset = spark.read.textFile(train_path).map(t => t.split(" ").distinct).toDF("items")
    val testset = spark.read.textFile(test_path).map(t => t.split(" ").distinct).toDF("items")

    println("Running with support: " + support + ", and confidence: " + confidence)

    val start = System.currentTimeMillis
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(support).setMinConfidence(confidence).setNumPartitions(args(0).toInt)
    val model = fpgrowth.fit(trainset)
    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
    val resultDF = model.transform(testset)

    resultDF.rdd.map(r => (r.getSeq[String](0).mkString(", "), r(1).toString)).collect().foreach(printRule)
  }

  def printRule(rule: (String, String)): Unit = {
    print("[" + rule._1 + "] ")
    val candidates = rule._2.split(",")
    for (i <- candidates.indices) {
      print("["+ rule._1 + ", " + candidates(i).trim + "]")
      if (i != candidates.size - 1) {
        print(", ")
      }
    }
    println()
  }
}
