package fdu.cst.bigdata.pfp

import org.apache.spark.sql.SparkSession

/**
 * @author 王维饶
 * @date 2022/6/8 2:21 PM
 * @version 1.0
 */
object PFP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PFP")
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
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(support).setMinConfidence(confidence).setNumPartitions(2)
    val model = fpgrowth.fit(trainset)
    val resultDF = model.transform(testset)

    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))

    resultDF.rdd.map(r => (r.getSeq[String](0).toList, List(r(1).toString))).map(r => (r._1.head, r._2.head)).collect().foreach(printRule)
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
