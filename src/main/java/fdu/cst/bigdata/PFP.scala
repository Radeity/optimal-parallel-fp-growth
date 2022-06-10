package fdu.cst.bigdata
//import org.apache.spark.ml.fpm.FPGrowth
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

    //   val dataset = spark.createDataset(Seq(
    //     "1 2 5",
    //     "1 2 3 5",
    //     "1 2")
    //   ).map(t => t.split(" ")).toDF("items")

    print("\n \n \n")
    println(" -*-*-*-*  THIS IS PARALLEL FP-GROWTH Implementation *-*-*-*-")
    println()

    val transactionsFile = scala.io.StdIn.readLine("Insert the transactions file name within datas/: ")
    val testFile = scala.io.StdIn.readLine("Insert test filename: ")
    print("Insert Support por favor (between 0-1): ")
    val support = scala.io.StdIn.readDouble()

    print("Now, insert Confidence (between 0-1): ")
    val confidence  = scala.io.StdIn.readDouble() // STOPPED FOR FALAFEL ON READDOUBLE ... TEST IT WHEN BACK ...

    val train_path = "datas/train.txt"  //"datas/"+transactionsFile
    val test_path = "datas/test.txt"    //"datas/"+testFile

    val start = System.currentTimeMillis()
    val trainset =spark.read.textFile(train_path).map(t => t.split(" ").distinct).toDF("items")

    val testset =spark.read.textFile(test_path).map(t => t.split(" ").distinct).toDF("items")

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(support).setMinConfidence(confidence).setNumPartitions(2)
    val model = fpgrowth.fit(trainset)


    // Display frequent itemsets.
    val fpDF = model.freqItemsets
    fpDF.show()

    // Display generated association rules.
    val arDF = model.associationRules
    arDF.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    val resultDF = model.transform(testset)
    resultDF.show()

    val end = System.currentTimeMillis()

    val duration = (end-start)/1000

    println(s"\n ===== Total duration is: $duration ===== \n \n ")


    val outputFile = transactionsFile.split("\\.").head +"_"+ "S"+support.toString +"_"+ "C"+confidence.toString +"_"+ duration.toString


    fpDF.write.format("json").mode("overwrite").save("datas/output/FP_"+outputFile+".json")

    arDF.write.format("json").mode("overwrite").save("datas/output/AR_"+outputFile+".json")

    resultDF.write.format("json").mode("overwrite").save("datas/output/RESULT_"+outputFile+".json")
  }
}
