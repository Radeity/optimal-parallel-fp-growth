package fdu.cst.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 王维饶
 * @date 2022/6/8 1:46 PM
 * @version 1.0
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    // 建立和Spark框架连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    println(sc.applicationId)
    // 执行业务操作
    wordCount(sc)

    // 关闭连接
    sc.stop()
  }

  def wordCount(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.textFile("datas/111.txt")
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val count: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    println(count.collect().size)
    count.collect().foreach(println)
  }
}
