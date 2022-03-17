package top.kagerou.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount").set("spark.ui.port", "34040")
    val sc = new SparkContext(sparkConf)
    val linens: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt,src/main/resources/datas/1.txt")
    val words: RDD[String] = linens.flatMap(_.split(" "))

    val word2One: RDD[(String, Int)] = words.map((_, 1))
    //rdd1.reduceByKey((x,y) => x+y) = rdd1.reduceByKey(_ + _)
    val word2Count: RDD[(String, Int)] = word2One.reduceByKey(_ + _)

    val array: Array[(String, Int)] = word2Count.collect()
    array.foreach(println)
    sc.stop()
  }

}
