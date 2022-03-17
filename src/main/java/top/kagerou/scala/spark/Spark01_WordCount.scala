package top.kagerou.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)
    val linens: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt,src/main/resources/datas/1.txt")
    val words: RDD[String] = linens.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
