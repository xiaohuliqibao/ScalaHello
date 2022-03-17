package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Mappartition {

  def main(args: Array[String]): Unit = {

    //获取每个数据分区的最大
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineStr: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt",2)

    val lineInt: RDD[Int] = lineStr.map(_.toInt)

    val maxRDD: RDD[Int] = lineInt.mapPartitions(
      iter => {
        List(iter.max).toIterator
      }
    )

    maxRDD.collect().foreach(println)

    sc.stop()
  }
}
