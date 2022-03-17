package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Glom {

  def main(args: Array[String]): Unit = {

    //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineInt: RDD[Int] =  sc.textFile("src/main/resources/datas/2.txt",2).map(_.toInt)

    /**
     * 倒是实现了但是没有用到Glom
    val partitionMax: RDD[Int] = lineInt.mapPartitions(
      iter => {
        List(iter.reduce(_ max _)).iterator
      }
    )
    val sum: Int = partitionMax.reduce(_ + _)
    */

    val glomRDD: RDD[Array[Int]] = lineInt.glom()
    val maxRDD: RDD[Int] = glomRDD.map(data => {
      data.max
    })
    val sum: Int = maxRDD.collect().sum
    println(sum)
    sc.stop()
  }
}
