package top.kagerou.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Par {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val linen2: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt",2)
    linen2.saveAsTextFile("output")

    sc.stop()
  }
}
