package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Qibao_RDD_Distinct {

  def main(args: Array[String]): Unit = {

    //Distinct
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineString = sc.textFile("src/main/resources/datas/1.txt").flatMap(_.split(" "))
    val groupStr: RDD[String] = lineString.distinct()
    //groupStr.map{ case (chr,iter) => {(chr,iter.size)}}.collect.foreach(println)
    groupStr.collect.foreach(println)
    sc.stop()
  }
}
