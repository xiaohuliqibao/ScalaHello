package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_SortBy {

  def main(args: Array[String]): Unit = {

    //SortBy
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineInt = sc.makeRDD(List(1,6,13,4,3,62),3)

    /**
     * function sortBy(排序规格，升序/降序，分区数量)
     * 排序存在shuffle行为会打乱数据重新分区
     */
    val sortByRDD: RDD[Int] = lineInt.sortBy(num => num, false, 2)
    sortByRDD.saveAsTextFile("output")
    sc.stop()
  }
}
