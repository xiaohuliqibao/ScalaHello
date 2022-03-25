package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_CountByKey {

  def main(args: Array[String]): Unit = {

    //AggregateByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

    val cntValue: collection.Map[String, Long] = lineList2.countByKey()
    cntValue.foreach(println)
    sc.stop()
  }
}
