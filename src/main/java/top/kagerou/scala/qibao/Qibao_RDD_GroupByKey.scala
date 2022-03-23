package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Qibao_RDD_GroupByKey {

  def main(args: Array[String]): Unit = {

    //GroupByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("b",4),("e",5),("f",6)))

    val groupList: RDD[(String, Iterable[Int])] = lineList.groupByKey()

    /**
     * GroupByKey 在做shuffle操作的时候，不会改变原数据内容和数据量，所以相比于ReduceByKey操作在在预聚合combine之后，通过聚合降低了shuffle时数据在磁盘上落盘量，
     * 相当于时减少了磁盘的OI,提升了操作的性能。
     */
    groupList.collect.foreach(println)
    sc.stop()
  }
}
