package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_AggregateByKey {

  def main(args: Array[String]): Unit = {

    //AggregateByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4),("a",5),("a",6)),3)

    def inPartition(x: Int,y: Int): Int = {
      return math.max(x,y)
    }
    def outPartition(x: Int,y: Int): Int = {
      x + y
    }

    /**
     * AggregateByKey 适用于实现分区内计算规则和分区间计算规则不相同的情况中。
     */
    //zeroValue 分区内计算的初始值带入inPartition方法中和原数据进行计算，可能会影响计算结果。
    val aggregateValue = lineList.aggregateByKey(3)((x, y) => inPartition(x, y), (x, y) => outPartition(x, y))

    val aggregateValue1 = lineList.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    aggregateValue.collect().foreach(println)
    aggregateValue1.collect().foreach(println)
    sc.stop()
  }
}
