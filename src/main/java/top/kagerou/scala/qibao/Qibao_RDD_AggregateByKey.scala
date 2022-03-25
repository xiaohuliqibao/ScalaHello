package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_AggregateByKey {

  def main(args: Array[String]): Unit = {

    //AggregateByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4),("a",5),("a",6)),3)
    val lineList2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

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

    /**
     * 求每个Key的平均值
     * 能想到的基本思路是先对Key进行一个计数，然后在求Key的value的总值。
     *
     * 将两个RDD进行第二部分进行计算，sum/cnt 求得平均值
     */
    //val sumValueRDD: RDD[(String, Int)] = lineList2.reduceByKey(_ + _)
    //val cntValue: collection.Map[String, Long] = lineList2.countByKey()
    val sum_cnt_RDD: RDD[(String, (Int, Int))] = lineList2.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val resultRDD: RDD[(String, Int)] = sum_cnt_RDD.mapValues {
      case (sum, cnt) => {
        sum / cnt
      }
    }

    aggregateValue.collect().foreach(println)
    aggregateValue1.collect().foreach(println)
    sum_cnt_RDD.collect().foreach(println)
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
