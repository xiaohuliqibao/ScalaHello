package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_CombineByKey {

  def main(args: Array[String]): Unit = {

    //CombineByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)



    /**
     * 求每个Key的平均值
     * 能想到的基本思路是先对Key进行一个计数，然后在求Key的value的总值。
     *
     * 将两个RDD进行第二部分进行计算，sum/cnt 求得平均值
     */

    val sum_cnt_RDD: RDD[(String, (Int, Int))] = lineList2.combineByKey(
      v => (v,1),
      (t:(Int,Int), v) => (t._1 + v, t._2 + 1),
      (t1:(Int,Int), t2:(Int,Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val resultRDD: RDD[(String, Int)] = sum_cnt_RDD.mapValues {
      case (sum, cnt) => {
        sum / cnt
      }
    }

    sum_cnt_RDD.collect().foreach(println)
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
