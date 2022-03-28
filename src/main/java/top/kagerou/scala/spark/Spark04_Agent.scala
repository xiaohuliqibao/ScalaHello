package top.kagerou.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Agent {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)
    val logFile: RDD[String] =  sc.textFile("src/main/resources/datas/agent.log")
    //agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    //统计出每一个省份每个广告被点击数量排行的 Top3

    //课程实现

    val prvAndAd: RDD[((String, String), Int)] = logFile.map(
      line => {
        ((line.split(" ")(1),line.split(" ")(4)),1)
      }
    )
    val prvAndAdAndCnt: RDD[(String, (String, Int))] = prvAndAd.reduceByKey(_ + _).map {
      case ((prv, ad), cnt) => {
        (prv, (ad, cnt))
      }
    }

    val prvGroup: RDD[(String, Iterable[(String, Int)])] = prvAndAdAndCnt.groupByKey()
    //scala 中list和seq的sortBy排序简单控制升序和降序的方法
    // sortBy(x => -x)降序
    // sortBy(x => x)升序
    val result: RDD[(String, List[(String, Int)])] = prvGroup.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    result.collect().foreach(println)

    sc.stop()
  }

}
