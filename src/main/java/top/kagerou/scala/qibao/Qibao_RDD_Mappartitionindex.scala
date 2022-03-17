package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Mappartitionindex {

  def main(args: Array[String]): Unit = {

    //获取第二个数据分区的内容
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val listInt3: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, 4, List(5, 6)))
    val flatRDD: RDD[Any] = listInt3.flatMap({
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    })
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
