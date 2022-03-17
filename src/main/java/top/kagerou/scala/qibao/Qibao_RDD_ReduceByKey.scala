package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_ReduceByKey {

  def main(args: Array[String]): Unit = {

    //ReduceByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val data: RDD[(String, Int)] = sc.makeRDD(List(("Chengdu", 45), ("Shanghai", 54), ("Beijing", 42), ("Beijing", 12), ("Chengdu", 3), ("Shanghai", 4)))

    val sumData: RDD[(String, Int)] = data.reduceByKey((x: Int, y: Int) => {
      x + y
    })
    sumData.collect().foreach(println)
    sc.stop()
  }
}
