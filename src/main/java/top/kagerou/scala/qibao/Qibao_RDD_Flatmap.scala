package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Flatmap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineStr: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt",2)
    //获取第二个数据分区的内容
    val partition1RDD: RDD[String] = lineStr.mapPartitionsWithIndex(
      (index,iter) => {
        if (index == 1) {
          iter
        }
        else {
          Nil.iterator
        }
      }
    )
    //获取(分区号，数据)的RDD
    val partitionRDD = lineStr.mapPartitionsWithIndex(
      (index,iter) => {
            iter.map(num => {
              (index,num)
            })
      }
    )

    partition1RDD.collect().foreach(println)
    partitionRDD.collect().foreach(println)
    sc.stop()
  }
}
