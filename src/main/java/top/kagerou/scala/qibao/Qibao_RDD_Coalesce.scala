package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Coalesce {

  def main(args: Array[String]): Unit = {

    //Coalesce
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineString = sc.makeRDD(List(1,2,3,4,5,6),3)
    lineString.saveAsTextFile("oldOutput")
    /**
     * coalesce 默认是改变分区数量，通过shuffle来控制是否打乱每个数据中的数据，默认为false。
     * 若想更变分区数据均衡分布或者增加分区数据，则shuffle为true
     * 另外增加分区可以使用repartition来实现,repartition(numPartitions)的底层就是一个coalesce(numPartitions, shuffle = true)
     * */
    val newLineString: RDD[Int] = lineString.coalesce(2,true)
    //val newLineString: RDD[Int] = lineString.repartition(6)
    newLineString.saveAsTextFile("newOutput")
    sc.stop()
  }
}
