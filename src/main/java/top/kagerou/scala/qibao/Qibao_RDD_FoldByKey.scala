package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_FoldByKey {

  def main(args: Array[String]): Unit = {

    //FoldByKey
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",3),("a",4),("b",5),("a",6)),3)

    /** FoldByKey
     *
     * 当AggregateByKey分区间和分区内计算规则相同时可以使用FoldByKey来简化
     *
     * lineList.aggregateByKey(0)(_+_,_+_)  ==  lineList.FoldByKey(0)(_+_,_+_)
     */
    //zeroValue 分区内计算的初始值带入inPartition方法中和原数据进行计算，可能会影响计算结果。
    val foldValue = lineList.foldByKey(0)(_+_)

    foldValue.collect().foreach(println)
    sc.stop()
  }
}
