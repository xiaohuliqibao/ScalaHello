package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Cogroup {

  def main(args: Array[String]): Unit = {

    //Cogroup
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("b",3),("a",4),("c",5),("c",6)),3)
    val lineList2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

    val joinRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = lineList.cogroup(lineList2)
    joinRDD.collect().foreach(println)
    sc.stop()
  }
}
