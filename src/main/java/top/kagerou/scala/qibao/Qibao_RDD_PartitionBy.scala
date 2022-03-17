package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Qibao_RDD_PartitionBy {

  def main(args: Array[String]): Unit = {

    //PartitionBy
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val lineList: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4),("e",5),("f",6)))
    val data1: RDD[(String, String)] = sc.makeRDD(List(("May","Chengdu"),("January","Shanghai"),("February","Guangzhou"),("March","Chongqing"),("April","Beijing"),("June","Nanjing")))
    val data2: RDD[(String, String)] = sc.makeRDD(List(("May","Shanghai"),("January","Guangzhou"),("February","Chongqing"),("March","Guangzhou"),("April","Nanjing"),("June","Beijing")))

    /**
     * partitionBy(分区规则器)
     *
     *  --HashPartitioner(分区数量)
     *
     *  --RangePartitioner(分区数量,rdd,升序/降序,samplePointsPerPartitionHint = 20)
     */
    val newLineList: RDD[(String, Int)] = lineList.partitionBy(new HashPartitioner(2))

    /**
     * 自定义分区规则，自己构造分区器
     *
     * MyMouthPartitioner 按月分区分配数据
     */
    val dataUnion: RDD[(String, String)] = data1.union(data2)
    dataUnion.partitionBy(new MyMouthPartitioner(6)).saveAsTextFile("output")

    sc.stop()
  }
}

class MyMouthPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
  def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    //这并不能把6个月完全区分开来。如果是依据的实际按月可以使用case
    //TODO 但实际情况是我们的Key和numPartitions不会向月份一样规则，需要达到的目的是将key和从0开始的分区数一一做一个分配
    case "January" => 0
    case "February" => 1
    case "March" => 2
    case "April" => 3
    case "May" => 4
    case "June" => 5
    case _ => 0

  }

  override def equals(other: Any): Boolean = other match {
    case mouth: MyMouthPartitioner =>
      mouth.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode(): Int = numPartitions

}
