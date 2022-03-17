package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Qibao_RDD_GroupBy {

  def main(args: Array[String]): Unit = {

    //GroupBy
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    //将数字按照奇偶分组
    val lineInt: RDD[Int] =  sc.textFile("src/main/resources/datas/2.txt",2).map(_.toInt)
    def groupFunction(num:Int): Int = {
       num % 2
    }

    val groupValue: RDD[(Int, Iterable[Int])] = lineInt.groupBy(groupFunction)
    groupValue.collect().foreach(println)

    /**
    *按照单词首字母分组,如果取单词出现的数量可以直接用迭代器的长度来计算 iter.size
    *groupStr.map{ case (chr,iter) => {(chr,iter.size)}}.collect.foreach(println)
    */
    val lineString = sc.textFile("src/main/resources/datas/1.txt").flatMap(_.split(" "))
    /**
     * 自写
    val groupStr: RDD[(String, Iterable[String])] = lineString.groupBy(str => {
      str.substring(0, 1)
    })
    */
    val groupStr: RDD[(Char, Iterable[String])] = lineString.groupBy(_.charAt(0))
    //groupStr.map{ case (chr,iter) => {(chr,iter.size)}}.collect.foreach(println)
    groupStr.collect.foreach(println)
    /**
     * 从服务器日志数据 apache.log 中获取每个时间段访问量。
     * */
    val lineFile = sc.textFile("src/main/resources/datas/apache.log")
    val hourRdd: RDD[(String)] = lineFile.map(
      line => {
        val data = line.split(" ")
        val date = data(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val sdf1 = new SimpleDateFormat("HH")
        val hour = sdf1.format(sdf.parse(date))
        hour
      })
    val hourGroup: RDD[(String, Iterable[String])] = hourRdd.groupBy( hour => {hour})
    //TODO 为什么要加case，还有map{}和map()的区别
    hourGroup.map{
      case (num,iter) => {
        (num, iter.size)
      }
    }.collect.foreach(println)

    sc.stop()
  }
}
