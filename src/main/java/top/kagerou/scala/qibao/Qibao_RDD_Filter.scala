package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object Qibao_RDD_Filter {

  def main(args: Array[String]): Unit = {

    //Filter
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    /**
     * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
     * */
    val lineFile = sc.textFile("src/main/resources/datas/apache.log")
    /**尝试一下，有很多null，没有改变line的数量。
    val dayRdd: RDD[(String)] = lineFile.map(
      line => {
        val data = line.split(" ")
        val date = data(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val sdf1 = new SimpleDateFormat("yyyyMMdd")
        val day = sdf1.format(sdf.parse(date))
        if (day == "20150517") line else null
      })

    def filterFunction(line:String,dayCondition:String): Boolean ={
      val data = line.split(" ")
      val date = data(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val sdf1 = new SimpleDateFormat("yyyyMMdd")
      val day = sdf1.format(sdf.parse(date))
      if (day == dayCondition) true else false
      //day.startsWith(dayCondition)
    }

    val dayFilter = lineFile.filter( day => filterFunction(day,"20150517") )
     */
    //筛选 2015 年 5 月 17 日的数据
    var dayFilter =  lineFile.filter( line => {
      val data = line.split(" ")
      val date = data(3)
      date.startsWith("17/05/2015")
    })
    //获取数据的请求路径
    dayFilter.map(day => {
      day.split(" ")(6)
    }).collect.foreach(println)

    sc.stop()
  }
}
