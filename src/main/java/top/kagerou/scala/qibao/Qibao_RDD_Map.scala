package top.kagerou.scala.qibao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Qibao_RDD_Map {

  def main(args: Array[String]): Unit = {
    /**
     * 从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
     * */
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Map")
    val sc = new SparkContext(sparkConf)

    val logFile: RDD[String] =  sc.textFile("src/main/resources/datas/apache.log")
    val url = logFile.map(
      line => {
        //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
        //可以用空格区分，url信息在第7个位置
        line.split(" ")(6)
      }
    )
    url.collect().foreach(println)
    sc.stop()
  }

}

