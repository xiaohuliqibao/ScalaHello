package top.kagerou.scala.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.util.Properties


object Spark06_Mysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)
    val ss = SparkSession.builder.config(sparkConf).getOrCreate()
    val url = "jdbc:mysql://??.mysql.cn-chengdu.rds.aliyuncs.com:3306/langbao?useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&useSSL=false"
    val table = "group_message"
    val properties = new Properties()

    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")
    properties.setProperty("user","qibao")
    properties.setProperty("password","??")

    var groupMessage: DataFrame = ss.read.jdbc(url, table, properties)
    groupMessage.show()

    sc.stop()

  }
}
