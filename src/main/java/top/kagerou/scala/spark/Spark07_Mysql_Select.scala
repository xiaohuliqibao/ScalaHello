package top.kagerou.scala.spark

import org.apache.spark
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import java.util.Properties



object Spark07_Mysql_Select {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Spark_Mysql_Select")
    val ss = SparkSession.builder.config(sparkConf).getOrCreate()
    val url = "jdbc:mysql://??.mysql.cn-chengdu.rds.aliyuncs.com:3306/langbao?useUnicode=true&characterEncoding=UTF-8&allowPublicKeyRetrieval=true&useSSL=false"
    val table = "group_message"
    val properties = new Properties()

    properties.setProperty("driver","com.mysql.cj.jdbc.Driver")
    properties.setProperty("user","qibao")
    properties.setProperty("password","??!")
    val frame: DataFrame = ss.read.jdbc(url, table, properties)
    val messageJavaRDD: JavaRDD[Row] = frame.select( "message").where("sender_number == ?? AND group_number == ?? ").limit(20).toJavaRDD
    val messageRdd: RDD[Row] = messageJavaRDD.rdd
    val message: RDD[String] = messageRdd.map(_.mkString(","))
    message.collect().foreach(println)
    ss.close()

  }
}
