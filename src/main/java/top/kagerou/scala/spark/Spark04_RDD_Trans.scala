package top.kagerou.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Trans {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Transformations")
    val sc = new SparkContext(sparkConf)

    val linen1: RDD[String] =  sc.textFile("src/main/resources/datas/1.txt")
    val linen2: RDD[String] =  sc.textFile("src/main/resources/datas/2.txt")
    val linen3: RDD[String] =  sc.textFile("src/main/resources/datas/3.txt")
    val listInt1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val listInt2: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    val listInt3: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6)))

    /**
     * 试一下makeRDD和parallelize是不是一样的
     */
    if (listInt1 != listInt2) println("False")

    /**
     * RDD_map实现RDD的值或数据类型的基本转化
     * linen2.map(_.toInt * 2) = linen2.map(linen => (linen.toInt * 2))
     * */
    val valueInt2: RDD[Int] = linen2.map(_.toInt * 2)
    val arrayInt2: Array[Int] = valueInt2.collect()
    println("map操作RDD中的值的数据类型的转化和简单的数据计算")
    arrayInt2.foreach(println)
    /**
     * linen3.map(_.split(" ")) 会生成一个有Array的RDD 和 linen3.flatMap(_.split(" "))不同的是，后者是一个直接的RDD
     * */
    val valueArray3: RDD[Array[String]] = linen3.map(_.split(" "))
    val arrayInt3: Array[Array[String]] = valueArray3.collect()
    println("单独使用Map没有真正找到实现到FlatMap的扁平化效果")
    arrayInt3.foreach(
      arrayInt => arrayInt.foreach(println)
    )
    val valueArray3_1 = linen3.flatMap(_.split(" "))
    val arrayInt3_1: Array[String] = valueArray3_1.collect()
    arrayInt3_1.foreach(println)
    //flatMap将RDD[List[Int]] 转化为 RDD[Int]，把每一个元素单独分离开来。
    val arrayInt4: RDD[Int] = listInt3.flatMap({
      list => {
        list
      }
    })
    arrayInt4.collect().foreach(println)

    /**
     * def glom(): RDD[Array[T]]
     * 将同一个分区的数据转化为同类型的数组
     * */
    println("glom操作")
    val arrayIntRDD: RDD[Array[Int]] = listInt1.glom()
    arrayIntRDD.collect().foreach(data => {println( data.mkString(","))})

    val words: RDD[String] = linen1.flatMap(_.split(" "))
    //words.map((_, 1)) = words.map( word => (word,1))
    val word2One: RDD[(String, Int)] = words.map((_, 1))
    //rdd1.reduceByKey((x,y) => x+y) = rdd1.reduceByKey(_ + _)
    val word2Count: RDD[(String, Int)] = word2One.reduceByKey(_ + _)

    val array: Array[(String, Int)] = word2Count.collect()
    array.foreach(println)
    sc.stop()
  }

}

/**   makeAdd分区存放数据的规则
 *    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
 *      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
 */
