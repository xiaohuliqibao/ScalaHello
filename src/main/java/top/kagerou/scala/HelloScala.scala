package top.kagerou.scala

object HelloScala {
  def main(args: Array[String]): Unit = {
    val list = List("May","January","February","March", "April", "June")
    list.foreach( t => {
      println {
        t.hashCode % 7
      }
    })
  }
}
