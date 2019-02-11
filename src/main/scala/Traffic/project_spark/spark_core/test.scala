package Traffic.project_spark.spark_core

import scala.collection.mutable.HashMap

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-18 16:13:39
  */
object test {
  def main(args: Array[String]): Unit = {
    val map =  HashMap[String,Int]("1" ->1,"xxd2" ->2 )
    println(map.getOrElse("1",0))

  }

}
