package cleanlog.util

import org.apache.spark.SparkContext
import cleanlog.SparkCleaner.IPDictionaryFileDir

/**
  * description: IP地址和字典文件相关的工具类
  * author: Mmy
  * create: 2018-08-10 14:34
  **/
object IPDataUtil {
  /**
    * 加载IP字典文件
    * @return
    */
  def loadIPDictionaryFile(sc:SparkContext):Array[(Long,Long,String)] ={
    sc.textFile(IPDictionaryFileDir).map{line =>
      val items = line.split(",")
      (IP2Long(items(0)), IP2Long(items(1)), items(4))
    }.collect() //RDD[(Long,Long,String)] ==> Array[(Long,Long,String)]
  }

  /**
    * 将给定的IP地址转出成Long型的值
    * @param IP 给定的IP地址 示例：1.0.8.0
    * @return 转换后的Long型的值 示例：16779264
    */
  def IP2Long(IP:String):Long = {
    val items = IP.split("\\.")
    items(0).toLong*256*256*256 + items(1).toLong*256*256 + items(2).toLong*256 + items(3).toLong
  }
}
