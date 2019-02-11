package cleanlog

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Locale, TimeZone}

import cleanlog.bean.Log
import cleanlog.util.{AddressUtil, IPDataUtil, URLUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}


/**
  * description:  Spark版的数据清洗器
  * author: Mmy
  * create: 2018-08-04 14:31
  **/
object SparkCleaner {
  /*------------------------程序所使用的全部地址变量------------------------*/
  //ip字典文件在hdfs上的路径
  var IPDictionaryFileDir = ""
  //日志文件在hdfs上的路径
  var logFileDir = ""
  //清洗完成后结果文件的输出路径
  var cleanedFileDir = ""

  /*-----------------------程序所使用的全部日期格式化器-----------------------*/
  //将旧的字符串形式的时间转换成Date类型的对象的日期格式化器
  val df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.UK)
  df.setTimeZone(TimeZone.getTimeZone("GMT+8:00"))
  //将Date类型的时间转换成yyyy-MM-dd HH:mm:ss时间的日期格式化器
  val output = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * main方法，程序入口
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //接受程序提交过程中传递进来的参数
    IPDictionaryFileDir = args(0)
    logFileDir = args(1)
    cleanedFileDir = args(2)

    //创建sparkConf对象
    val conf = new SparkConf().setAppName("spark Cleaner")
    //根据SparkConf对象创建SparkContext对象
    val sc = new SparkContext(conf)
//    //加载IP字典文件
    val IPDictionaryArray = IPDataUtil.loadIPDictionaryFile(sc)
    //对IPDictionaryArray进行广播
    val IPDictionaryArrayBroadcast = sc.broadcast(IPDictionaryArray)
    //清洗数据完整流程
    clean(sc,IPDictionaryArrayBroadcast)
    //清洗完成后结束程序
    sc.stop()
  }

  /**
    * 清洗数据
    * @param sc
    */
  def clean(sc:SparkContext,IPDictionaryArrayBroadcast:Broadcast[Array[(Long,Long,String)]]): Unit ={
    //spark将hdfs上的日志文件读取成一个RDD对象
    val logFileRDD = sc.textFile(logFileDir);
    //对logFileRDD执行map算子，每循环处理一行，就构建一个Log对象
    //将log对象上的信息封装完毕后，对log对象调用toString()方法
    //map算子全部循环完毕后，结果形成一个cleanedFileRDD
    val cleanedFileRDD = logFileRDD.map{ line =>
      var log = new Log
      //获取包含IP和时间的字符串
      val theFirstQuoteIndex = line.indexOf("\"")
      val IPAndTimeStr = line.substring(0,theFirstQuoteIndex)
      //获取URL对应的字符串
      val theSecondQuoteIndex = line.indexOf("\"",theFirstQuoteIndex+1)
      val URLStr = line.substring(theFirstQuoteIndex+1,theSecondQuoteIndex)
      //设置log对象的其他信息（URL信息）
      log = URLUtil.setLogOther(URLStr,log)
      if (log==null){//表示数据可能是爬虫产生的
        ""
      }else{
        //修正包含ip时间的字符串中的时间部分
        val IPAndNewTimeStr = updateTime(IPAndTimeStr)
        //设置log对象的IP和时间信息
        setLogIPAndTime(log,IPAndNewTimeStr)
        //设置log对象的地址信息
        setLogAddress(IPDictionaryArrayBroadcast,log)
        log.toString
      }
    }
    //对cleanedFileRDD进行存储
    cleanedFileRDD.saveAsTextFile(cleanedFileDir)
  }

  /**
    * 设置log对象的地理位置
    *   1.先通过log对象身上已经封装的IP信息去查找对应的地址信息
    *   2.然后再将地址信息封装到log对象身上
    * @param IPDictionaryArrayBroadcast
    * @param log
    */
  def setLogAddress(IPDictionaryArrayBroadcast:Broadcast[Array[(Long,Long,String)]],log:Log):Unit = {
    //1.先通过log对象身上已经封装的IP信息去查找对应的地址信息
    val address = getAddressByIP(IPDictionaryArrayBroadcast,IPDataUtil.IP2Long(log.IP),0,IPDictionaryArrayBroadcast.value.length-1)
    //2.将查询出的对应的地理位置封装到log对象上
    val allAddressInfo = AddressUtil.get4PartFrom1AddressStr(address)
    log.country = allAddressInfo._1
    log.province = allAddressInfo._2
    log.city = allAddressInfo._3
    log.county = allAddressInfo._4
  }

  /**
    * 根据IP地址查询对应的地理位置
    * @param IPLongValue 给定的IP地址的Long型值
    * @return IP地址对应的地理位置
    */
  def getAddressByIP(IPDictionaryArrayBroadcast:Broadcast[Array[(Long,Long,String)]],IPLongValue:Long,beginIndex:Int,endIndex:Int):String = {
    if(beginIndex>endIndex){
      return ""
    }
    var currentIndex = (beginIndex + endIndex) / 2
    val ipArea = IPDictionaryArrayBroadcast.value(currentIndex)
    if (IPLongValue>= ipArea._1){
      if (IPLongValue<=ipArea._2){
        return ipArea._3
      }else{
        getAddressByIP(IPDictionaryArrayBroadcast,IPLongValue,currentIndex+1,endIndex)
      }
    }else{
      getAddressByIP(IPDictionaryArrayBroadcast,IPLongValue,beginIndex,currentIndex-1)
    }
  }

  /**
    * 设置log对象的time和ip属性
    * 实际上，根据Hive分析的需要，
    * 我们还需要将时间分隔成多个部分封装到log对象上，
    * 具体要封装的属性列表如下：
    *   1.ip地址 IP
    *   2.客户端访问时间，包含日期和时间的完整部分 clinetTime
    *   3.客户端访问日期，只包含日期部分 clinetDate
    *   4.客户端访问时间的Long型值  milliseconds
    *   5.客户端访问的时间的年份 clientTimeYear
    *   6.客户端访问的时间的月份儿 clientTimeMonth
    *   7.客户端访问的时间的天 clientTimeDay
    *   8.客户端访问的时间的小时 clientTimeHour
    * @param log log对象
    * @param IPAndNewTimeStr 包含ip和修正后的时间的字符串
    */
  def setLogIPAndTime(log:Log,IPAndNewTimeStr:String):Unit = {
    //1.把字符串中的时间部分截取出来
    val timeStartIndex = IPAndNewTimeStr.indexOf("[")+1
    val timeStopIndex = IPAndNewTimeStr.indexOf("]")
    val timeStr = IPAndNewTimeStr.substring(timeStartIndex,timeStopIndex)
    //封装字符串形式的日期和时间的完整部分到log对象上的clientTime属性
    log.clientTime = timeStr
    //将字符串类型的日期时间对象转换成LocalDateTime类型的对象
    val timeLocalDateTime = LocalDateTime.parse(log.clientTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    log.clientDate = timeLocalDateTime.toLocalDate.toString
    log.milliseconds = Timestamp.valueOf(timeLocalDateTime).getTime().toString
    log.clientTimeYear = timeLocalDateTime.getYear.toString
    log.clientTimeMonth = timeLocalDateTime.getMonthValue.toString
    log.clientTimeDay = timeLocalDateTime.getDayOfMonth.toString
    log.clientTimeHour = timeLocalDateTime.getHour.toString

    //2.把字符串中的IP部分截取出来
    val IPStartIndex = 0
    val IPStopIndex = IPAndNewTimeStr.indexOf("-")-1
    val IP = IPAndNewTimeStr.substring(IPStartIndex,IPStopIndex)
    //封装IP属性
    log.IP = IP
  }

  /**
    * 将任意时区的时间修改成东八区的时间
    * 示例：
    * 原始字符串长相：125.70.11.34 - - [23/Apr/2014:10:59:59 +0800]
    * 新的字符串长相：125.70.11.34 - - [2014-04-23 10:59:59]
    * 注意：上述示例的最后有一个空格
    * @param ipAndTimeStr 包含ip和时间的字符串
    */
  def updateTime(ipAndTimeStr:String): String ={
    //开始截取的位置
    var startLocation = ipAndTimeStr.indexOf("[") + 1
    //结束截取的位置
    var stopLocation = ipAndTimeStr.indexOf("]")
    //截取出来的原始的时间信息 示例：14/Aug/2014:19:15:53 -0700
    val oldTimeStr = ipAndTimeStr.substring(startLocation,stopLocation)
    //将日志中旧的字符串类型的时间转换成东八区的Date类型的时间
    val oldTime = df.parse(oldTimeStr)
    val newTimeStr = output.format(oldTime)
    val ipAndNewTimeStr = ipAndTimeStr.replace(oldTimeStr,newTimeStr)
    //将包含ip和修正后的时间的字符串返回
    ipAndNewTimeStr
  }
}
