package Traffic.spark

import Traffic.bjsxt.util.{DateUtils, StringUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import util.SparkContextUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockData {
  def mock(sc:SparkContext, sqlContext:SQLContext)={
    var dataList:ArrayBuffer[Row] =  ArrayBuffer[Row]()
    var random:Random = new Random()
    val locations: List[String] =  List("鲁","京","京","京","沪","京","京","深","京","京")
    val date:String = DateUtils.getTodayDate()

    //模拟300辆车
    for (i <- 0 until  3000) {
      var car:String = locations(random.nextInt(10)) + (65 + random.nextInt(26)).toChar.toString + StringUtils.fulfuill(5,random.nextInt(100000)+"")
      //baseActionTime模拟24小时
      var baseActionTime:String = date + " " + StringUtils.fulfuill(random.nextInt(24) +"")
      //这里for循环模拟每辆车经过不同的卡扣不同的摄像头数据。
      for ( j <- 0 until random.nextInt(300)+1) {
        //模拟每个车辆每被30个摄像头拍摄后，时间上累加1小时。这样做事数据更加真实。
        if ( j % 30 == 0 && j != 0) {
          baseActionTime = date + " " + StringUtils.fulfuill(baseActionTime.split(" ")(1).toInt+1+"")
        }
        //模拟经过此卡扣开始时间，如2017-10-01 20:09:10
        var actionTime:String = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60)+"") + ":" + StringUtils.fulfuill(random.nextInt(60)+"")
        var monitorId:String = StringUtils.fulfuill(4,random.nextInt(9)+"")//模拟9个卡扣monitorId
        var speed:String = random.nextInt(260)+1+"" //模拟速度
        var roadId:String = random.nextInt(50)+1+"" //模拟道路id[1-50个道路]
        var cameraId:String = StringUtils.fulfuill(5,random.nextInt(100000)+"") // 模拟摄像头id cameraId
        var areaId:String = StringUtils.fulfuill(2,random.nextInt(8)+1+"") // 模拟areaId[一共8个区域]
        var row:Row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId)
        dataList.append(row)
      }

    }
    //将数据持久化到内存中
    var rowRdd = sc.parallelize(dataList)
    //创建数据元信息
    val cameraFlowSchema: StructType = StructType(Seq(StructField("date",StringType,true)
      ,StructField("monitorId",StringType,true)
      ,StructField("cameraId",StringType,true)
      ,StructField("car",StringType,true)
      ,StructField("actionTime",StringType,true)
      ,StructField("speed",StringType,true)
      ,StructField("roadId",StringType,true)
      ,StructField("areaId",StringType,true)
    ))
    val monitor_flow_action_df = sqlContext.createDataFrame(rowRdd,cameraFlowSchema)
    //默认打印出来df里面的20行数据,注册一张临时表
    println("----打印 车辆信息数据----")
    monitor_flow_action_df.registerTempTable("monitor_flow_action")
    monitor_flow_action_df.show(1000)
    /**
      * 生成monitor_id 对应camera_id表
      */
    var monitorAndCameras:mutable.HashMap[String,ArrayBuffer[String]] = mutable.HashMap() //一个卡扣对应两个摄像头
    var index = 0
    for (row <- dataList) {
      var list:ArrayBuffer[String] = monitorAndCameras.getOrElse(row.getString(1),null)
      if (list == null) {
        list =  ArrayBuffer()
        monitorAndCameras.put(row.getString(1),list)
      }
      index += 1
      // 这里每个1000条数据随机插入一条数据，模拟出来标准表中的卡扣对应摄像头的数据。这个摄像头的数据不一定会在车辆数据中有。
      if (index % 1000 == 0) {
        list.append(StringUtils.fulfuill(5,random.nextInt(1000000)+""))
      }
      //row.getString(2) camera_id
      list.append(row.getString(2))
    }

    //清空原来的数据
    dataList.clear()
    //遍历摄像头id
    var keys: Iterator[(String, ArrayBuffer[String])] = monitorAndCameras.iterator
    for (key <- keys) {
      var monitor_id:String = key._1
      var array: mutable.Seq[String] = key._2
      var row:Row= null
      for (camera_id<- array) {
        row = RowFactory.create(monitor_id,camera_id);
        dataList.append(row)

      }
    }

    var monitorSchema :StructType= StructType(Seq(StructField("monitorId",StringType,true)
      ,StructField("cameraId",StringType,true)))
    rowRdd = sc.parallelize(dataList)

    var monitor_camera_df = sqlContext.createDataFrame(rowRdd,monitorSchema)
    monitor_camera_df.registerTempTable("monitor_camera_info")
    println("----打印 卡扣号对应摄像头号 数据----")
    monitor_camera_df.show(100)
  }


  def main(args: Array[String]): Unit = {
      val sc = SparkContextUtil.getSparkContextOnLocal("xxd","local[*]")
    mock(sc,new SQLContext(sc))
  }
}
