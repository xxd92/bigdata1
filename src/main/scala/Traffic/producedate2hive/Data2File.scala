package Traffic.producedate2hive

import java.io._

import Traffic.bjsxt.util.{DateUtils, StringUtils}
import org.apache.spark.sql.{Row, RowFactory}

import scala.collection.mutable._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 14:50:06
  */
object Data2File  {
  val  MONITOR_FLOW_ACTION:String = "./monitor_flow_action"
  val  MONITOR_CAMERA_INFO = "./monitor_camera_info"

  def main(args: Array[String]): Unit = {
    CreateFile(MONITOR_FLOW_ACTION)
    CreateFile(MONITOR_CAMERA_INFO)
    println("running......")
    mock()
    println("finished")
  }
  def CreateFile(pathFileName:String):Unit={
    try {
      var file:File = new File(pathFileName)
      if (file.exists()){
        file.delete()
      }
      var createNewFile:Boolean = file.createNewFile()
      println("create file" + pathFileName + "success!")
      createNewFile
    } catch {
      case e:IOException =>println(e)
    } finally {
      false
    }
  }
  def WriteDataToFile(pathFilterName:String,newContent:String):Unit ={
    var fos:FileOutputStream = null
    var osw:OutputStreamWriter = null
    var pw:PrintWriter = null
    try {
      //产生一行模拟数据
      var content:String = newContent
      var file:File = new File(pathFilterName)
      fos = new FileOutputStream(file,true)
      osw = new OutputStreamWriter(fos,"UTF-8")
      pw = new PrintWriter(osw)
      pw.write(content+"\n")
      //注意关闭的先后顺序，先打开德厚关闭，后打开的先关闭
      pw.close()
      osw.close()
      fos.close()
    } catch {
      case e:IOException => println(e)
    }
  }
  //生成模拟数据
  def mock(): Unit ={
    var dataList:ArrayBuffer[Row] =  ArrayBuffer[Row]()
    var random:Random = new Random()
    val locations: List[String] =  List("鲁","京","京","京","沪","京","京","深","京","京")
    val date:String = DateUtils.getTodayDate()

    for (i <- 0 until  100) {
      var car:String = locations(random.nextInt(10)) + (65 + random.nextInt(26)).toChar.toString + StringUtils.fulfuill(5,random.nextInt(100000)+"")
      //baseActionTime模拟24小时
      var baseActionTime:String = date + " " + StringUtils.fulfuill(random.nextInt(24) +"")
      //这里for循环模拟每辆车经过不同打的卡扣不同的摄像头数据。
      for ( j <- 0 until random.nextInt(300)+1) {
        //模拟每个车辆没被30个摄像头拍摄后，时间上累加1小时。这样做事数据更加真实。
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
        //将数据写入到文件中
        var content:String = date+"\t"+monitorId+"\t"+cameraId+"\t"+car+"\t"+actionTime+"\t"+speed+"\t"+roadId+"\t"+areaId
        WriteDataToFile(MONITOR_FLOW_ACTION,content)
        var row:Row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId)
        dataList.append(row)
      }
    }

    /**
      * 生成monitor_id 对应camera_id表
      */
    var monitorAndCameras:HashMap[String,ArrayBuffer[String]] = HashMap() //一个卡扣对应两个摄像头
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



    var keys: Iterator[(String, ArrayBuffer[String])] = monitorAndCameras.iterator
    for (key <- keys) {
      var monitor_id:String = key._1
      var array:Seq[String] = key._2
      for (str<- array) {
        var content = monitor_id +"\t"+str
        WriteDataToFile(MONITOR_CAMERA_INFO,content)
      }
    }
  }
}
