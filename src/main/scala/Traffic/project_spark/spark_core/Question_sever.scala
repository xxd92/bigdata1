package Traffic.project_spark.spark_core
import util.SparkContextUtil
import scala.collection.mutable._
/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-17 14:38:59
  */
object Question_sever {

  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[1]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
       val  all_monitor_flow_byday = all_monitor_flow.map(line=>{
       val lineArr = line.split("\t")
        val date = lineArr(0)
        val action_time_hour:String = (lineArr(4).split(" "))(1).split(":")(0)
        val action_time_hour_new:String = action_time_hour.toInt+1+""
        ((lineArr(0)),(date+"_"+ action_time_hour+"-"+action_time_hour_new,lineArr(3),lineArr(6),lineArr(7)))
      }).groupByKey().distinct().collect().map(ele=>{
         ele._2.map(ele1=>{
           (ele._1,(ele1._1,ele1._2))
         })
       })

    //all_monitor_flow_byday.foreach(println)

    val all_monitor_flow_byday_carcount = all_monitor_flow_byday.length //一天的车辆

    all_monitor_flow_byday.foreach(println)






  }
}

//date
//monitorId 模拟9个卡扣monitorId
// cameraId  模拟摄像头id cameraId
//car
//actionTime 模拟经过此卡扣开始时间，如2017-10-01 20:09:10
// speed 模拟速度
// roadId 模拟道路id[1-50个道路]
// areaId 模拟areaId[一共8个区域]
