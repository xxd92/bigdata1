package Traffic.project_spark.spark_core

import util.SparkContextUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-16 19:48:17
  */
object Question_Six {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[1]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
    all_monitor_flow.map(line=>{
      val lineArr = line.split("\t")
      ((lineArr(4)+"_"+lineArr(3)),lineArr(2))
    }).groupByKey().map(ele=>{
      val eleArr = ele._1.split("_")

      (eleArr(1),eleArr(0),ele._2.toSet.size)
    }).filter(_._3 > 1).foreach(ele=>{
      println("车牌号为"+ele._1+"在"+ele._2+"有套牌行为")})
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