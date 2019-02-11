package Traffic.project_spark.spark_core

import util.SparkContextUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-16 17:27:38
  */
object Question_Five {
  def main(args: Array[String]): Unit = {
    val date = "2019-01-16"
    val monitor_id = "0001"
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[1]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
    val all_monitor_flowbyday = all_monitor_flow.filter(line =>line.toString.split("\t")(0).equals(date) )

    val allcar_0001 = all_monitor_flowbyday.filter(_.toString.split("\t")(1).trim.equals(monitor_id))
      .map(line=>{
        val lineArr = line.split("\t")
        lineArr(3)
      }).collect().toSet

    val allcar_0001_broadcast = sc.broadcast(allcar_0001)
    all_monitor_flowbyday.filter(line=>allcar_0001_broadcast.value.contains(line.toString.split("\t")(3)))
      .map(line=>{
      val lineArr = line.split("\t")
      (lineArr(3),lineArr(4),(lineArr(6),lineArr(7)))
    }).sortBy(ele=>(ele._1,ele._2))
    .foreach(println)
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
