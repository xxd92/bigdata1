package Traffic.project_spark.spark_core

import util.SparkContextUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-16 08:54:51
  */
object Question_Four {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[*]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
    //all_monitor_flow.take(10).foreach(println)
    val all_monitor_id_speed = all_monitor_flow.map(line =>{
      val lineArr = line.split("\t")
      var monitor_id = lineArr(1)
      val speed = lineArr(5).toDouble
      speed match {
        case speed if(speed >= 120) => monitor_id = lineArr(1)+"_high"
        case speed if(speed >= 90 &&  speed < 120) => monitor_id = lineArr(1)+"_middle"
        case speed if(speed >= 60 && speed < 90) =>monitor_id = lineArr(1)+"_normal"
        case speed if(speed > 0 && speed < 60) =>monitor_id = lineArr(1)+"_low"
      }
      (monitor_id,1)
    }).reduceByKey(_+_).map(ele=>{
      val eleArr = ele._1.split("_")
      (eleArr(0),eleArr(1)+"_"+ele._2)
    }).groupByKey().map(ele=>{
      var high = 0
      var middle = 0
      var normal = 0
      var low = 0
      for(x <- ele._2){
        val xArr = x.split("_")
        val spendmodecount = xArr(0)
        spendmodecount match {
          case "high" => high = xArr(1).toInt
          case "middle" => middle = xArr(1).toInt
          case "normal" => normal = xArr(1).toInt
          case "low" => low = xArr(1).toInt
        }
      }
      (ele._1,high,middle,normal,low)
    }).sortBy(ele=>(ele._2,ele._3,ele._4,ele._5),false).take(8).foreach(println)











  }
}
//4.车辆通过速度相对比较快的topN卡扣
//车速：
//120=<speed 		高速
//90<=speed<120	中速
//60<=speed<90	正常
//0<speed<60		低速