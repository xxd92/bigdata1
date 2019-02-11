package Traffic.project_spark.spark_core

import util.SparkContextUtil

import scala.collection.mutable._

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-18 14:34:45
  */
object Question_Eight {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[1]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
    val xxd = all_monitor_flow.map(line=>{
      val lineArr = line.split("\t")
      ((lineArr(0),lineArr(3),lineArr(4)),lineArr(1))
    }).sortByKey().collect()

      sc.parallelize(xxd).map(ele=>{
      ((ele._1._1,ele._1._2),ele._2)
    }).groupByKey().map(ele=>{
       val map =  HashMap[String,Int]()
        val allmonitorId = ele._2.toList
        val allmonitorIdlength = ele._2.toList.length
        for(i <- 0 until allmonitorIdlength){
          val stringBuffer = new StringBuffer
               stringBuffer.append(allmonitorId(i)+"_")
          for(j <- i+1 until allmonitorIdlength){
            stringBuffer.append(allmonitorId(j)+"_")
            if(map.contains(stringBuffer.toString))
              map.put(stringBuffer.toString,map.getOrElse(stringBuffer.toString,0)+1)
            else
              map.put(stringBuffer.toString,1)
          }
        }
        val map1 = Map[String,Double]()
        for(i <- map.keys){
          val j = i.split("_")
          val k = j.length-1
          if(map.contains(j.drop(k).toString)){
            //map1.put(i,((map.getOrElse(i,0))/(map.getOrElse(j.drop(k).toString,1))))
            map1.put("xxd",1)
          }
        }
        map.foreach(println)
        map1.foreach(println)

      }).foreach(println)
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
