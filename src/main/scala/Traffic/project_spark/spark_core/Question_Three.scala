package Traffic.project_spark.spark_core

import util.SparkContextUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 21:21:58
  */
object Question_Three {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[*]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")
   //all_monitor_flow.take(10).foreach(println)
    all_monitor_flow.map(line =>{
      val lineArr = line.split("\t")
      (lineArr(1),line)
  }).groupByKey().map(ele=>{
      (ele._2.toList.size,ele._2)
    }).sortBy(_._1,false).take(1).map(ele=>{
      ele._2.toList.foreach(println)
    })
}
}
