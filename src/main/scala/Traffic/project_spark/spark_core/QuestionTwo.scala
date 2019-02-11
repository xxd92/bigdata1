package Traffic.project_spark.spark_core

import util.SparkContextUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-15 20:34:07
  */
object QuestionTwo {
  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContextOnLocal(QuestionTwo.getClass.getName,"local[1]")
    val all_monitor_flow = sc.textFile("file:///C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata1\\monitor_flow_action")

    /**rdd.sortBy(_._2,false).top(10)(Ordering.by(e => e._2))。
    * 再次运行，果然能得到正确结果。后来再仔细想想，觉得sortBy()函数有点多余，
    * 于是变成rdd.top(10)(Ordering.by(e => e._2))。
    */

    val topN_monitor1 = all_monitor_flow.map(line =>{
        val lineArr = line.split("\t")
        (lineArr(1),1)
      }).reduceByKey(_+_).top(2)(Ordering.by(x=>x._2))
      .foreach(x =>println("车流量最高的卡口有"+x._1+"经过了"+x._2+"辆") )

println("-----------------------------------------------------------------------------")
  /**
    * 方式二
    */
    val topN_monitor2 = all_monitor_flow.map(line =>{
      val lineArr = line.split("\t")
      (lineArr(1),1)
    }).reduceByKey(_+_).sortBy(_._2,false).take(3)
      .foreach(x =>println("车流量最高的卡口有"+x._1+"经过了"+x._2+"辆") )


}
}

