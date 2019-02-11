package Traffic.bjsxt.util

import Traffic.bjsxt.conf.ConfigurationManager
import Traffic.bjsxt.constant.Constants
import Traffic.spark.MockData
import com.alibaba.fastjson.JSONObject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


/**
  * spark工具类
  */
object SparkUtils {

  /**
    * 根据当前是否本地测试的设置，决定如何设置SparkConf的master
    * @param conf  SparkConf对象
    * @return
    */
  def setMaster(conf:SparkConf) = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      conf.setMaster("local")
    }
  }

  /**
    * 获取SQLContext
    * 如果spark.local设置为true，那么就创建SQLContext;否则，创建HiveContext
    * @param sc
    * @return
    */
  def getSQLContext(sc:SparkContext) = {
    val local:Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      new SQLContext(sc)
    } else {
      new HiveContext(sc)
    }
  }

  def mockData(sc:SparkContext, sqlContext:SQLContext) = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    /**
      * 如何local为trye 说明在本地测试  应该生产模拟数据
      * false HiveContext 直接可以操作hive表
      */
    if (local) {
      MockData.mock(sc,sqlContext)
    }
  }

  /**
    * 获取指定日期范围内的卡扣信息
    * @param sqlContext
    * @param taskParamsJsonObject
    */
  def getCameraRDDByDateRange( sqlContext:SQLContext,  taskParamsJsonObject:JSONObject) = {
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val sql =
        "select * " + "from monitor_flow_action" + "where date >= '" + startDate + "'" + "and date <= '" + endDate + "'"
    val monitorDF = sqlContext.sql(sql)

    /**
      * repartition 可以提高stage的并行度
      */
    monitorDF.rdd
  }

  /**
    * 获取指定日期内出现指定车辆的卡扣信息
    * @param sqlContext
    * @param taskParamsJsonObject
    */
  def getCameraRDDByDateRangeAndCars( sqlContext:SQLContext,  taskParamsJsonObject:JSONObject) = {
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val cars = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_CARS)
    val carArr = cars.split(",")
    var sql = "SELECT * " + "FROM monitor_flow_action " + "WHERE date>='" + startDate + "' " + "AND date<='" + endDate + "' " + "AND car IN ("
    for (i <- 0 until carArr.length) {
      sql += "'" + carArr(i) + "'"
      if(i < carArr.length - 1){
        sql += ","
      }
    }
    sql += ")"

    val monitorDF = sqlContext.sql(sql)

    /**
      * repartition 可以提高stage的并行度
      */
    monitorDF.rdd
  }

  /**
    * 获取指定日期范围和指定区域内的卡扣信息
    * @param sqlContext
    * @param taskParamsJsonObject  传入的参数json队先后
    * @param a 区域
    * @return
    */
  def getCameraRDDByDateRangeAndArea( sqlContext:SQLContext, taskParamsJsonObject:JSONObject, a:String) = {
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)

    val sql = "SELECT * "+ "FROM monitor_flow_action "+ "WHERE date>='" + startDate + "' " + "AND date<='" + endDate + "'" + "AND area_id in ('"+a +"')"
    val monitorDF = sqlContext.sql(sql)
    monitorDF.show()

    /**
      * repartition可以提高stage的并行度
      */
    monitorDF.rdd
  }
}