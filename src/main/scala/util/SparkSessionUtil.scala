package util

import org.apache.spark.sql.SparkSession

/**
  * @description 生成sparkSession实例的工具类
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-10 10:24:46
  */
object SparkSessionUtil {

  def SparkSessionRunOnlocalWithLocalMaster(appName:String)={
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder()
      .appName(appName).master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }


  def SparkSessionRunOnHDFSWithLocalMaster(appName:String)= {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder()
      .appName(appName).master("local[*]")
      .config("spark.jars", "C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata\\target\\bigdata-1.0-SNAPSHOT.jar")
      .config("spark.driver.host", "10.30.161.62")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def SparkSessionRunOnHDFSWithSparkMaster(appName:String)= {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.jars", "C:\\SoftWare\\WorkSoftWare\\Compiler\\IDEA\\WorkSpace\\bigdata\\target\\bigdata-1.0-SNAPSHOT.jar")
      .config("spark.master", "spark://ma3:7077")
      .config("spark.driver.host", "10.30.161.62")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def SparkSessionRunOnLocalWithSparkMaster(appName:String)= {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.master", "spark://ma3:7077")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }


}
