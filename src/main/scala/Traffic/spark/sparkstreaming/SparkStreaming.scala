package Traffic.spark.sparkstreaming
import Traffic.spark.kafkapruducer.MockRealTimeData.TOPIC
import util.SparkSessionUtil

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-16 11:10:05
  */
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtil.SparkSessionRunOnlocalWithLocalMaster("SparkStreaming")
    val rawDataSet=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ma3:9092")
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .load()
      //.withWatermark("timestamp","1 minutes");
    rawDataSet.toDF().rdd.foreach(println)

  }
}
