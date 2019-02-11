package Traffic.spark.kafkapruducer

import java.util.Properties

import Traffic.bjsxt.util.{DateUtils, StringUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import util.KafkaUtil

import scala.util.Random

/**
  * @description $description
  * @author:XXD 1418208762@qq.com
  *             create: 2019-01-16 09:36:36
  */
object MockRealTimeData {
  val TOPIC:String ="trafficFlow"
  val random:Random = Random
  val locations:Array[String] = Array("鲁","京","京","京","沪","京","京","深","京","京")
  val kafkaProducerProperties:Properties = KafkaUtil.getKafkaProducerProperties

  def main(args: Array[String]): Unit = {
    while(true){
      val producer = new KafkaProducer[String,String](kafkaProducerProperties)
      val date:String = DateUtils.getTodayDate()
      var baseActionTime:String = date + " " + StringUtils.fulfuill(random.nextInt(24)+"")
       baseActionTime = date + " " + StringUtils.fulfuill((baseActionTime.split(" ")(1).toInt+1)+"")
      val actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60)+"") + ":" + StringUtils.fulfuill(random.nextInt(60)+"")
      val monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");
      val car = locations(random.nextInt(10)) + (65+random.nextInt(26)).toChar + StringUtils.fulfuill(5,random.nextInt(99999)+"");
      val speed = random.nextInt(260)+"";
      val roadId = random.nextInt(50)+1+"";
      val cameraId = StringUtils.fulfuill(5, random.nextInt(9999)+"");
      val areaId = StringUtils.fulfuill(2,random.nextInt(8)+"");
      //生产者向“Topic”
      producer.send(new ProducerRecord[String, String](TOPIC,date+"\t"+monitorId+"\t"+cameraId+"\t"+car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId ))
      try { //根据上述构建的properties对象构建kafka生产者
        Thread.sleep(10000)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally producer.close()
    }
  }
}
