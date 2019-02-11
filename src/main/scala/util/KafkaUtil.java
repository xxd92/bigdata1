package util;/**
 * @description kafka的工具类
 * @author:XXD 1418208762@qq.com
 * create: 2019-01-02 19:01:35
 */

import java.util.Properties;

/**
 *@description kafka
 *@author:XXD 1418208762@qq.com
 *create: 2019-01-02 19:01:35
 */
public class KafkaUtil {
    public static Properties getKafkaProducerProperties(){
    //使用java.util.Properties类的对象来封装一些链接kafka必备的配置属性
    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers","ma3:9092");
    //设置分区响应策略（0或者1或者all）
    kafkaProducerProperties.put("acks","all");
    kafkaProducerProperties.put("retries",0);
    kafkaProducerProperties.put("batch.size",16384);
    kafkaProducerProperties.put("linger.ms",1);
    //
    kafkaProducerProperties.put("buffer.memory",33554432);
    kafkaProducerProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return kafkaProducerProperties;
}
    //
    public static Properties getKafkaConsumerProperties(){
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put("bootstrap.servers", "ma3:9092");
        kafkaConsumerProperties.put("group.id", "test11111");
        kafkaConsumerProperties.put("enable.auto.commit", "true");
        kafkaConsumerProperties.put("auto.commit.interval.ms", "1000");
        kafkaConsumerProperties.put("auto.offset.reset", "earliest");
        kafkaConsumerProperties.put("session.timeout.ms", "30000");
        kafkaConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return  kafkaConsumerProperties;
    }

}
