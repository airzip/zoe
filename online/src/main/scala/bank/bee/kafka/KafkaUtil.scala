package bank.bee.kafka

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties

object KafkaUtil {

  val properties = new Properties()
  properties.put("bootstrap.servers","localhost:9092")
  properties.put("acks", "all")                 // 响应方式
  properties.put("retries", "3")                  //消息发送尝试次数
  properties.put("batch.size", "16384")           //批消息处理大小
  properties.put("linger.ms", "1")                //请求延时
  properties.put("buffer.memory", "33554432")     //发送缓存区内存大小
  properties.put("request.timeout.ms", "60000")   //超时时间

  // 消息的序列化
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


}
