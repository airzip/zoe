package bank.bee.kafka

import akka.dispatch.forkjoin.ForkJoinPool
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.concurrent.ExecutionContext

object ConsumerGuava {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("group.id","test")
    properties.put("auto.offset.reset", "earliest")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("enable.auto.commit", "true")
    properties.put("session.timeout.ms", "30000")

    val consumerData = new KafkaConsumer[String,String](properties)
    consumerData.subscribe(Collections.singletonList("test"))

    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(new ForkJoinPool(4))
    while (true) {
      val msgs: ConsumerRecords[String,String] = consumerData.poll(Duration.ofSeconds(1))
      val it =msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        println(
          s"ts:${msg.timestamp()}," +
            s"partition: ${msg.partition()}," +
            s"offset: ${msg.offset()}," +
            s"key:${msg.key()}," +
            s"value:${msg.value()}"
        )
      }
    }

  }

}
