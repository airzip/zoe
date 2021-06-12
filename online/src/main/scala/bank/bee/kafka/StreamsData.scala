package bank.bee.kafka

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.time.Duration
import java.util.Properties

object StreamsData {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"KS")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.stringSerde)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.stringSerde)

    import Serdes._
    val streamsBuilder:StreamsBuilder = new StreamsBuilder()
    val streamsData: KStream[String,String] = streamsBuilder.stream[String,String]("test")

    val resultData = streamsData.flatMapValues(_.toLowerCase.split(" "))
      .groupBy((_,word)=>word).count()(Materialized.as("CountResult"))
    resultData.toStream.to("testETL")

    val stream: KafkaStreams = new KafkaStreams(streamsBuilder.build(), properties)
    stream.start()

    sys.ShutdownHookThread{ stream.close(Duration.ofSeconds(10)) }
  }

}
