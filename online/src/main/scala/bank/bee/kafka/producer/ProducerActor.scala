package bank.bee.kafka.producer

import akka.actor.Actor
import bank.bee.kafka.producer.ProducerActorSystem.{messageData, producerData}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

class ProducerActor extends Actor{
  def receive: Receive = {
    case message =>
      val sexData:RecordMetadata = producerData.send(new ProducerRecord[String,String]("love",messageData)).get()
      println("Sex: " + sexData.toString)
      Thread.sleep(300)
  }
}
