package bank.bee.kafka.producer

import akka.actor.{ActorSystem, Props}
import bank.bee.kafka.KafkaUtil.properties
import org.apache.kafka.clients.producer.KafkaProducer

import java.util.UUID
import scala.util.Random

object ProducerActorSystem {

  val messageData =
    s"""{
       |'ID':'${UUID.randomUUID()}',
       |'frequent':'${Random.nextInt(888)}'',
       |'success':'${Random.nextBoolean()}',
       |'fertilized':'${Random.nextGaussian()}',
       |'time':'${Random.nextFloat()}'
       |}""".stripMargin
  val producerData = new KafkaProducer[String,String](properties)

  val actorSystem = ActorSystem("Producer")
  val actor = actorSystem.actorOf(Props[ProducerActor])

  def main(args: Array[String]): Unit = {
    //var threshold:Int = 1
    while (true) {
      //threshold+=1
      println("Send ..." + messageData)
      actor ! messageData
      println(Thread.currentThread())
    }
  }
}
