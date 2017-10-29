package com.pop.spark.streaming.actor

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout
import com.google.gson.Gson
import com.pop.spark.twitter.service.SparkResourceSetUp
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumerActorBuilder {
   // create the actor system
   def props(implicit timeout: Timeout) =  Props(new KafkaConsumerActor())
   def name = "kafkaConsumerActor"
}

/*
 * Kafka consumer Actor
 */
class KafkaConsumerActor extends Actor with ActorLogging {

  val sparkConf = SparkResourceSetUp.getSparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("spark-streaming")

  /*
  * Method that use the direct kafka consumer API
  */

  def consumeFromSparkStreamingApi(): Unit ={

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(new Gson().toJson(_)).print()
    ssc.start()
    ssc.awaitTermination()
  }

  override def receive : Actor.Receive = {
    case "consume" => consumeFromSparkStreamingApi
  }

  override def finalize(): Unit = super.finalize()

  val toActor = (data: String) => self ! consumeFromSparkStreamingApi

}



