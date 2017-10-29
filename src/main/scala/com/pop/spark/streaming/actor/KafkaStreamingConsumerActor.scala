package com.pop.spark.streaming.actor

import java.util.Properties
import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import com.pop.spark.twitter.service.SparkResourceSetUp
import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingConsumerActor extends Actor with ActorLogging {

  val sparkConf = SparkResourceSetUp.getSparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("poc")

  /*
   * Consume messages off Kafka
   */
  def consumeMessagesOffKafka: Unit ={

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(new Gson().toJson(_)).print()

    ssc.start()
    ssc.awaitTermination()

  }

  def receive : Actor.Receive = {
    case e => // ignore
  }
}

object KafkaStreamingProducerActor extends Actor with ActorLogging {

  val topic = "poc"
  // Kafka broker host:port
  val brokers = "localhost:9092"
  val kafkaStringSerializerClass = "org.apache.kafka.common.serialization.StringSerializer"

  // minimum config to connect to Kafka; you can write your own serializers.
  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaStringSerializerClass)
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaStringSerializerClass)

  val producer = new KafkaProducer[String, String](kafkaProps)

  val sc = SparkResourceSetUp.getSparkContext
  val sourceDir = new File(SparkResourceSetUp.getTwitterBaseDir)
  val toActor = (data: String) => self ! publishMessages

  /* Extract tweets from the disk and place in Kafka
   *
   */
  def publishMessages: Unit ={
      val tweets: RDD[String] = sc.textFile(sourceDir.getCanonicalPath)

      val gson: Gson = new GsonBuilder().setPrettyPrinting().create
      val jsonParser = new JsonParser

      tweets.take(5) foreach { tweet =>
        println(gson.toJson(jsonParser.parse(tweet)))
        log.info(gson.toJson(jsonParser.parse(tweet)))
        val kafkaTweetMessage = new ProducerRecord[String, String](topic,null, gson.toJson(jsonParser.parse(tweet)))
        try{
          producer.send(kafkaTweetMessage).get()
        }catch{
          case ex: Exception => printf("Error while accesing Kafka")
        }
    }
  }

  def receive : Actor.Receive = {
    case e => // ignore
  }
}
