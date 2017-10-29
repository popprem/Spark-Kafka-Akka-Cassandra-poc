package com.pop.spark.examples

import java.util.{Collections, Properties}

import com.pop.spark.streaming.actor.SparkCassandraKafkaIntegration
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.{CommitFailedException, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object KafkaMessageConsumer extends SparkCassandraKafkaIntegration with App{

  val topics = Array("spark-streaming")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //consumeFromSparkStreamingApi

  consumeFromDirectKafkaApi

  /*
   * Method that use the direct kafka consumer API
   */
  def consumeFromSparkStreamingApi(): Unit ={

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(new Gson().toJson(_)).print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
  /*
   * Plain Kafka consumer API usage
   */
  def consumeFromDirectKafkaApi(): Unit = {

    val kafkaProps: Properties = new Properties
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("group.id", "group1")
    kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put("auto.offset.reset", "earliest")
    //kafkaProps.put("enable.auto.commit", "true") // can cause duplicate message consumption in case of rebalance from Kafka
    kafkaProps.put("enable.auto.commit", "false")

    val kafkaConsumer = new KafkaConsumer[String, String](kafkaProps)
    kafkaConsumer.subscribe(Collections.singletonList("spark-streaming"))

    try {
      // infinite loop. App need pooling for the messages. Need to close the consumer while testing.
      while (true) {
        val numberOfMessages = kafkaConsumer.poll(1000).count()
        println("Number of messages in the Kafka topic is " + numberOfMessages)
        try {
          kafkaConsumer.commitSync() // commit the ack to Kafka broker
          //kafkaConsumer.commitAsync() // commit to Kafka async - without blocking the thread but can not retry
        } catch{
          case ex => CommitFailedException
            printf("Error while connecting to Kafka..")
        }
      }
    }finally {
        kafkaConsumer.close()
      }
  }
}
