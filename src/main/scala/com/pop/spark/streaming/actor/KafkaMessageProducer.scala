package com.pop.spark.streaming.actor

import java.util.Properties

import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream


/**
  * You need to install Apache Kafka (0.10.1.0) and Zookeper 3.4.9
  * first start zookeper : <zookeper_home>/bin/zkServer start; this will start at localhost:2181
  * Then start Kafka : <kafka_home>/bin type kafka-server-start /usr/local/etc/kafka/server.properties --override property=
  * create a Kafka topic : <kafka_home>/bin type kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic poc
  * test the Kafka topic: <kafka_home>/bin type kafka-topics --zookeeper localhost:2181 --describe --topic poc
  * list all the Kafka topics: <kafka_home>/bin type kafka-topics --list --zookeeper localhost:2181
  * put some messages to the created topic: <kafka_home>/bin type kafka-console-producer --broker-list localhost:9092 --topic poc
  *                                                           This is a message from console
  *                                                           This is another message from console
  * Check if the messages are there in the topic : <kafka_home>/bin type kafka-console-consumer --zookeeper localhost:2181 --topic test --from-beginning
  * If you get the below error about unable to locate the leader then
  * edit the <kafka> server.properties and add port = 9092
  *                                            advertised.host.name = localhost
  */

class KafkaMessageProducer(tStream: DStream[String]) {

  // minimum config to connect to Kafka; you can write your own serializers.
  val topic = "spark-streaming"
  val brokers = "localhost:9092"
  val gson: Gson = new GsonBuilder().setPrettyPrinting().create
  val jsonParser = new JsonParser

  val kafkaStringSerializerClass = "org.apache.kafka.common.serialization.StringSerializer"

  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaStringSerializerClass)
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaStringSerializerClass)

  val producer = new KafkaProducer[String, String](kafkaProps)

  def publishMessagesToKafka : Unit ={
    tStream.foreachRDD((t) => {
      t.take(50) foreach { tweet =>
        val tweetJson = gson.toJson(jsonParser.parse(tweet))
        println("before sending to Kafka " + tweetJson)
        val kafkaTweetMessage = new ProducerRecord[String, String](topic,null,tweetJson)
        try{
          producer.send(kafkaTweetMessage).get()
        }catch{
          case ex :Exception => printf("Error accessing Kafka..")
        }
      }
    })
  }
}
