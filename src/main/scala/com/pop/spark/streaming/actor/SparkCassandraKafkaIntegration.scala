package com.pop.spark.streaming.actor

import com.datastax.spark.connector.cql.CassandraConnector
import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkCassandraKafkaIntegration {

  val sparkConfig = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[4]")
    .setAppName("StreamingApp")

  val cassandraConnection = CassandraConnector(sparkConfig)

  val sparkContext = new SparkContext(sparkConfig)

  val streamingContext = new StreamingContext(sparkContext, Seconds(3))

  val gson: Gson = new GsonBuilder().setPrettyPrinting().create
  val jsonParser = new JsonParser

}
