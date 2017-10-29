package com.pop.spark.streaming.actor

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

object SparkCassandraInterationApp extends App {

  val sparkConfig = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[4]")
    .setAppName("StreamingApp")

  val cassandraConnection = CassandraConnector(sparkConfig)

  val sparkContext = new SparkContext(sparkConfig)

  val streamingContext = new StreamingContext(sparkContext, Seconds(3))

  val gson: Gson = new GsonBuilder().setPrettyPrinting().create
  val jsonParser = new JsonParser

  // get the live twitter stream and map to json
  val tweetStream: DStream[String] = TwitterUtils.createStream(streamingContext, None)
    .map(new Gson().toJson(_))

  writeDataToCassandra(tweetStream)
  readTwitterDataFromCassandra

  /* method to save the stream to Cassandra
   * "CREATE KEYSPACE twitter WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };"
   * create a table in Cassandra "create table twitter.tweet(id text PRIMARY KEY, tweet_text text);"
   */

  def writeDataToCassandra(tStream: DStream[String]): Unit = {

    tStream.foreachRDD((t) => {
      val id = math.random.toString
      t.take(50) foreach { tweet =>
        val tweetJson = gson.toJson(jsonParser.parse(tweet))
        val collection = sparkContext.makeRDD(Seq((id, tweetJson)))
        collection.saveToCassandra("twitter", "tweet", SomeColumns("id", "tweet_text"))
      }
    })
  }

  def readTwitterDataFromCassandra: Unit = {

    // read from Cassandra
    cassandraConnection.withSessionDo { session =>
      val result = session.execute("select * from twitter.tweet").all().toArray
      result foreach println
    }
  }

  streamingContext.start()
  streamingContext.awaitTermination()
}
