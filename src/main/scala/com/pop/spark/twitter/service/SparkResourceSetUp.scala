package com.pop.spark.twitter.service

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cwijayasundara on 25/11/2016.
  */
object SparkResourceSetUp {

  //TODO : externalize to .properties files

  val spark: SparkSession = SparkSession
    .builder
    .appName(appName)
    .getOrCreate()
  private val appName: String = "spark-twitter-kafka-cassandra-sample"
  private val threadProfile: String = "local[4]"
  private val timeInterval: Int = 2
  // change with your local settings
  private val twitterBaseDirPath: String = "/Users/cwijayasundara/Documents/self-learning/spark/tweet-classify/tweets"
  private val twitterModelDirPath: String = "/Users/cwijayasundara/Documents/self-learning/spark/tweet-classify/model"
  // create the SparkConfig
  private val sparkConfiguration: SparkConf = new SparkConf().setAppName(appName).setMaster(threadProfile)
  // create the sparkContext
  private val sparkContext: SparkContext = new SparkContext(sparkConfiguration)
  // create the streamingContext
  private val streamingContext = new StreamingContext(sparkContext, Seconds(timeInterval))
  private val sqlContext: SQLContext = spark.sqlContext

  def getAppName: String = {
    this.appName
  }

  def getThreadingProfile: String = {
    this.threadProfile
  }

  def getSparkConfiguration: SparkConf = {
    this.sparkConfiguration
  }

  def getSparkContext: SparkContext = {
    this.sparkContext
  }

  def getStreamingContext: StreamingContext = {
    this.streamingContext
  }

  def getTwitterBaseDir: String = {
    this.twitterBaseDirPath
  }

  def getTwitterModelDirPath: String = {
    this.twitterModelDirPath
  }

  def getSqlContext: SQLContext = {
    this.sqlContext
  }
}

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

object TwitterClassifier {

  private val numFeatures = 1000
  private val tf = new HashingTF(numFeatures)

  /** Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
    * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
    * This is a common way to decrease the number of features in a model while still getting excellent accuracy
    * (otherwise every pair of Unicode characters would potentially be a feature). */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq)
}
