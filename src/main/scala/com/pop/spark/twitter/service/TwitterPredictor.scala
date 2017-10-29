package com.pop.spark.twitter.service

import java.io.File

import com.pop.spark.twitter.service.TwitterClassifier.featurize
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterPredictor extends App {

  val ssc: StreamingContext = SparkResourceSetUp.getStreamingContext
  val sc: SparkContext = SparkResourceSetUp.getSparkContext
  val numCluster: Int = 10
  val modelDir: String = SparkResourceSetUp.getTwitterModelDirPath
  val modelDirectory: File = new File(modelDir)

  new TwitterPredictorExec(sc, ssc, numCluster, modelDirectory).executeOperation()
}

class TwitterPredictorExec(sc: SparkContext, ssc: StreamingContext, numCluster: Int, modelDirectory: File) {

  def executeOperation(): Unit = {
    println("Initializing the the KMeans model...")

    val model: KMeansModel = new KMeansModel(sc.objectFile[Vector](modelDirectory.getCanonicalPath).collect)

    println("Materializing Twitter stream...")

    val twitterStream: DStream[String] = TwitterUtils.createStream(ssc, None).map(_.getText)
    twitterStream.print()
    twitterStream.foreachRDD { rdd =>
      rdd.filter(t => model.predict(featurize(t)) == numCluster)
        .foreach(print) // register DStream as an output stream and materialize it
    }
    println("Initialization complete, starting streaming computation.")
    ssc.start()
    ssc.awaitTermination()
  }
}