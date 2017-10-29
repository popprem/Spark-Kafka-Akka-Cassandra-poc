package com.pop.spark.twitter.service

import java.io.File

trait TwitterFilterAndModeBase {

  val twitterBaseDirPath: String = SparkResourceSetUp.getTwitterBaseDir
  val twitterModelDirPath: String = SparkResourceSetUp.getTwitterModelDirPath

  val tweetDirectory: File = new File(twitterBaseDirPath)
  val modelDirectory: File = new File(twitterModelDirPath)
  val numClusters: Int = 10
  val numIterations: Int = 100

}
