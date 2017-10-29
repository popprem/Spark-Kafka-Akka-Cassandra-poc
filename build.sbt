name := """spark-twitter-kafka-cassandra-example"""

version := "1.0.0"

scalaVersion := "2.11.8"

resolvers += "OSS Sonatype" at "https://repo1.maven.org/maven2/"

libraryDependencies ++= {
  val sparkVersion = "2.0.1"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVersion withSources(),
    "org.apache.spark"     %% "spark-mllib"             % sparkVersion withSources(),
    "org.apache.spark"     %% "spark-sql"               % sparkVersion withSources(),
    "org.apache.spark"     %% "spark-streaming"         % sparkVersion withSources(),
    "org.apache.bahir"     %% "spark-streaming-twitter" % "2.0.0"  withSources(),
    "com.google.code.gson" %  "gson"                    % "2.8.0"  withSources(),
    "org.twitter4j"        %  "twitter4j-core"          % "4.0.5"  withSources(),
    "com.github.acrisci"   %% "commander"               % "0.1.0"  withSources(),
    "com.datastax.spark" % "spark-cassandra-connector-embedded_2.11" % "2.0.0-M3" withSources(),
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3" withSources(),
    "org.apache.kafka" % "kafka_2.11" % "0.10.1.0" withSources(),
    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.2" withSources(),
    "com.typesafe.akka" % "akka-actor_2.11" % "2.4.14" withSources(),
    "com.typesafe.akka" % "akka-cluster_2.11" % "2.4.14" withSources(),
    "com.lmax" % "disruptor" % "3.3.6" withSources()
  )
}
