package com.pop.spark.streaming.actor

import akka.actor.ActorSystem
import akka.util.Timeout

object KafkaMessageConsumerApp extends App{

  implicit val system = ActorSystem()
  val kafkaMessageConsumerActor = system.actorOf(KafkaConsumerActorBuilder.props(Timeout.zero), KafkaConsumerActorBuilder.name)
  kafkaMessageConsumerActor ! "consume"

}
