package com.pop.spark.examples

import akka.actor.{Actor, ActorSystem, Props}

object ActorSample extends App{

  class Actor1 extends Actor{

    override def clone(): AnyRef = super.clone()

    override def finalize(): Unit = super.finalize()

    override def receive ={
      case s:String => println("String " + s)
      case i:Int => println("Int " + i)
    }

    def foo = printf("normal method")
   }

  val system = ActorSystem()
  val actor = system.actorOf(Props[Actor1])

  println("before the string")
  actor ! "hello world"
  println("after the string")
  println("before the int")
  actor ! 434
  println("after the int")

}
