package com.pop.disruptor

import com.lmax.disruptor.dsl.{Disruptor, ProducerType}
import com.lmax.disruptor._
import java.util.concurrent.{Executors}


case class ValueEvent(var value: Long)

case class ValueEventTranslator(value: Long) extends EventTranslator[ValueEvent] {
  def translateTo(event: ValueEvent, sequence: Long) = {
    event.value = value
    event
  }
}

class ValueEventHandler extends EventHandler[ValueEvent] {
  def onEvent(event: ValueEvent, sequence: Long, endOfBatch: Boolean) {
    // print all the odd numers..
    if (event.value % 2 == 1)
      println(event.value)
  }
}

object DisruptorExecutor extends App {

    val ring_size = 1024 * 8
    val executor = Executors.newFixedThreadPool(10)
    val blockingWaitStrategy = new BlockingWaitStrategy

    val factory = new EventFactory[ValueEvent] {
      def newInstance() = ValueEvent(0L)
    }

    val handler = new ValueEventHandler

    val disruptor = new Disruptor[ValueEvent](factory,ring_size,executor,ProducerType.MULTI,blockingWaitStrategy)

    disruptor.handleEventsWith(handler)

    disruptor.start()

    //Publishing
    for (i <- 1 to 1000000) {
      disruptor.publishEvent(ValueEventTranslator(i))
    }
    Thread.sleep(2000)

    disruptor.shutdown()
    executor.shutdown()
}
