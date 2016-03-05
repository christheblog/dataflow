package org.cc.dataflow.channel

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import org.cc.dataflow.Dataflow
import org.cc.dataflow.DependencyGraph
import org.scalatest.FlatSpec


class ChannelMessageRateBenchmark  extends FlatSpec {
  
  import Dataflow._
  import Chan._
  
  "1 producer with 1 consumer (x 1000)" should "work" in {
    implicit val dfcontext = DependencyGraph.newContext()
    val Producers = 1
    val Count = 1000
    val Channel = 1000
    val latch = new CountDownLatch(Channel)
    // Creating 1 channel -> 
    (1 to Channel).toList.map { num =>
      val counter = new AtomicInteger(0)
      val channel = chan[Int](s"test-${num}")(dfcontext)
      def produce(n: Int): Unit = 
        if(n > 0) channel.write(n) { written => produce(n - 1) }
      def consume(): Unit = channel.read { n =>
        if (counter.incrementAndGet >= Producers * Count) latch.countDown
        else thread() { consume() }
      }
        
      consume()
      produce(Count)
    }
    
    val start = System.currentTimeMillis
    latch.await
    val end = System.currentTimeMillis
    println(s"${Count * Producers * Channel} messages sent in ${(end -start)/1000.0} seconds. Rate = ${(Count * Producers * Channel) / ((end -start)/1000.0)} msg / sec")
    assert(true)
  }
  
  "N producers with 1 consumer" should "be thread safe. Consumer should see all messages" in {
    implicit val dfcontext = DependencyGraph.newContext()
    val Producers = 20
    val Count = 10000
    val Channel = 1
    val latch = new CountDownLatch(Channel)
    // Creating 1 channel -> 
    (1 to Channel).toList.map { num =>
      val counter = new AtomicInteger(0)
      val channel = chan[Int](s"test-${num}")(dfcontext)
      def produce(n: Int): Unit = 
        if(n > 0) channel.write(n) { written => produce(n - 1) }
      def consume(): Unit = channel.read { n =>
        if (counter.incrementAndGet >= Producers * Count) latch.countDown
        else thread() { consume() }
      }
      
      consume()
      (1 to Producers).toList.map { _ => produce(Count) }
    }
    
    val start = System.currentTimeMillis
    latch.await
    val end = System.currentTimeMillis
    println(s"${Count * Producers * Channel} messages sent in ${(end -start)/1000.0} seconds by ${Producers} producers to 1 consumer. Rate = ${(Count * Producers * Channel) / ((end -start)/1000.0)} msg / sec")
    assert(true)
  }
  
  "1 producer with N consumers" should "be thread safe. Consumer should see all messages" in {
    implicit val dfcontext = DependencyGraph.newContext()
    val Producers = 1
    val Consumers = 10
    val Count = 1000
    val Channel = 1
    val latch = new CountDownLatch(Channel)
    // Creating Channel independent channel groups with N producers for M Consumer for each group -> 
    (1 to Channel).toList.map { num =>
      val channel = chan[Int](s"test-${num}")(dfcontext)
      def produce(n: Int): Unit = 
        if(n > 0) channel.write(n) { written => produce(n - 1) } else latch.countDown
      def consume(id: Int): Unit = channel.read { n =>
        //println(s"Consumer ${id} read value ${n}")
        consume(id)  
      }
      
      (1 to Consumers).toList.map { id => consume(id) }
      (1 to Producers).toList.map { _ => produce(Count * Consumers) }
    }
    
    val start = System.currentTimeMillis
    latch.await
    val end = System.currentTimeMillis
    println(s"${Count * Consumers} messages sent in ${(end -start)/1000.0} seconds by ${Producers} producer(s) to ${Consumers} consumer(s). Rate = ${(Count * Consumers) / ((end -start)/1000.0)} msg / sec")
    assert(true)
  }
  
  "N producer with M consumers" should "be thread safe. Consumers should see all messages" in {
    implicit val dfcontext = DependencyGraph.newContext()
    val Producers = 5
    val Consumers = 5
    val Count = 1000
    val Channel = 4
    val latch = new CountDownLatch(Producers * Channel)
    // Creating Channel independent channel groups with N producers for M Consumer for each group -> 
    (1 to Channel).toList.map { num =>
      val channel = chan[Int](s"test-${num}")(dfcontext)
      def produce(n: Int): Unit = 
        if(n > 0) channel.write(n) { written => produce(n - 1) } else latch.countDown
      def consume(id: Int): Unit = channel.read { n => consume(id) }
      
      (1 to Consumers).toList.map { id => consume(id) }
      (1 to Producers).toList.map { _ => produce(Count * Consumers) }
    }
    
    val start = System.currentTimeMillis
    latch.await
    val end = System.currentTimeMillis
    println(s"${Count * Consumers * Producers * Channel} messages sent in ${(end -start)/1000.0} seconds by ${Producers} producer(s) to ${Consumers} consumer(s). Rate = ${(Count * Consumers * Producers * Channel) / ((end -start)/1000.0)} msg / sec")
    assert(true)
  }
  
}