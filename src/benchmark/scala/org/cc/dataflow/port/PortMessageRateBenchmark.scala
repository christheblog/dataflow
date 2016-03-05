package com.cc.dataflow.port

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

import org.scalatest.FlatSpec

import org.cc.dataflow.Dataflow
import org.cc.dataflow.DependencyGraph
import org.cc.dataflow.port.Port


class PortMessageRateBenchmark extends FlatSpec {
  
  import Dataflow._
  import Port._
  
  implicit val dfcontext = DependencyGraph.newContext()
  
  "A system using ports" should "have a high throughpout" in {
    import Port._
    Thread.sleep(10000)
    val N = 1000000
    val PORT_COUNT = 250
    val destCount = 5
    
    val counter = new AtomicInteger(0)
    val ports = (1 to PORT_COUNT).map { _ => new Port[Int] }.toArray
    
    val latch = new CountDownLatch(1)
    ports.map{ p => batch(p,25) { i => 
      if(counter.getAndIncrement==N) latch.countDown
      else if(counter.get < N) {
        // Sending a message to x other ports from the list
        val c = counter.get
        for(i <- (c to (c+destCount))) { send(ports(i%ports.length),i) }
      }
    }}
    
    val start = System.currentTimeMillis()
    send(ports(0),1)
    // N messages have been reached
    latch.await()
    val end = System.currentTimeMillis()
    println(end-start+" ms")
    
    // If the latch has been released => p2 has received message from p1 
    assert(counter.get >= N)
    
    // Time to have a look at the memory ...
    Thread.sleep(3600 * 1000)
    // Prevents GC
    send(ports(0),1)
    //Thread.sleep(250)
    //assert(dfcontext.size()==PORT_COUNT * 2)
  }
  
}