package org.cc.dataflow.port

import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.FlatSpec
import org.cc.dataflow.Dataflow
import org.cc.dataflow.port
import java.util.concurrent.CountDownLatch
import org.cc.dataflow.DependencyGraph


class PortTest extends FlatSpec {
  
  import Dataflow._
  import Port._
  
  implicit val dfcontext = DependencyGraph.newContext()
  
  "Messages" should "arrive and be in processed in order" in { 
    val port = new Port[Int]()
    val counter = new AtomicInteger(1)
    val latch = new CountDownLatch(100000)
    receive(port) { i => counter.compareAndSet(i, i + 1); latch.countDown() }
    (1 to 100000).foreach(i => send(port,i))
    latch.await()
    // If all messages have arrived in order, we should have 100 + 1 
    assert(counter.get==100001)
  }
  
  "2 Ports" should "be able to send each other messages" in { 
    val p1 = new Port[Int]()
    val p2 = new Port[Int]()
    val latch = new CountDownLatch(1)
    receive(p1) { i => send(p2,i+1) }
    receive(p2) { i => if(i==2) latch.countDown() }
    send(p1,1)
    latch.await()
    // If the latch has been released => p2 has received message from p1 
    assert(true)
  }
  
  "3 Ports" should "be able to send each other messages" in {
    val N = 10000
    val p1 = new Port[Int]()
    val p2 = new Port[Int]()
    val p3 = new Port[Int]()
    val latch = new CountDownLatch(1)
    // Cycle of messages. When we reach 100, the cycle is broken and the latch released
    receive(p1) { i => if(i==N) latch.countDown() else send(p2,i+1) }
    receive(p2) { i => if(i==N) latch.countDown() else send(p3,i+1) }
    receive(p3) { i => if(i==N) latch.countDown() else send(p1,i+1) }
    send(p1,1)
    latch.await()
    // If the latch has been released => p2 has received message from p1 
    assert(true)
  }
  
}