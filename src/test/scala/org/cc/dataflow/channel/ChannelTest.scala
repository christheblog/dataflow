package org.cc.dataflow.channel

import org.scalatest.FlatSpec
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import org.cc.dataflow.DependencyGraph


class ChannelTest extends FlatSpec {
  
  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.channel.Chan._
  
  implicit val dfcontext = DependencyGraph.newContext()
  
  "Write to / Read from a chan" should "work" in {
    val channel = chan[Int]("test")(dfcontext)
    channel.writeAll(12) 
    val check = channel.read { n => n }
    assert(check() == 12)
  }
  
  "Read from / Write to a chan" should "work" in {
    val channel = chan[Int]("test")(dfcontext)
    val check = channel.read { n => n }
    channel.writeAll(12) 
    assert(check() == 12)
  }
  
  "Mapping a function on a channel" should "return a channel where we can read mapped values" in {
    val channel = chan[Int]("test")(dfcontext)
    val mapped = Chan.map(channel) { n => n * 100 }
    channel.writeAll((1 to 10).toList) 
    assert((mapped.read { n => n }).apply == 100)
    assert((mapped.read { n => n }).apply == 200)
    assert((mapped.read { n => n }).apply == 300)
    assert((mapped.read { n => n }).apply == 400)
    assert((mapped.read { n => n }).apply == 500)
    assert((mapped.read { n => n }).apply == 600)
    assert((mapped.read { n => n }).apply == 700)
    assert((mapped.read { n => n }).apply == 800)
    assert((mapped.read { n => n }).apply == 900)
    assert((mapped.read { n => n }).apply == 1000)
  }
  
  "Filtering on a channel" should "return a channel where we can read filtered values" in {
    val channel = chan[Int]("test")(dfcontext)
    val filtered = Chan.filter(channel) { n => n % 2 == 0 }
    channel.writeAll((1 to 10).toList) 
    assert((filtered.read { n => n }).apply == 2)
    assert((filtered.read { n => n }).apply == 4)
    assert((filtered.read { n => n }).apply == 6)
    assert((filtered.read { n => n }).apply == 8)
    assert((filtered.read { n => n }).apply == 10)
  }
  
  "Folding a channel" should "return a channel where we can read a stream of folded values" in {
    val channel = chan[Int]("test")(dfcontext)
    val folded = Chan.foldLeft(channel)("0") { (acc: String,elt: Int) => acc + elt }
    channel.writeAll((1 to 4).toList) 
    assert((folded.read { n => n }).apply == "0")
    assert((folded.read { n => n }).apply == "01")
    assert((folded.read { n => n }).apply == "012")
    assert((folded.read { n => n }).apply == "0123")
    assert((folded.read { n => n }).apply == "01234")
  }
  
  "Reducing a channel" should "return a channel where we can read a stream of reduced values" in {
    val channel = chan[Int]("test")(dfcontext)
    val reduced = Chan.reduceLeft(channel) { _ + _ }
    channel.writeAll((1 to 4).toList) 
    assert((reduced.read { n => n }).apply == 1) // 1
    assert((reduced.read { n => n }).apply == 3) // 2
    assert((reduced.read { n => n }).apply == 6) // 3
    assert((reduced.read { n => n }).apply == 10) // 4
  }
  
  "Taking n values from a channel" should "return a channel where we can read only the n values" in {
    val channel = chan[Int]("test")(dfcontext)
    val taken = Chan.take(channel)(2)
    channel.writeAll((1 to 4).toList) 
    assert((taken.read { n => n }).apply == 1)
    assert((taken.read { n => n }).apply == 2)
    //assert((taken.read { n => n }).apply == 3) // this calls blocks - which is expected
  }
  
  "Dropping n values from a channel" should "return a channel where we can read only values after the nth one" in {
    val channel = chan[Int]("test")(dfcontext)
    val taken = Chan.drop(channel)(2)
    channel.writeAll((1 to 5).toList) 
    assert((taken.read { n => n }).apply == 3)
    assert((taken.read { n => n }).apply == 4)
    assert((taken.read { n => n }).apply == 5)
  }
  
  "Taking values while a condition is true from a channel" should "return a channel with the nth first values validating the condition" in {
    val channel = chan[Int]("test")(dfcontext)
    val taken = Chan.takeWhile(channel)(_ < 4)
    channel.writeAll((1 to 10).toList) 
    assert((taken.read { n => n }).apply == 1)
    assert((taken.read { n => n }).apply == 2)
    assert((taken.read { n => n }).apply == 3)
//    assert((taken.read { n => n }).apply == 4) // this calls blocks - which is expected
  }
  
  "Dropping values while a condition is true from a channel" should "return a channel with the nth first values validating the condition dropped" in {
    val channel = chan[Int]("test")(dfcontext)
    val taken = Chan.dropWhile(channel)(_ < 4)
    channel.writeAll((1 to 10).toList) 
    assert((taken.read { n => n }).apply == 4)
    assert((taken.read { n => n }).apply == 5)
    assert((taken.read { n => n }).apply == 6)
    assert((taken.read { n => n }).apply == 7)
    assert((taken.read { n => n }).apply == 8)
    assert((taken.read { n => n }).apply == 9)
    assert((taken.read { n => n }).apply == 10)
  }
  
  "Merging 2 channels" should "return a channel where we can read values from both channels" in {
    val c1 = chan[Int]("test-A")(dfcontext)
    val c2 = chan[Int]("test-B")(dfcontext)
    val merged = Chan.merge(c1,c2)
    val counted = Chan.count(merged)
    
    val N = 1000
    c1.writeAll((1 to N).toList)
    c2.writeAll((1 to N).toList)
    
    val latch = new CountDownLatch(1)
    def consume(): Unit = counted.read { n => if(n < 2* N) consume() else latch.countDown }
    consume()
    latch.await()
    
    assert(true)
  }
  
  "Zipping 2 channels" should "return a channel with pairs of element" in {
    val c1 = chan[Int]("test-A")(dfcontext)
    val c2 = chan[Int]("test-B")(dfcontext)
    val zipped = Chan.zip(c1,c2)
    val N = 5
    c1.writeAll((1 to N).toList) // will try to write 5
    c2.writeAll((1 to N - 2).toList) // will write only 3
    
    assert((zipped.read { n => n }).apply == (1,1))
    assert((zipped.read { n => n }).apply == (2,2))
    assert((zipped.read { n => n }).apply == (3,3))
    //assert((zipped.read { n => n }).apply == (4,4)) // this calls blocks - which is expected
    c2.write(4) { _ => }
    c2.write(5) { _ => }
    assert((zipped.read { n => n }).apply == (4,4))
    assert((zipped.read { n => n }).apply == (5,5))
  }

}