package org.cc.dataflow.datastructure

import org.scalatest.FlatSpec

import org.cc.dataflow.Dataflow
import org.cc.dataflow.DataflowExtension
import org.cc.dataflow.DependencyGraph


class DFStreamTest extends FlatSpec {

  import DFStream._
  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.DataflowExtension._
  
  implicit val dfcontext = DependencyGraph.newContext()
  
  "Creating an empty stream and applying basic functions on it" should "return without blocking" in {
    val stream = empty[Int]()
    assert(isEmpty(stream))
    assert(size(stream)==0)
  }
  
  "Appending to an empty stream" should "create a longer stream" in {
    val stream = empty[Int]()
    assert(isEmpty(stream))
    assert(size(stream)==0)
    // Appending 1
    append(stream,1)
    assert(!isEmpty(stream))
    assert(size(stream)==1)
    // Appending 2
    append(stream,2)
    assert(!isEmpty(stream))
    assert(size(stream)==2)
    for(i <- 3 until 100) {        
	  append(stream,i)
	  assert(!isEmpty(stream))
	  assert(size(stream)==i)
    }
  }

  "An initialized stream" should "contains be appendable with new elements" in {
    val s = stream[Int]((1 to 50):_*)
    assert(!isEmpty(s))
    assert(size(s)==50)
    // Appending new elements
    append(s,51)
    assert(size(s)==51)
    append(s,52)
    assert(size(s)==52)
  }
  
  // Map
  
  "Mapping over an empty stream" should "produce an unbounded stream of mappings" in {
    val s = empty[Int]()
    assert(size(s)==0)
    // Creating a mapped stream from s
    val ms = DFStream.map(s) { _ + 100 }
    Thread.sleep(100)
    assert(size(s)==size(ms))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,1)
    Thread.sleep(100)
    assert(size(s)==1)
    assert(size(s)==size(ms))
    assert(head(ms)==101)
  }
  
  "Mapping over a stream" should "produce an unbounded stream of mappings" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms = DFStream.map(s) { _ + 100 }
    Thread.sleep(100)
    assert(size(s)==size(ms))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(s)==size(ms))
  }
  
  
  "Mapping N times over a stream" should "produce N unbounded streams of mappings" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms1 = DFStream.map(s) { _ + 100 }
    Thread.sleep(100)
    assert(size(s)==size(ms1))
    // Creating a mapped stream from s
    val ms2 = DFStream.map(s) { _ + 1000 }
    Thread.sleep(100)
    assert(size(s)==size(ms2))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(s)==size(ms1))
    assert(size(s)==size(ms2))
  }

  
  // Filter()
  
  "Filtering over an empty stream" should "produce an unbounded stream of matching elements" in {
    val s = empty[Int]()
    assert(size(s)==0)
    // Creating a mapped stream from s
    val ms = DFStream.filter(s) { _ % 2 == 0 }
    Thread.sleep(100)
    assert(size(s)== size(ms))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,1)
    Thread.sleep(100)
    assert(size(s)==1)
    assert(size(ms)==0)
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,2)
    Thread.sleep(100)
    assert(size(s)==2)
    assert(size(ms)==1)
    assert(head(ms)==2)
  }
  
  "Filtering over a stream" should "produce an unbounded stream of matching elements" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms = DFStream.filter(s) { _ % 2 == 0 }
    Thread.sleep(100)
    assert(size(s) / 2 == size(ms))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(s) / 2 == size(ms))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,52)
    Thread.sleep(100)
    assert(size(s)==52)
    assert(size(s) / 2 == size(ms))
  }
  
  "Filtering N times over a stream" should "produce N unbounded streams of matching elements" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms1 = DFStream.filter(s) { _ % 2 == 0 }
    Thread.sleep(100)
    assert(size(s) / 2 == size(ms1))
    val ms2 = DFStream.filter(s) { _ % 2 != 0 }
    Thread.sleep(100)
    assert(size(s) / 2 == size(ms2))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(s) / 2 == size(ms1))
    assert(size(s) / 2 + 1 == size(ms2))
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,52)
    Thread.sleep(100)
    assert(size(s)==52)
    assert(size(s) / 2 == size(ms1))
    assert(size(s) / 2 == size(ms2))
  }
  
  // Fold() / Reduce()
  
  "Folding a stream" should "produce an unbounded stream of folded values" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms1 = DFStream.foldLeft(s)(0) { (acc,elt) => acc + elt }
    Thread.sleep(100)
    assert(size(ms1)==1)
    assert(head(ms1)==(1 to 50).sum)
    val ms2 = DFStream.foldLeft(s)(0) { (acc,elt) => acc * elt }
    Thread.sleep(100)
    assert(size(ms2)==1)
    assert(head(ms2)==(1 to 50).product)
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(ms1) == 2)
    assert(last(ms1)==(1 to 51).sum)
    assert(size(ms2) == 2)
    assert(last(ms2)==(1 to 51).product)
  }
  
  "Reducing a stream" should "produce an unbounded stream of folded values" in {
    val s = stream[Int]((1 to 50):_*)
    assert(size(s)==50)
    // Creating a mapped stream from s
    val ms1 = DFStream.reduceLeft(s) { (acc,elt) => acc + elt }
    Thread.sleep(100)
    assert(size(ms1)==1)
    assert(head(ms1)==(1 to 50).sum)
    val ms2 = DFStream.reduceLeft(s) { (acc,elt) => acc * elt }
    Thread.sleep(100)
    assert(size(ms2)==1)
    assert(head(ms2)==(1 to 50).product)
    // Appending an element x to the main stream should be appended to the map stream as f(x) 
    append(s,51)
    Thread.sleep(100)
    assert(size(s)==51)
    assert(size(ms1) == 2)
    assert(last(ms1)==(1 to 51).sum)
    assert(size(ms2) == 2)
    assert(last(ms2)==(1 to 51).product)
  }
  
  
  
  // Debugging print
  private def printlnS[A](s: DFStream[A]): Unit = s match {
    case Suspended(t) if !isAssigned(t) => print("_"); println()
    case Suspended(t) if isAssigned(t) => printlnS(t()) 
    case Cons(h,t) if !isAssigned(t) => print(s"${h}|_"); println()
    case Cons(h,t) => 
      print(h+"|")
      printlnS(t())
  }
  
}