package org.cc.dataflow

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

import org.scalatest.FlatSpec


class DataflowTest extends FlatSpec {

  import Dataflow._
  
  implicit val dfcontext = DependencyGraph.newContext()
  
  "Simple x = a + b" should "work if we bind a and b after x" in {
    val a = df[Int]
    val b = df[Int]
    val x = thread(a,b) { a() + b() }
    thread() { Thread.sleep(250); a << 2 }
    thread() { Thread.sleep(250); b << 3 }
    assert(x()==5)
  }
  
  "Simple x = a + b" should "work if we bind a and b before x" in {
    val a = df[Int]
    val b = df[Int]
    val x = thread(a,b) { a() + b() }
    val latch = new CountDownLatch(2)
    thread() { a << 2; latch.countDown }
    thread() { b << 3; latch.countDown }
    latch.await
    assert(x()==5)
  }
  
  "Simple x = a + b" should "work if we bind a before and b after x" in {
    val a = df[Int]
    val b = df[Int]
    val x = thread(a,b) { a() + b() }
    val latch = new CountDownLatch(1)
    thread() { a << 2; latch.countDown }; latch.await
    thread() { b << 3 }
    assert(x()==5)
  }
  
  "Binding a variable a second time" should "throw an exception" in {
    val a = df[Int]
    val b = df[Int]
    val x = thread(a,b) { a() + b() }
    thread() { a << 2 }
    thread() { b << 3 }
    assert(x()==5)
    
    try { x << 7 } 
    catch { 
      case e: RuntimeException => assert(true) 
      case _: Throwable => assert(false)
    }
    // if exception has been throw as expected, we should still have the hold value
    assert(x()==5)
  }
  
  "Binding variables using thread syntax" should "produce the right result" in {
    val a = thread() { 2 }
    val b = thread() { 3 }
    val x = thread(a,b) { a() + b() }
    assert(x()==5)
  }
  

  "A function call" should "wait for a dataflow variable to be bound before returning" in {
    val x = df[Int]
    def fun(a: Int) = x() + a
    thread() { Thread.sleep(100); x << 1 }
    val res = fun(12)
    assert(res==13)
    
  }
  
  "A function call" should "be able to perform a computation in a dataflow variable" in {
    val x = df[Int]
    def fun(a: Int) = thread(x) { x() + a }
    thread() { Thread.sleep(100); x << 1 }
    val res = fun(12)
    assert(res()==13)
  }
  
  "Thread calls" should "be embeddable" in {
    val x = df[Int]
    val y = df[Int]
    val res = thread(x) {
      val z = df[Int]
      thread(y) { z << y() }
      y << x() + 3
      z()
    }
    x << 2
    assert(res()==5)
  }
  
  "Thread calls" should "be embeddable - 2" in {
    val res = df[Int]
    val x = df[Int]
    thread(x) {
      val y = df[Int]
      thread(y) {
        val z = df[Int]
        thread(z) { res << 10 }
        z << y()
      }
      y << x()
    }
    x << 2
    assert(res()==10)
  }
  

  "Join(Var[Var[_]])" should "not block caller" in {
    val v = df[Var[Int]]
    val res = join(v)
    val inner = df[Int]
    v << inner
    inner << 10
    assert(res()==10)
  }
  
  "Join(thread() { ... })" should "not block caller - 1" in {
    val v = df[Var[Int]]
    val res = join(thread(v) { v() })
    delay(250) { v << (df << 10) }
    assert(!v.isAssigned)
    assert(res()==10)
    // expected execution of v << 10
  }
  
  "Join(thread() { ... })" should "not block caller - 2" in {
    val v = df << (df[Int] << 10); v()
    val res = join(thread(v) { v() })
    assert(res()==10)
  }
  
  "fthread() { ... })" should "not block caller" in {
     val inner = df[Int]
     val v = df[Var[Int]] 
	 val res = fthread(v) { delay(100) { inner << 10 }; v() }
     delay(100) { v << inner }
	 assert(res()==10)
  }
  
  "Recursive threaded function call" should "be possible" in {
    def f(x: Int): Var[Int] = x match {
      case 0 => df << 0
      case n => fthread() { f(n-1) }
    }
    val res = f(20000)
    assert(res()==0)
  }
  
  "Fibonacci function" should "produce the correct fibbonacci sequence without hanging" in {
    def fib(x: Int): Var[Int] = x match {
      case 0 => df << 1
      case 1 => df << 1
      case n =>
        val a = fthread() { fib(n-1) }
        val b = fthread() { fib(n-2) }
        thread(a,b) { a() + b() }
    }
    assert(fib(15)()==987)
  }
  
  
  
  
  
  
  
  // Utils
  
  private val delayExec = Executors.newSingleThreadExecutor
  private def delay(d: Int)(block: =>Unit) {
    delayExec.execute(new Runnable() { def run = { block }})
  }
  
}