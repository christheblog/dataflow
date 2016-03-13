package org.cc.dataflow

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicInteger

object Dataflow {
  
  trait DataflowContext {
    private[dataflow] def bind[T](v: Var[T], value: T)
    private[dataflow] def bind[T](v: Var[T], ex: Exception)
    private[dataflow] def compute[T](v: Var[T], using: List[Var[_]])(computation: => T)
    // Introduced for testing purpose only
    private[dataflow] def size(): Int 
  }
  
  sealed trait Var[T] {
    def id() = s"@${System.identityHashCode(this)}"
    def apply(): T // blocking call
    def <<(value: T): Var[T]
    def <<!(ex: Exception): Var[T]
    def !!() : Var[T] = new ReadOnlyVar(this)
    override def toString() = id()
    // Non deterministic capabilities, accessible only through extensions.
    private[dataflow] def isAssigned: Boolean
    private[dataflow] def <<??(value: T): Boolean // safe binding. Returns true if our call is the one binding the variable
    private[dataflow] def <<?(value: T): Var[T] =  { <<??(value); this }
  }
  
  
  // Primitive functions
  
  def df[T](implicit ctx: DataflowContext): Var[T] = new DFVar[T]()
  
  def df[T](iid: String)(implicit ctx: DataflowContext): Var[T] = 
    new DFVar[T]() { override def id() = iid }
  
  private[dataflow] val threadCounter = new AtomicInteger(0)
  def thread[T](dependencies: Var[_]*)(block: =>T)(implicit ctx: DataflowContext): Var[T] = {
    val res = df[T](s"thread-res-${threadCounter.getAndIncrement}")
    ctx.compute(res,dependencies.toList) {
      // Checking that dependencies are all bound
      dependencies.foreach { v => 
        if(!v.isAssigned) 
          throw new RuntimeException(s"${Thread.currentThread.getName} : Variable ${v.id()} should be bound to some value here ! Was computing ${res.id()}")
      }
      // Executing block
      block
    }
    res
  }
  
  // Flattening Var[Var[T]] to Var[T]
  def join[T](v: Var[Var[T]])(implicit ctx: DataflowContext): Var[T] = {
    val res = df[T](s"join-${v.id()}")
    thread(v) {
      val inner = v()
      thread(inner) { res << inner() } 
    }
    res
  }
  
  // Functions
  
  // join . thread
  def fthread[T](dependencies: Var[_]*)(block: =>Var[T])(implicit ctx: DataflowContext): Var[T]  = 
    join(thread(dependencies:_*)({ block }))

  // Blocking until both variables are bound
  def waitBoth[T,U](x: Var[T], y: Var[U])(implicit context: DataflowContext): (T,U) = {
    val res = thread(x, y) { (x(), y()) }
    res()
  }
  
  
  // We could provide map/flatMap directly in the Var[T] to allow for comprehension
  
  // map a function over the result of the var when it will be bound   
  def map[T,U](v: Var[T])(f: T => U)(implicit context: DataflowContext): Var[U] = 
    thread(v) { f(v()) }
  
  // Flatmap a function over the result of the var when it will be bound
  def flatMap[T,U](v: Var[T])(f: T => Var[U])(implicit context: DataflowContext): Var[U] = 
    fthread(v) { f(v()) }
  
  
  // Lifting
    
  def lift[A,B](f: A => B)(implicit ctx: DataflowContext): (Var[A] => Var[B]) = 
    (a: Var[A]) => thread(a) { f(a()) }
  
  def lift2[A,B,C](f: (A,B) => C)(implicit ctx: DataflowContext): (Var[A],Var[B]) => Var[C] = 
    (a: Var[A],b: Var[B]) => thread(a,b) { f(a(),b()) }
  
  def lift3[A,B,C,D](f: (A,B,C) => D)(implicit ctx: DataflowContext): (Var[A],Var[B],Var[C]) => Var[D] = 
    (a: Var[A],b: Var[B],c: Var[C]) => thread(a,b,c) { f(a(),b(),c()) }
    
    
    
  // Future/Promise dataflow variable implementation 
    
  import scala.concurrent.{Await, Future, Promise}
  import scala.concurrent.duration._
  import scala.util.{Try, Success,Failure}
    
  private class DFVar[T](promise: Promise[T] = Promise[T]())(implicit ctx: DataflowContext) extends Var[T] {
    
    def <<(value: T) = {
      promise.complete(Success(value))
      ctx.bind(this,value)
      this
    }
    
    def <<!(ex: Exception) =  {
      promise.complete(Failure(ex))
      ctx.bind(this,ex)
      this
    }
    
    def apply(): T = 
      Await.result(promise.future, Duration.Inf)
    
    // This may be needed - but should not be exposed
    protected[dataflow] def isAssigned = 
      promise.future.isCompleted
    
    // Assign a value if variable is not already bound. 
    // Returns true if this is our call binding the value
    protected[dataflow] def <<??(value: T) = 
      if(promise.tryComplete(Success(value))) {
        ctx.bind(this,value)
        true
      }
      else false
  }

  
  // Loosely proxy another variable - in a read-only fashion
  private class ReadOnlyVar[T](v: Var[T]) extends Var[T] {
    
    def <<(value: T) = 
      throw new RuntimeException(s"${Thread.currentThread.getName} : Cannot bind a value to a read-only variable")
    
    def <<!(ex: Exception) = 
      throw new RuntimeException(s"${Thread.currentThread.getName} : Cannot bind a value to a read-only variable")
    
    protected[dataflow] def <<??(value: T) = 
      throw new RuntimeException(s"${Thread.currentThread.getName} : Cannot bind a value to a read-only variable")
    
    override def !!() : Var[T] = this
    
    def apply(): T = v()
    
    protected[dataflow] def isAssigned = v.isAssigned
  }
   
  
}