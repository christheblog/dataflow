package org.cc.dataflow.port

import scala.annotation.tailrec

import org.cc.dataflow.Dataflow
import org.cc.dataflow.DataflowExtension
import org.cc.dataflow.cell.Cell
import org.cc.dataflow.datastructure.DFStream

object Port {

  import Dataflow._
  import DFStream._
  import Cell._
  
  // Port implementation
  final class Port[T]()(implicit val ctx: DataflowContext) {
    private[port] val next = Cell(end(empty[T]))
  }
  
  @tailrec
  def send[T](p: Port[T], value: T): Unit = {
    implicit val ctx = p.ctx
	val current = p.next.get
	val unbound = df[DFStream[T]] // new end
	if(exchange(p.next,current, unbound))
	  current << Cons(value,unbound)
	else
	  send(p,value)
  }
  
  // Receive function - each message is received in a new thread
  def receive[T](p: Port[T])(f: T => Unit): Unit = {
    implicit val ctx = p.ctx
    def tloop(x: Var[DFStream[T]]): Unit = 
      thread(x) { x() match {
        case Cons(h,t) => f(h); tloop(t)
        case _ =>
      }}
    tloop(p.next.get)
  }
  
  // Process a batch of messages in the same thread following messages are available (df variables are bound) 
  def batch[T](p: Port[T],bucket: Int = 100)(f: T => Unit): Unit = {
    import DataflowExtension._
    implicit val ctx = p.ctx
    def loop(x: Var[DFStream[T]], count: Int): Unit = if(count > 0 && isAssigned(x)) {
      x() match {
        case Cons(h,t) => f(h); loop(t, count - 1)
        case _ => 
      }
    }
    else tloop(x)
    // Loop in thread waiting on the variable
    def tloop(x: Var[DFStream[T]]): Unit = thread(x) { x() match {
      case Cons(h,t) => f(h); loop(t,bucket) 
      case _ =>
    }}

    loop(p.next.get,bucket)
  }
  
  // Process ALL available messages in one go - ie in the same thread
  // Once all available messages have been processed, returning to wait mode on the unbound variable
  def drain[T](p: Port[T])(f: T => Unit): Unit = {
    import DataflowExtension._
    implicit val ctx = p.ctx
    def loop(x: Var[DFStream[T]]): Unit = if(isAssigned(x)) {
      x() match {
        case Cons(h,t) => f(h); loop(t)
        case _ => 
      }
    }
    else tloop(x)
    
    def tloop(x: Var[DFStream[T]]): Unit = thread(x) { x() match {
      case Cons(h,t) => f(h); loop(t) 
      case _ =>
    }}

    loop(p.next.get)
  }
  
}