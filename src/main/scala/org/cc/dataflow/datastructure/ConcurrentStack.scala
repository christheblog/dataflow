package org.cc.dataflow.datastructure

import org.cc.dataflow.cell.Cell

object ConcurrentStack {
	
  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.cell.Cell._
  
  sealed trait Element[T]
  private[ConcurrentStack] case class Empty[T]() extends Element[T]
  private[ConcurrentStack] case class Cons[T](val current: Var[T],val previous: Var[Element[T]]) extends Element[T]
  
  final class Stack[T](private[ConcurrentStack] val cell: Cell[Element[T]] = Cell[Element[T]](Empty[T]())) {
    private[ConcurrentStack] def top = cell.get
  }

  def apply[T] = new Stack[T]()
  
  def push[T](stack: Stack[T], elt: T)(implicit ctx: DataflowContext): Unit = {
    val current = stack.top
    if(!exchange(stack.cell,current,Cons(df << elt,df << current))) 
      push(stack, elt)
  }

  def pop[T](stack: Stack[T]): T = 
    popOption(stack).getOrElse(throw new RuntimeException("Cannot pop() from an empty stack"))
  
  def popOption[T](stack: Stack[T]): Option[T] = {
    val current = stack.top
    current match {
      case Empty() => None
      case Cons(curr, prev) =>
        if (!exchange(stack.cell, current, prev())) popOption(stack)
        else Some(curr())
    }
  }

  def peek[T](stack: Stack[T]): T = peekOption(stack).getOrElse(throw new RuntimeException("Cannot peek() from an empty stack"))
  def peekOption[T](stack: Stack[T]): Option[T] = {
    val current = stack.top
    current match {
      case Empty() => None
      case Cons(curr, prev) => Some(curr())
    }
  }
    
  def isEmpty[T](stack: Stack[T]) = stack.top match {
    case Empty() => true
    case _ => false
  }
  
}