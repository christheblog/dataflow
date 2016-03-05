package org.cc.dataflow.cell

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

// Non-blocking mutable store
// work with CAS and busy-spin loop in case of a failed transaction 
object Cell {
	
  final class Cell[T](private[cell] val ref: AtomicReference[T]) {
    def this(value: T) = this(new AtomicReference(value))
    def get : T = ref.get
    def :=(value: T) = ref.set(value)
  }
  
  
  def apply[T](value: T): Cell[T] = new Cell(value)
  
  // Compare and swap exchange. False if exchange failed 
  def exchange[T](cell: Cell[T], expected: T, update: T): Boolean = {
    cell.ref.compareAndSet(expected, update) 
  }

  // Mutate the content of the cell by a transaction, applying a function f(old) = new
  // Transaction fails (and retry) if the content of the cell has been modified 
  // before the function returns and exchange the cell value
  def transact[T](cell: Cell[T], retry: Int = -1)(f: T => T): Boolean = {
    @tailrec
    def loop(count: Int): Boolean = {
      if (count == 0) false
      else {
        val old = cell.get
        if (!exchange(cell, old, f(old)))
          // Shall we add a Thread.`yield`
          loop(if(count > 0) count - 1 else count)
        else true
      }
    }
    loop(retry)
  }
  
}