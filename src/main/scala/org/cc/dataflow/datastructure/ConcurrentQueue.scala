package org.cc.dataflow.datastructure

object ConcurrentQueue {
  
  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.cell.Cell._

  // Non-blocking concurrent queue
  final class Queue[T](private[ConcurrentQueue] val q: Cell[(Int,List[T],List[T])])
  
  def isEmpty[T](q: Queue[T]) = size(q)==0
  
  def size[T](q: Queue[T]) = q.q.get._1 
  
  def enqueue[T](q: Queue[T],e: T): Unit =  
    transact(q.q) { case (size,start,end) => (size+1,start,e::end) }

  def dequeue[T](q: Queue[T]): T = {
    val elt = new Cell[Option[T]](None)
    transact(q.q) {
      case (0, _,_) => 
        throw new RuntimeException("Empty queue")
      case (size, e :: start,end) => 
        elt := Some(e)
        (size-1,start,end)
      case (size, Nil ,end) =>
        val h :: t = end.reverse
        elt := Some(h)
        (size-1,t,Nil)
    }
    elt.get.get
  }

}