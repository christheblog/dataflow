package org.cc.dataflow.datastructure

object DFStream {

  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.DataflowExtension._

  // A stream is a list with the last element being an unbound dataflow variable
  // Appending to a stream is O(1) if you have an access to the last element
  // Last element is replaced with a new Cons and a new unbound variable
  
  sealed trait DFStream[A]
  final case class Suspended[A](val next: Var[DFStream[A]]) extends DFStream[A]
  final case class Cons[A](val head: A, val tail: Var[DFStream[A]]) extends DFStream[A]
  

  def empty[A]()(implicit context: DataflowContext): DFStream[A] = 
    Suspended(df[DFStream[A]])
  
  def stream[A](as: A*)(implicit context: DataflowContext): DFStream[A] =
    if (as.isEmpty) empty[A]()
    else as.foldLeft(empty[A]()) {
      case (acc, a) =>
        val (s, _) = append(acc, a)
        s // FIXME inefficient
    }

  // Stream manipulation

  def head[A](s: DFStream[A]): A = 
    headOption(s).getOrElse { throw new RuntimeException("Cannot get head of an empty stream") }
  
  def headOption[A](s: DFStream[A]): Option[A] = s match {
    case Suspended(t) if !isAssigned(t) => None
    case Suspended(t) => headOption(t()) 
    case Cons(h,t) => Some(h)
  }
  
  def headVar[A](s: DFStream[A])(implicit context: DataflowContext): Var[DFStream[A]] = s match {
    case Suspended(t) if !isAssigned(t) => t
    case Suspended(t) => headVar(t()) 
    case hd@Cons(h,t) => df << hd 
  }

  def tail[A](s: DFStream[A]): Var[DFStream[A]] = 
    tailOption(s).getOrElse { throw new RuntimeException("Cannot get head of an empty stream") }
  
  def tailOption[A](s: DFStream[A]): Option[Var[DFStream[A]]] = s match {
    case Suspended(t) if !isAssigned(t) => None
    case Suspended(t) => tailOption(t()) 
    case Cons(h,t) => Some(t)
  }

  def last[A](s: DFStream[A]): A = 
    lastOption(s).getOrElse { throw new RuntimeException("Cannot get last of an empty stream") }
  
  def lastOption[A](s: DFStream[A]): Option[A] = s match {
    case Suspended(t) if !isAssigned(t) => None
    case Suspended(t) => lastOption(t()) 
    case Cons(h,t) if isAssigned(t) => lastOption(t())
    case Cons(h,_) => Some(h)
  }


  // Get the unbound variable of a stream
  def end[A](s: DFStream[A]): Var[DFStream[A]] = s match {
    case Suspended(t) if !isAssigned(t) => t
    case Suspended(t) => end(t()) 
    case Cons(h,t) if isAssigned(t) => end(t())
    case Cons(h,t) => t
  }

  def isEmpty[A](s: DFStream[A]): Boolean = s match {
    case Suspended(t) if !isAssigned(t) => true
    case Suspended(t) if isAssigned(t) => isEmpty(t())
    case _ => false
  }

  def size[A](s: DFStream[A]): Int = s match {
    case Suspended(t) if !isAssigned(t) => 0
    case Suspended(t) => size(t()) 
    case Cons(h,t) if !isAssigned(t) => 1
    case Cons(h,t)  => 1 + size(t())
  }
  
  def append[A](s: DFStream[A], a: A)
  			   (implicit context: DataflowContext): (DFStream[A], Var[DFStream[A]]) = s match {
    case Suspended(t) if !isAssigned(t) =>
      val unbound = df[DFStream[A]]
      t << Cons(a, unbound)
      (s, unbound)
    case _ =>
      val unbound = end(s)
      val newUnbound = df[DFStream[A]]
      unbound << (Cons(a, newUnbound))
      (s, newUnbound)
  }
  
  // Appends stream s2 to s1
  def append[A](s1: DFStream[A], s2: DFStream[A])
  			   (implicit context: DataflowContext): (DFStream[A], Var[DFStream[A]]) = s1 match {
  	 case Suspended(t) if !isAssigned(t) => ((t << s2)() ,end(s2))
  	 case Suspended(t) if isAssigned(t) => append(t(),s2)
  	 case Cons(h, t) if !isAssigned(t) => ((t << s2)() ,end(s2))
     case Cons(h, t) if isAssigned(t) => append(t(),s2)
  }
    
  
  
  // Higher-order functions

  def map[A, B](s: DFStream[A])(f: A => B)
  			   (implicit context: DataflowContext): DFStream[B] = s match {
    case Suspended(t) if !isAssigned(t) => Suspended(thread(t) { map(t())(f) })
    case Suspended(t) if isAssigned(t) => map(t())(f)
    case Cons(h,t) => Cons(f(h), thread(t) { map(t())(f) })
  }
  
  def filter[A, B](s: DFStream[A])(p: A => Boolean)
  				  (implicit context: DataflowContext): DFStream[A] = s match {
    case Suspended(t) if !isAssigned(t) => Suspended(thread(t) { filter(t())(p) })
    case Suspended(t) if isAssigned(t) => filter(t())(p)
    case Cons(h,t) if p(h) => Cons(h, thread(t) { filter(t())(p) })
    // Filtering cases
    case Cons(h,t) if isAssigned(t) => filter(t())(p)
    case Cons(h,t) if !isAssigned(t) => Suspended(thread(t) { filter(t())(p) }) // we need to suspend here
  }

  def flatMap[A, B](s: DFStream[A])(f: A => DFStream[B])(implicit context: DataflowContext): DFStream[B] = s match {
    case Suspended(t) if !isAssigned(t) => Suspended(thread(t) { flatMap(t())(f) })
    case Suspended(t) if isAssigned(t) => flatMap(t())(f)
    case Cons(h, t) if !isAssigned(t) => append(f(h), Suspended(thread(t) { flatMap(t())(f) }))._1
    case Cons(h, t) if isAssigned(t) => append(f(h), flatMap(t())(f))._1
  }
  
  // Fold operation will provide a stream of fold
  // First value will be the fold of all of the available stream 
  def foldLeft[A,B](s: DFStream[A])(acc: B)(f: (B,A) => B)
  				   (implicit context: DataflowContext): DFStream[B] = s match {
    case Suspended(t) if !isAssigned(t) => Suspended(thread(t) { foldLeft(t())(acc)(f) })
    case Suspended(t) if isAssigned(t) => foldLeft(t())(acc)(f)
    case Cons(h,t) if isAssigned(t) => foldLeft(t())(f(acc,h))(f)
    // Last element reached
    case Cons(h,t) if !isAssigned(t) =>
      val newAcc = f(acc,h)
      Cons(newAcc, thread(t) { foldLeft(t())(newAcc)(f) })
  }
  
  def reduceLeft[A](s: DFStream[A])(f: (A,A) => A)
  				   (implicit context: DataflowContext): DFStream[A] = s match {
    case Suspended(t) if !isAssigned(t) => Suspended(thread(t) { reduceLeft(t())(f) })
    case Suspended(t) if isAssigned(t) => reduceLeft(t())(f)
    case Cons(h,t) if isAssigned(t) => foldLeft(t())(h)(f)
    case Cons(h,t) if !isAssigned(t) => Cons(h, thread(t) { foldLeft(t())(h)(f) })
  }
  
}