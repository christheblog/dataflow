package org.cc.dataflow

// Dataflow extension that allows non-deterministic programming 
object DataflowExtension {

  import Dataflow._
  
  // Checks is the variable has been bound already
  def isAssigned[T](v: Var[T]) = v.isAssigned
  
  // Returns a variable that will be bound as soon as one of the 2 variables is bound  
  def oneOf[T](x: Var[T], y: Var[T])(implicit context: DataflowContext): Var[T] = {
    val res = df[T](s"one-of(${x.id()},${y.id()})")
    thread(x) { res <<? x() }
    thread(y) { res <<? y() }
    res
  }
  
  // Wait for one of the variable to be bound
  def oneOf[T](vars: Var[T]*)(implicit context: DataflowContext): Var[T] = {
    val res = df[T](s"one-of(${vars.map(_.id).mkString(",")})")
    vars.foreach { x => thread(x) { res <<? x() } }
    res
  }
  
  // Returns a variable that will be bound as soon as one of the 2 variables is bound  
  def either[T,U](x: Var[T], y: Var[U])(implicit context: DataflowContext): Var[Either[T,U]] = {
    val res = df[Either[T,U]](s"either(${x.id()},${y.id()})")
    thread(x) { res <<? Left(x()) }
    thread(y) { res <<? Right(y()) }
    res
  }
  
  // Blocking until one of the variable is bound
  def waitOneOf[T](x: Var[T], y: Var[T])(implicit context: DataflowContext): T = 
    oneOf(x,y)(context)()
  
  // Blocking until one of the variable is bound
  def waitEither[T,U](x: Var[T], y: Var[U])(implicit context: DataflowContext): Either[T,U] = 
    either(x,y)(context)()

  // Selects the first Var to be assigned, and execute the corresponding code
  def select[T,U](oneOfThose: (Var[T], T => U) *)(implicit context: DataflowContext): U = {
    val map = oneOfThose.toMap[Var[T], T => U]
    val selected = oneOf(oneOfThose.map(_._1):_*)
    map(selected)(selected())
  }

}