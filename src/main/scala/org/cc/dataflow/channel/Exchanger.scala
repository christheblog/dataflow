package org.cc.dataflow.channel

object Exchanger {

  import org.cc.dataflow.cell.Cell._
  import org.cc.dataflow.Dataflow._
  
  trait Exchanger[T,U] {
    def putLeft(value: T)(receive: U => Unit): Unit
    def putRight(value: U)(receive: T => Unit): Unit
  }
  
  type LeftRight[T,U] = (Var[T], Var[U])
  
  // Each putLeft/Right must be called exactly one time
  def oneTimeExchanger[T,U](implicit ctx: DataflowContext): Exchanger[T,U] = {
    val (left,right) = (df[T],df[U])
    new Exchanger[T,U] {
      def putLeft(value: T)(receive: U => Unit) = {
        thread(left,right) { receive(right()) }
        left << value
      }
      def putRight(value: U)(receive: T => Unit) = {
        thread(left,right) { receive(left()) }
        right << value
      }
    }
  }
  
  
}