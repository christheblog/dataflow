package org.cc.dataflow.channel

// CSP
object Chan {

  import org.cc.dataflow.Dataflow._
  import org.cc.dataflow.cell.Cell._
  
  type InOut[T] = (Var[T], Var[Unit],Var[Unit]) // (in, out, done)

  // Chan is thread-safe and any thread can write/read messages
  // Performances degrade quickly with several writer/readers
  // There is no queuing for multiple writers / readers
  final class Chan[T](val name: String, private val cell : Cell[InOut[T]])
  			   	(implicit val ctx: DataflowContext) {
    
    newInOut()
    
    private def in: Var[T] = cell.get._1
    private def out: Var[Unit] = cell.get._2
    private def done: Var[Unit] = cell.get._3
    
    def write(t: T)(f: T => Unit): Unit = {
      val currdone = done // capturing current done
      thread(out) { 
        if(in <<?? t) thread(currdone) { f(t) }
        else write(t)(f) // someone else bound the value => we retry the write from scratch 
      } 
    }
    
    def writeAll(t: T*): Unit = writeAll(t.toList)
    
    def writeAll(all: scala.collection.immutable.Seq[T]): Unit = all match {
      case x +: xs => write(x) { _ => writeAll(xs) }
      case _ => 
    }
    
    def read[U](f: T => U): Var[U] = { 
      val (currin,currdone) = (in,done) // capturing current (in,done)
      if(out <<?? ()) thread(currin,currdone) { f(currin()) }
      else fthread() { read(f) }
    }
    
    private def newInOut(): Unit = thread(in,out) {
      val unlock = done
      transact(cell) { _ => (df[T]("in"), df[Unit]("out"), df[Unit]("done")) }
      newInOut() // scheduling next in/out
      unlock << () // freeing read action and write callback
    }
  }

  
  // Channel creation
  def chan[T](name: String = "unknown")(implicit ctx: DataflowContext): Chan[T] = 
    new Chan(name,new Cell((df[T],df[Unit],df[Unit])))
  
  
  
  // Channel manipulation
  
  def take[T](source: Chan[T])(n: Int): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-take-"+n)
    def consume(count: Int): Unit = if(count > 0) source.read { elt => res.write(elt) { _ => consume(count-1) } }
    consume(n)
    res
  }
  
  def drop[T](source: Chan[T])(n: Int): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-drop-"+n)
    def consume(count: Int): Unit = 
      if(count < n) source.read { elt => consume(count + 1) }
      else source.read { elt => res.write(elt) { _ => consume(count) } }
    consume(0)
    res
  }
  
  def count[T](source: Chan[T]): Chan[Long] = {
    implicit val ctx = source.ctx
    val res = chan[Long](s"${source.name}-count")
    def consume(n: Long): Unit = source.read { elt => res.write(n+1) { n => consume(n) } }
    consume(0L)
    res
  }
  
  def merge[T,A <: T, B <: T](a: Chan[A], b: Chan[B]): Chan[T] = {
    require(a.ctx == b.ctx)
    implicit val ctx = a.ctx
    val res = chan[T](s"merge(${a.name},${b.name})")
    def consumeA(): Unit = a.read { elt => res.write(elt) { _ => consumeA() } }
    def consumeB(): Unit = b.read { elt => res.write(elt) { _ => consumeB() } }
    consumeA(); consumeB()
    res
  }
  
  def zip[T,U](c1: Chan[T], c2: Chan[U]): Chan[(T,U)] = {
    require(c1.ctx == c2.ctx)
    implicit val ctx = c1.ctx
    val res = chan[(T,U)](s"(${c1.name}-${c2.name})")
    // Pair of variables to wait for both writing channels
    val cell = new Cell((df[T]("left"),df[U]("right")))
    def consume1(): Unit = c1.read { v => cell.get._1 << v }
    def consume2(): Unit = c2.read { v => cell.get._2 << v }
    def waitBoth(): Unit = { 
      val (l,r) = cell.get
      thread(l,r) { 
        val exchanged = cell := (df[T]("left"),df[U]("right"))
        val zipped = (l(),r())
        res.write(zipped) { _ => waitBoth(); consume1(); consume2() }
      }
    }
    // Consuming data and waiting for both side to be linked
    waitBoth()
    consume1(); consume2()
    res
  }

  
  // Higher-order functions

  def takeWhile[T](source: Chan[T])(p: T => Boolean): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-takeWhile")
    def consume(): Unit = source.read { elt => if(p(elt)) res.write(elt) { _ => consume() } }
    consume()
    res
  }
  
  def dropWhile[T](source: Chan[T])(p: T => Boolean): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-dropWhile")
    def consume(discard: Boolean): Unit = source.read { elt => 
      if(discard && p(elt)) consume(true)
      else res.write(elt) { _ => consume(false) } 
    }
    consume(true)
    res
  }
  
  def map[T,U](source: Chan[T])(f: T => U): Chan[U] = {
    implicit val ctx = source.ctx
    val res = chan[U](source.name+"-mapped")
    def consume(): Unit = source.read { elt => res.write(f(elt)) { _ => consume() } }
    consume()
    res
  }
  
  def filter[T](source: Chan[T])(p: T => Boolean): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-filtered")
    def consume(): Unit = source.read { elt => 
      if(p(elt)) res.write(elt) { _ => consume() } 
      else thread() { consume() } 
    }
    consume()
    res
  }

  // FIXME What does it means to flatten a channel ? shall we push anything coming from a Chan[U] in whatever order ?
  def flatMap[T,U](source: Chan[T])(f: T => Chan[U]): Chan[U] = ???
		  
  def foldLeft[T,U](source:Chan[T])(acc: U)(f: (U,T) => U): Chan[U] = {
    implicit val ctx = source.ctx
    val res = chan[U](source.name+"-folded")
    def consume(acc: U): Unit = source.read { elt => res.write(f(acc,elt)) { updatedAcc => consume(updatedAcc) } }
    res.write(acc) { _ => consume(acc) } // shall we post the first event
    res
  }
  
  def reduceLeft[T](source: Chan[T])(f: (T,T) => T)(implicit ctx: DataflowContext): Chan[T] = {
    implicit val ctx = source.ctx
    val res = chan[T](source.name+"-reduced")
    // We wait to read one element from the channel, and then we can start consuming the rest of the data
    source.read { elt => 
      def consume(acc: T): Unit = source.read { elt => res.write(f(acc,elt)) { uAcc => consume(uAcc) } }
      res.write(elt) { _ => consume(elt) }
    }
    res
  }
  

  // TODO : Writing has to synchronize with 2 readers with 1 writer
  def unzip[T,U](source: Chan[(T,U)]): (Chan[T],Chan[U]) = {
    implicit val ctx = source.ctx
    ???
  }
  
}
