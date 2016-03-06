# Dataflow Variables


## Description

This project intends to provide me a playground to experiment in Scala what can be done with dataflow variables, and experiment which kind of data structure we can build on the top of them.
The current implementation is an on-going work, and there is no plan at the moment to make it a proper library. 

I am just sharing this code in case you are curious about the topic.

This work has been largely inspired by the excellent book "Concept, Techniques and Models of Computer Programming" from Peter VanRoy and Seif Haridi.
You can experiment the power of dataflow variables within the OZ language, but it lacks modern tools and IDE.

Dataflow variables can be assigned only once, and are blocking when trying to read them if they are unbound.


## Example

First of all you need the following imports and implicit context variable :
 ```scala
import Dataflow._
implicit val ctx = DependencyGraph.newContext()
```

After you use dataflow variables and threads as follow : 
```scala
// Declaration
val x = df[Int]
val y = df[Int]

// Concurrent binding using light-weight threads
thread(x,y) { x() + y() } // compute x + y in a thread
thread() { x << 1 } // binds x to 1
thread() { y << 2 } // binds y to 2
```

A dataflow computation is run in parallel, but is DETERMINISTIC (provided you don't attempt to bind concurrently a variable several time). If a deadlock occurs, it will occur deterministically.

The DSL is not as light as one could desire. The `thread() { ... }` syntax needs to declare all the variables that will be read in the block. This was an easy way to provide light-weight threads. 
When writing the code `thread(x,y) { x + y }`, the block `{ x + y }` is executed only when `x` and `y` have been bound, and never before. 
This guarantees that we will never block an OS thread and we can easily create thousands or millions of light-weight threads.

A `thread()` call returns an unbound dataflow variable that will be bound with the result of the thread code block has been executed.

you could write :
```scala
thread() { x << 1 }
thread() { y << 2 }
val z = thread(x,y) { x() + y() } 
z() // Blocks the current OS thread until z is bound.
```

As a last example, we can consider the following function :   
```scala
def fib(x: Int): Var[Int] = x match {
  case 0 => df << 1
  case 1 => df << 1
  case n =>
    val a = fthread() { fib(n-1) }
    val b = fthread() { fib(n-2) }
    thread(a,b) { a() + b() }
}

val result = fib(15)
result() // blocks until result is computed
```

This function will create and "block" an exponential number of threads, until `f(0)` and `f(1)` are computed.

Note that fthread will flatten the result of the thread.
```scala
thread() { fib(n-1) }  // Var[Var[Int]]
fthread() { fib(n-1) } // Var[Int]
```


#### Remark

Ideally we would like a syntax similar to the OZ language syntax :
```scala 
thread x + y end
``` 
which could be translated in Scala as 
```scala
thread { x() + y() }
```
but this would requires macro/compiler's help to translate it into some :
```scala 
thread(x,y) { x() + y() }
```

## Extensions

The DataflowExtension object provides some way to look inside a dataflow variable. This allows to introduce NON-DETERMINISIM in the computation.

You need to add the following import :
```scala
import DataflowExtension._
```

You can for instance check if a variable is assigned :
```scala
val x = df[Int]
thread() { x << 100 }
val undetermined = if(isAssigned(x)) x() + 1 else  0
```

Or wait for one of 2 variables to be bound :
```scala
val x = df[Int]
val y = df[Int]
val z: Int = waitOneOf(x,y) // this call is blocking and returns an Int
```

One could use a variation that returns a dataflow variable instead :
```scala
val x = df[Int]
val y = df[Int]
// This call returns a dataflow variable that will be bound with the value of x or y
val z: Var[Int] = oneOf(x,y)
// so you can do something useful with z in a thread
thread(z) { ... z() ... }
```
   
## What else can we do ? 

### Streams 

A DFStream is a list in which the last element is an unbounded dataflow variable.

```scala
import DFStream._
val s = empty[Int]()
append(s,1)
append(s,2)
append(s,3)
```

Mapping/Filtering a stream returns a new stream which will be updated each time we append a new value to the original one :  
```scala
val s = empty[Int]()
val sPlus10 = DFStream.map(s,_ + 10) // empty
val evenNum = DFStream.filter(s,_ % 2 ==0) //empty
append(s,1)
append(s,2)
append(s,3)
// sPlus10 is now [11,12,13]
// evenNum is now [2]
```

### Cells
A cell is just mutable store that work like an AtomicReference. This is not built on the top of the dataflow variable, but is useful to build concurrent data structures.

### Ports (Actor-like)

A port works like an actor. The mailbox is just a stream. Sending a message to the port is equivalent to appending a new value to the stream. 
The receiver is just looping, attempting to read the unbound variable in a thread until it is becomes bound.    

A port is designed to work with multiple senders and one receiver. Receiving is a blocking operation as the receiver attempt to read an unbound variable. Sending to a port is asynchronous.

```scala
import Port._

val port = new Port[Int]()
receive(port) { i => println(s"Receive ${i}" }
// Messages will be processed in order
send(port,1)
send(port,2)
send(port,3)
```

### Channel implementation (CSP-like)

A channel supports several writers and readers. Writing to a channel "blocks" until a reader is ready to read the value. Reading from a channel is blocking until a writer has written a value.
In case of multiple writers and readers, order in which values are written and which readers will read the value is undetermined. 

```scala
import Chan._

val channel = chan[Int]("test")
// Producing 10 values to be written in the channel
def produce(n: Int): Unit = 
  if(n >= 0) channel.write(n) { written => produce(n - 1) }
produce(10)

// Nothing is written until we have a reader
val result: Var[Int] = channel.read { n => n } // Now one value is written and read through the channel.
channel.read { n => n } // Now a second value is written and subsequently read.
```

There is also some usual combinators available for the channels :
```scala
val channel = chan[Int]("test")

val mapped = Chan.map(channel) { n => n * 100 }
val filtered = Chan.filter(channel) { n => n % 2 == 0 }
val folded = Chan.foldLeft(channel)("0") { (acc: String,elt: Int) => acc + elt }
val reduced = Chan.reduceLeft(channel) { _ + _ }
// ...
```


## What's next ?

The current implementation of the Var[T] and the DependencyGraph has been written quickly to provide me the base I needed to experiment and play around with them in scala. 
Code for these is not easy to understand, and would definitely benefits from a re-engineering. The next version will probably be based on actors.


## If you are interested in dataflow variables ...

- Akka (https://github.com/akka/akka) used to have a dataflow variable implementation based on continuations (I am not sure if this is still the case).
- You can also have a look at Ozma (which I haven't tried myself) : (https://github.com/sjrd/ozma)
- You can also have a look at Oz itself (https://mozart.github.io/)
- And of course read the excellent book from the creators of Oz/Mozart (http://www.amazon.co.uk/Concepts-Techniques-Models-Computer-Programming/dp/0262220695/)
