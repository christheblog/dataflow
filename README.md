# Dataflow Variables


## Description

This project intends to provide me a playground to experiment in Scala what can be done with dataflow variables, and experiment which kind of data structure we can build.
The current implementation is an on-going work, and there is no plan at the moment to make a library. I am just sharing it in case you are curious about the topic.

This work has been largely inspired by the excellent book "Concept, Techniques and Models of Computer Programming" from Peter VanRoy and Seif Haridi.
You can experiment the power of dataflow variables within the OZ language, but the lack of modern tools and IDE like eclipse or ItelliJ plugins make the learning curve a bit steep.

Dataflow variables can be written only once, and are blocking when trying to read them if they are unbound.


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

Dataflow computation is run in parallel, but is DETERMINISTIC as long as you have only a blocking read and a write operation. If a deadlock occurs, it will occur deterministically.

The DSL is not as light as one could desire. The thread() { ... } syntax needs to declare all the variables that will be read in the block. This was an easy way to provide light-weight threads. 
When writing the code thread(x,y) { x + y }, the block { x + y } is executed only when x and y have been bound, and never before. 
This guarantees that we will never block an OS thread and we can easily create thousands or millions of light-weight threads.

A thread returns an unbound dataflow variable that will be bound once the thread block has been executed.

you could write :
```scala
thread() { x << 1 }
thread() { y << 2 }
val z = thread(x,y) { x() + y() } 
z() // Blocks the current OS thread until z is bound.
```

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
if(isAssigned(x)) 
  x() + 1
else 
  0
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
   
## What's in the project ?

In this project you will also find :
- Cell : mutable store (based on an AtomicReference)

Based on the dataflow variable and the mutable store, you will also find :
- Ports implementation (Actor-like)
- Channel implementation (CSP-like)
- A few data structures like DFStream (List in which the last element is an unbounded dataflow variable)
- and everything I want to try here :)


## What's next ?

The current implementation of the Var[T] and the DependencyGraph has been done quickly to provide me a playground. 
Code for these is no easy to understand, and would definitely benefits from a re-engineering. 
The next version will probably be based on actors.


## If you are interested in dataflow variables ...

- Akka used to have a dataflow variable implementation based on continuations (I am not sure if this is still the case). There is also probably a lot of other implementations available.
- You can also have a look at Ozma (which I haven't tried myself) : (https://github.com/sjrd/ozma)
- You can also have a look at OZ itself (https://mozart.github.io/) 
- And of course buy the excellent book from the creators of Oz/Mozart (http://www.amazon.co.uk/Concepts-Techniques-Models-Computer-Programming/dp/0262220695/)
