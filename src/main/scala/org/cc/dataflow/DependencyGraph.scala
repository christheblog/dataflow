package org.cc.dataflow

import scala.concurrent.ExecutionContext
import scala.collection.concurrent.TrieMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean


object DependencyGraph {

  import Dataflow._

  // That's here we are building a DataflowContext - based on a non-blocking dependency graph 
  def newContext(cores: Int = Runtime.getRuntime().availableProcessors()): DataflowContext = new DataflowContext {
    private val grid = new ComputationExecutor(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(cores,
      new ThreadFactory() {
        val counter = new AtomicInteger(0)
        def newThread(r: Runnable) = { new Thread(r, s"dataflow-computation-${counter.getAndIncrement}") }
      })))
    private val graph = new SingleThreadedGraph(ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(
      new ThreadFactory() {
        val counter = new AtomicInteger(0)
        def newThread(r: Runnable) = { new Thread(r, s"dataflow-graph-${counter.getAndIncrement}") }
      })), grid)

    def size = graph.size
    def bind[T](v: Var[T], value: T) = graph.bound(v)
    def bind[T](v: Var[T], ex: Exception) = graph.bound(v)
    def compute[T](v: Var[T], using: List[Var[_]])(computation: => T) =
      graph.declare(v)(using)(computation)
  }

  private trait DependencyGraph {
    def size: Int
    def bound[T](v: Var[T]): Unit
    def declare[T](v: Var[T])(depencies: List[Var[_]])(computation: => T): Unit
  }

  // Non blocking graph calls, triggering multi-threaded execution of computations
  // All calls are enqueue in a single thread execution context to enforce thread safety
  // FIXME Change the implementation to use an actor
  private final class SingleThreadedGraph(private val context: ExecutionContext,
                                          private val grid: ComputationExecutor) extends DependencyGraph {

    // Graph nodes
    private class Node(val variable: Var[_],
                       @volatile var parents: List[Node],
                       @volatile var children: List[Node],
                       val assignedRef: AtomicBoolean = new AtomicBoolean(false)) {
      def isAssigned = assignedRef.get && variable.isAssigned
    }

    private class CalculationNode[T](v: Var[T], p: List[Node], c: List[Node], a: AtomicBoolean,
                                     val calculation: () => T) extends Node(v, p, c, a) {
      private val computingRef = new AtomicBoolean(false)
      private val computedRef = new AtomicBoolean(false)
      def computed = computedRef.get && computingRef.get
      def compute(): Unit = if (!computingRef.get && computingRef.compareAndSet(false, true)) {
        val result = calculation()
        enqueue {
          v << result
          assignedRef.set(true)
          computedRef.set(true)
        }
      }
    }

    private val nodes = new scala.collection.concurrent.TrieMap[Var[_], Node]()

    // Gets the number of variables in that context
    // When variables are bound and computed, they should be removed from the graph
    def size() = nodes.size

    def bound[T](v: Var[T]): Unit = enqueue {
      //nodes.putIfAbsent(v, new Node(v,Nil,Nil))
      if (nodes.contains(v)) {
        val node = nodes(v)
        if (node.assignedRef.compareAndSet(false, true))
          node.children.foreach(checkNode)
      }
    }

    // Declare a computation with its dependencies. Result will be bound in the provided variable
    def declare[T](res: Var[T])(dependencies: List[Var[_]])(computation: => T): Unit = enqueue {
      val compNode = new CalculationNode(res, Nil, Nil, new AtomicBoolean(false), () => { computation })
      dependencies.foreach { v => nodes.putIfAbsent(v, new Node(v, Nil, List(compNode), new AtomicBoolean(v.isAssigned))).map { n => n.children = compNode :: n.children } }
      compNode.parents = dependencies.map { nodes(_) }
      nodes.put(res, compNode)
      checkNode(compNode)
    }

    private def checkNodeQ(n: Node): Unit = enqueue { checkNode(n) }
    private def checkNode(n: Node): Unit = {
      n match {
        case cn: CalculationNode[_] =>
          // We enter one of the grid computation threads ... 
          // No read/change to the graph nodes except vals
          if (!cn.computed && cn.parents.forall(_.isAssigned)) {
            grid.compute {
              cn.compute()
              enqueue {
                if (cn.computed) {
                  cn.children.foreach(checkNode);
                  cn.parents.foreach(cleanNode)
                  cleanNode(cn)
                }
              }
            }
          }
        case n: Node if !n.isAssigned && n.parents.forall(_.isAssigned) =>
          if (n.assignedRef.compareAndSet(false, true)) {
            n.children.foreach(checkNode)
            n.parents.foreach(cleanNode)
            cleanNode(n)
          }
        case _ =>
      }
    }

    // We can clean node if all his children are assigned/computed
    // In this case we can remove the children/parents links
    // and clean the nodes map from the (variable -> node) association
    //
    // Note : Cleaning node ensures that we don't have retain dataflow variables and node references 
    // if they are not needed for further calculations
    private def cleanNodeQ(node: Node) = enqueue { cleanNode(node) }
    private def cleanNode(node: Node) = {
      if (node.isAssigned && node.children.forall(_.isAssigned)) {
        //println(s"Cleaning node for variable ${node.variable.id} : ${size()}")
        // Cleaning the node from its parents' children list 
        node.parents.foreach { p => p.children = p.children.filter(_ != node) }
        // Cleaning the node from its children's parents list
        node.children.foreach { c => c.parents = c.parents.filter(_ != node) }
        // Cleaning the (variable -> node)
        nodes.remove(node.variable).foreach { v => s"Removed var ${node.variable.id()}" }
      }
    }

    // Any operation on the graph must be done with the graph queue
    private def enqueue(block: => Unit) =
      context.execute(new Runnable() { def run() { block } })
  }

  // Computation executor
  private class ComputationExecutor(private val context: ExecutionContext) {
    def compute(block: => Unit) =
      context.execute(new Runnable() { def run() = block })
  }

}