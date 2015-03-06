package com.bd.propogation.ic

import com.bd.util.GraphUtil
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf, Logging}

/**
 * Given an input graph with an initial set of active nodes
 * Find the spread size
 * edge probabiliy is constant
 * Input :
 *
 * Graph with vertices marked as active and non active
 * Number of iterations for the Simulation
 *
 * Output :
 * Spread size
 * Graph with active nodes
 *
 * @author Behrouz Derakhshan
 */
object Simulation extends Logging {
  val ACTIVE: Long = 1
  val NOT_ACTIVE: Long = 0
  val TRIED: Long = 2
  val SKIP: Long = -1

  def run(graph: Graph[Long, Double],
          activeNodes: List[VertexId],
          iterations: Int): Double = {

    val icGraph = graph.mapVertices {
      (id, attr) =>
        if (activeNodes.contains(id))
          ACTIVE
        else
          NOT_ACTIVE
    }.cache()

    def vertexProgram(id: VertexId, attr: Long, msg: Long): Long = {
      if (msg == SKIP) {
        attr
      }
      else if (attr == NOT_ACTIVE) {
        msg
      }
      else if (attr == ACTIVE) {
        TRIED
      }
      else {
        attr
      }
    }

    def sendMessage(edge: EdgeTriplet[Long, Double]) = {
      if (edge.dstAttr == ACTIVE || edge.dstAttr == TRIED) {
        Iterator.empty
      } else if (edge.srcAttr == ACTIVE) {
        if (math.random <= edge.attr) {
          Iterator((edge.dstId, ACTIVE))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Long, b: Long) = ACTIVE

    val initialMessage = SKIP

    var iter = 0
    var sum = 0L

    while (iter < iterations) {
      sum += Pregel(icGraph, initialMessage,
        activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
        .vertices.filter(vd => vd._2 == ACTIVE || vd._2 == TRIED)
        .count
      iter += 1
      if (iter % 50 == 0) {
        println("iter : " + iter)
      }
    }

    sum.toDouble / iterations.toDouble
  }


  // get the graph
  // filter all active vertices
  // try to active along edges
  // change the attr of active vertices to some other constant
  // do until no vertices are "active" anymore
  // return number of vertices activated in the process
  def main(args: Array[String]) {
    val start = System.currentTimeMillis
    val conf = new SparkConf().setAppName("IC Simulation")
    val sc = new SparkContext(conf)
    val inputGraphFile = args(0)
    val activeNodesFile = args(1)
    val iterations = args(2).toInt
    println("Number of Simulations: " + iterations)
    val prob = args(3).toDouble
    println("Propagation Prob : " + prob)

    val activeNodes = sc.textFile(activeNodesFile)
      .flatMap(l => l.split(','))
      .map(l => l.toLong)
      .collect()
      .toList

    println("Number of Initial Active Nodes " + activeNodes.length)
    val graph = GraphUtil.undirected(GraphUtil.mapTypes(GraphLoader.edgeListFile(sc, inputGraphFile)), prob)

    val spread = run(graph, activeNodes, iterations)
    println("Total Spread: " + spread)

    var totalTime = System.currentTimeMillis - start
    println("Total Time : " + totalTime.toDouble / 1000)
  }

}
