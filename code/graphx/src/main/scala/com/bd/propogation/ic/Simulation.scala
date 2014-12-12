package com.bd.propogation.ic

import com.bd.util.EdgeListTransformer
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
  val ACTIVE: Int = 1
  val NOT_ACTIVE: Int = 0
  val TRIED: Int = 2
  val SKIP: Int = -1

  def run(graph: Graph[Int, Int],
          activeNodes: List[VertexId],
          iterations: Int,
          prob: Double): Double = {

    val icGraph = graph.mapVertices {
      (id, attr) =>
        if (activeNodes.contains(id))
          ACTIVE
        else
          NOT_ACTIVE
    }.cache()

    def vertexProgram(id: VertexId, attr: Int, msg: Int): Int = {
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

    def sendMessage(edge: EdgeTriplet[Int, Int]) = {
      if (edge.srcAttr == ACTIVE) {
        if (math.random < prob) {
          Iterator((edge.dstId, ACTIVE))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Int, b: Int) = ACTIVE

    val initialMessage = SKIP

    var iter = 0
    var sum = 0


    while (iter < iterations) {
      sum += Pregel(icGraph, initialMessage, activeDirection = EdgeDirection.Out)(
        vertexProgram, sendMessage, messageCombiner)
        .vertices.filter(vd => vd._2 == ACTIVE || vd._2 == TRIED)
        .collect.length
      iter += 1
      if (iter % 50 == 0) {
        println("iter : " + iter)
      }
    }

    (sum.toDouble / iterations.toDouble)
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
      .collect
      .toList


    println("Number of Initial Active Nodes " + activeNodes.length)
    val graph = EdgeListTransformer
      .transform(GraphLoader.edgeListFile(sc, inputGraphFile))

    val spread = run(graph, activeNodes, iterations, prob)
    println("Total Spread: " + spread)

    var totalTime = System.currentTimeMillis - start
    println("Total Time : " + totalTime.toDouble / 1000)


  }

}
