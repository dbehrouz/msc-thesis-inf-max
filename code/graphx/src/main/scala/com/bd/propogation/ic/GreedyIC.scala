package com.bd.propogation.ic

import com.bd.util.EdgeListTransformer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import scala.util.control.Breaks._

import scala.reflect.io.File

/**
 * Single Cycle IC, each vertex tries to propagate based on edge weight
 * Assume constant edge weight of 0.01 for now
 * @author Behrouz Derakhshan
 */
object GreedyIC {
  // args : graph location (edgeFileList), output location
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Greedy Algorithm")
    val sc = new SparkContext(conf)

    val inputGraphFile = args(0)
    val seedSize = args(1).toInt
    println("Seed Size :" + seedSize)
    val iterations = args(2).toInt
    println("Iterations :" + iterations)
    val prob = args(3).toDouble
    println("Propagation Probability: " + prob)
    // read graph and pre process it
    val graph = EdgeListTransformer.transform(GraphLoader.edgeListFile(sc, inputGraphFile))
    val vertices = graph.vertices.collect.toList.map(l => (l._1, 0.0))

    var sorted = vertices.map {
      v => (v._1, Simulation.run(graph, List(v._1), iterations, prob))
    }

    var initialSeed = List[VertexId]()
    for (i <- 0 to seedSize) {
      println("Selecting seed : " + i)
      var bestVertex = (-1L, 0.0)
      sorted = sorted.sortBy(_._2)
      breakable {
        for (j <- 0 to sorted.length) {
          val activeNodes = sorted(j)._1 :: initialSeed
          val spread = Simulation.run(graph, activeNodes, iterations, prob)
          if (spread > bestVertex._2) {
            bestVertex = (sorted(j)._1, spread)
            if (j < sorted.length - 1 && spread > sorted(j + 1)._2) {
              break
            } else {
              sorted = sorted.updated(j, bestVertex)
            }
          }
        }
      }

      println("Best Vertex : " + bestVertex)
      initialSeed = bestVertex._1 :: initialSeed
      println("Initial Seed : " + initialSeed)
    }

    def output = if (args.length == 5) args(4) else "target/activenodes.txt"

    println("Final Initial Seed : " + initialSeed)

    File(output).writeAll(initialSeed.toString)


  }

}
