package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

/**
 * Single Cycle IC, each vertex tries to propagate based on edge weight
 * Assume constant edge weight of 0.01 for now
 * @author Behrouz Derakhshan
 */
object GreedyIC extends SeedFinder {
  // args : graph location (edgeFileList), output location
  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    val vertices = graph.vertices.collect.toList.map(l => (l._1, 0.0))

    var sorted = vertices.map {
      v => (v._1, Simulation.run(graph, List(v._1), iterations))
    }

    var initialSeed = List[VertexId]()
    for (i <- 0 to seedSize) {
      println("Selecting seed : " + i)
      var bestVertex = (-1L, 0.0)
      sorted = sorted.sortBy(_._2)
      breakable {
        for (j <- 0 to sorted.length) {
          val activeNodes = sorted(j)._1 :: initialSeed
          val spread = Simulation.run(graph, activeNodes, iterations)
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
    sc.parallelize(initialSeed)
  }

}
