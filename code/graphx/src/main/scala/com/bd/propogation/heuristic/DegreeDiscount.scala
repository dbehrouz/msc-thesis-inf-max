package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object DegreeDiscount extends SeedFinder {

  class VertexType(dc: Long, ddc: Long, tc: Long, seed: Boolean) {
    var d: Long = dc
    var dd: Long = ddc
    var t: Long = tc
    var isSeed: Boolean = seed
  }

  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    // vertex (d, dd, t, isSeed)
    val newVertices = graph.vertices.leftJoin(graph.outDegrees.mapValues((vid, d) => (d, d.toDouble, 0, false))) {
      case (vid, val1, None) => (0, 0.0, 0, false)
      case (vid, val1, val2) => val2.get
    }
    var degreeGraph = Graph(newVertices, graph.edges)
    var seed = List[VertexId]()
    for (i <- 1 to seedSize) {
      val chosen = degreeGraph.vertices
        .filter(!_._2._4) // filter nodes that are not selected already
        .top(5)(Ordering.by(_._2._2))
        .map(_._1).toList(1)
      seed = chosen :: seed
      val neighbors = sc.broadcast(degreeGraph.edges.filter {
        e => e.srcId == chosen
      }.map {
        _.srcId
      }.collect.toList)

      degreeGraph = degreeGraph.mapVertices {
        (vid, attr) => {
          if (vid == chosen) {
            (attr._1, attr._2, attr._3, true)
          }
          else if (neighbors.value.contains(vid)) {
            val d = attr._1
            val t = attr._3 + 1
            val dd = d - 2 * t - (d - t) * t * 0.01
            (d, dd, t, attr._4)
          } else {
            attr
          }
        }
      }
      println("iteration " + i + " ended")
    }
    sc.parallelize(seed)
  }
}


