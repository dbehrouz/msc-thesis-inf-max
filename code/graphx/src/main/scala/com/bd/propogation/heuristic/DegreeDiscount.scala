package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._

/**
 * @author Behrouz Derakhshan
 */
object DegreeDiscount extends SeedFinder {

  class VertexType(dc: Long, ddc: Long, tc: Long) {
    var d: Long = dc
    var dd: Long = ddc
    var t: Long = tc
  }

  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    // vertex (d, dd, t)
    val newVertices = graph.vertices.leftJoin(graph.outDegrees.mapValues((vid, d) => (d, d.toDouble, 0))) {
      case (vid, val1, None) => (0, 0.0, 0)
      case (vid, val1, val2) => val2.get
    }
    var degreeGraph = Graph(newVertices, graph.edges)
    var seed = List[VertexId]()
    for (i <- 1 to seedSize) {
      val vList = degreeGraph.vertices.top(seedSize)(Ordering.by(_._2._2)).map(_._1).toList
      var chosen: Long = 0L
      breakable {
        for (v <- vList) {
          if (!seed.contains(v)) {
            chosen = v
            break
          }
        }
      }
      seed = chosen :: seed
      val neighbors = sc.broadcast(degreeGraph.edges.filter {
        e => e.srcId == chosen
      }.map {
        _.srcId
      }.collect.toList)

      degreeGraph = degreeGraph.mapVertices {
        (vid, attr) => {
          if (neighbors.value.contains(vid)) {
            val d = attr._1
            val t = attr._3 + 1
            val dd = d - 2 * t - (d - t) * t * 0.01
            (d, dd, t)
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


