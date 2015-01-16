package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, Pregel, VertexId, Graph}
import org.apache.spark.rdd.RDD

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
    var degreeGraph = Graph(graph.outDegrees.mapValues((vid, d) => (d, 0, 0)), graph.edges)
    var seed = List[VertexId]()
    for (i <- 1 to seedSize) {
      val v = degreeGraph.vertices.top(1)(Ordering.by(_._2._1)).map(_._1).toList(0)
      seed = v :: seed
      Pregel(degreeGraph, initialMessage, activeDirection = EdgeDirection.Out)(
        vprog = (id, attr, msg) => math.min(attr, msg),
        sendMsg = sendMessage,
        mergeMsg = (a, b) => math.min(a, b))
    }
  }
}
