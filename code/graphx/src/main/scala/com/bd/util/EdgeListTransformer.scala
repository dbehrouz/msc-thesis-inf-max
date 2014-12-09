package com.bd.util

import org.apache.spark.graphx._

/**
 * @author Behrouz Derakhshan
 */
object EdgeListTransformer {
  // gets a graph in edge list format and return with edge repetition translated as
  // edge weight
  def transform(graph: Graph[Int, Int]) = {
    val edges = graph.edges.groupBy {
      e => (e.srcId, e.dstId)
    }.map {
      l => new Edge[Int](l._1._1, l._1._2, l._2.size)
    }
    Graph(graph.vertices.map(l => (l._1, 0)), edges)
  }

}
