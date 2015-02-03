package com.bd.util

import org.apache.spark.graphx.{Edge, Graph}

/**
 * @author Behrouz Derakhshan
 */
object GraphUtil {
  // Spark supports only directed graph, this method will return a copy of the graph, duplicating the original edges
  // and changing their directions. If there are already edges that connects two nodes in either direction
  // their values are summed
  def undirected(graph: Graph[Long, Double]) = {
    val edges = graph.edges.map {
      e => {
        if (e.srcId < e.dstId) {
          e
        } else {
          new Edge[Double](e.dstId, e.srcId, e.attr)
        }
      }
    }.groupBy {
      e => (e.srcId, e.dstId)
    }.map {
      l => new Edge[Double](l._1._1, l._1._2, l._2.map(e => e.attr).sum)
    }
    val allEdges = edges.union(edges.map(e => new Edge[Double](e.dstId, e.srcId, e.attr)))
    Graph(graph.vertices, allEdges)
  }

}
