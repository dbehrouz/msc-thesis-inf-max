package com.bd.util

import org.apache.spark.graphx._

/**
 * @author Behrouz Derakhshan
 */
object GraphUtil {
  // Spark supports only directed graph, this method will return a copy of the graph, duplicating the original edges
  // and changing their directions. If there are already edges that connects two nodes in either direction
  // their values are summed
  def undirected(graph: Graph[Long, Double], prob: Double = 1.0, ps: PartitionStrategy = PartitionStrategy.CanonicalRandomVertexCut): Graph[Long, Double] = {
    val partitionedGraph = graph.partitionBy(ps)
    Graph(partitionedGraph.vertices, partitionedGraph.edges.union(partitionedGraph.reverse.edges))
      .groupEdges(_ + _)
      .mapEdges(e => e.attr * prob)
  }

  def mapTypes(graph: Graph[Int, Int]): Graph[Long, Double] = {
    graph.mapVertices((id, value) => value.toLong).mapEdges(e => e.attr.toDouble)
  }

}
