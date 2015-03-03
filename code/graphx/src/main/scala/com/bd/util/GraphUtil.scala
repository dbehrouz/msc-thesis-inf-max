package com.bd.util

import org.apache.spark.graphx._

/**
 * @author Behrouz Derakhshan
 */
object GraphUtil {
  // Spark supports only directed graph, this method will return a copy of the graph, duplicating the original edges
  // and changing their directions. If there are already edges that connects two nodes in either direction
  // their values are summed
  def undirected(graph: Graph[Int, Int], prob: Double = 1.0, partitionStrategy: PartitionStrategy = PartitionStrategy.CanonicalRandomVertexCut): Graph[Long, Double] = {
    val mappedPartitionedGraph = mapTypes(graph, prob).partitionBy(partitionStrategy)
    Graph(mappedPartitionedGraph.vertices, mappedPartitionedGraph.edges.union(mappedPartitionedGraph.reverse.edges)).groupEdges(_ + _)
  }

  def mapTypes(graph: Graph[Int, Int], prob: Double = 1.0): Graph[Long, Double] = {
    graph.mapVertices((id, value) => value.toLong).mapEdges(e => e.attr.toDouble * prob)
  }

}
