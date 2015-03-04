package com.bd.util

import com.bd.SparkTestBase
import org.apache.spark.graphx.Edge
import org.junit.Test

/**
 * @author Behrouz Derakhshan
 */
class GraphUtilTest extends SparkTestBase {

  def createSimpleGraph(verticesCount: Int, weight: Double): List[Edge[Double]] = {
    val vertices = createVertices(verticesCount)
    var edges: List[Edge[Double]] = List()
    edges
    for (v <- vertices) {
      for (u <- vertices) {
        if (v != u) {
          edges ::= Edge(v._1, u._1, weight)
        }
      }
    }
    edges
  }

  @Test def checkUndirectedGraphHasCorrectEdges() {
    // create graph
    // 4 edges of weight 1
    val size = 5
    val weight = 1
    val vertices = createVertices(size)
    var edges: List[Edge[Double]] = List()
    edges ::= Edge(1, 2, weight)
    edges ::= Edge(1, 3, weight)
    edges ::= Edge(1, 4, weight)
    edges ::= Edge(1, 5, weight)

    val undirectedGraph = GraphUtil.undirected(graph(vertices, edges))
    if (undirectedGraph.edges.count != 8) {
      fail("Expected edge count = " + 8 + ", actual = " + undirectedGraph.edges.count)
    }
    assert(undirectedGraph.edges.filter(_.attr == 1.0).count == 8)
  }

  @Test def checkWeightsAreAddedCorrectly() {
    // create graph
    // 6 edges of weight 1
    val size = 3
    val weight = 1
    val vertices = createVertices(size)
    var edges: List[Edge[Double]] = List()
    edges ::= Edge(1, 2, weight)
    edges ::= Edge(1, 2, weight)
    edges ::= Edge(1, 2, weight)
    edges ::= Edge(1, 3, weight)
    edges ::= Edge(1, 3, weight)
    edges ::= Edge(2, 3, weight)

    val edgeList = GraphUtil.undirected(graph(vertices, edges)).edges.collect().toList
    assert(edgeList.size == 6)
    assert(getWeight(edgeList, 1L, 2L) == 3)
    assert(getWeight(edgeList, 2L, 1L) == 3)
    assert(getWeight(edgeList, 1L, 3L) == 2)
    assert(getWeight(edgeList, 3L, 1L) == 2)
    assert(getWeight(edgeList, 2L, 3L) == 1)
    assert(getWeight(edgeList, 3L, 2L) == 1)
  }

  private def getWeight(edgeList: List[Edge[Double]], src: Long, dst: Long): Double = {
    val filtered = edgeList.filter(e => e.srcId == src && e.dstId == dst)
    assert(filtered.size == 1)
    filtered(0).attr
  }
}
