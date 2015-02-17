package com.bd.propogation.ic

import com.bd.SparkTestBase
import com.bd.util.GraphUtil
import org.apache.spark.graphx.Edge
import org.junit.Test

/**
 * @author Behrouz Derakhshan
 */
class ConnectedComponentsTest extends SparkTestBase {

  @Test def singleCCGraph() {
    // create graph
    // 4 edges of weight 1
    val size = 4
    val weight = 1.0
    val vertices = createVertices(size)
    var edges: List[Edge[Double]] = List()
    edges ::= Edge(1L, 2L, weight)
    edges ::= Edge(1L, 3L, weight)
    edges ::= Edge(1L, 4L, weight)
    val undirectedGraph = GraphUtil.undirected(graph(vertices, edges))
    val cc = ConnectedComponents.runCC(undirectedGraph).vertices
    assert(cc.collect.toList.count(_._2 == 1L) == 4)
  }

}
