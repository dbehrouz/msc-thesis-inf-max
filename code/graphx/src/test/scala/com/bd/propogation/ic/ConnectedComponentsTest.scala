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
    val weight = 1
    val vertices = createVertices(size)
    var edges: List[Edge[Double]] = List()
    edges ::= Edge(1, 2, weight)
    edges ::= Edge(1, 3, weight)
    edges ::= Edge(1, 4, weight)
    val undirectedGraph = GraphUtil.undirected(graph(vertices, edges))
    val cc = ConnectedComponents.runCC(undirectedGraph).vertices
    assert(cc.collect.toList.count(_._2 == 1L) == 4)
  }

}
