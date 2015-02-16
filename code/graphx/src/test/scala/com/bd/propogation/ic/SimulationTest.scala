package com.bd.propogation.ic

import com.bd.SparkTestBase
import org.apache.spark.graphx.{Edge, Graph}
import org.junit.Test

/**
 * @author Behrouz Derakhshan
 */
class SimulationTest extends SparkTestBase {

  def fullyConnectedGraph(size: Long): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val edgeArray = fullyConnected(vertexArray)
    graph(vertexArray,edgeArray)
  }


  def twoCCGraph(size: Long): Graph[Long, Double] = {
    nCCGraph(size, 2)
  }


  def nCCGraph(size: Long, n: Int): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val batchSize = (size / n).toInt
    var start = 0
    var allEdges: List[Edge[Double]] = List()
    for (i <- 1 to n) {
      val end = if (start + batchSize >= size) size.toInt else start + batchSize
      allEdges = allEdges ::: fullyConnected(vertexArray.slice(start, end))
      start += batchSize
    }
    graph(vertexArray, allEdges)
  }

  def simpleTree(size: Long): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val edgeArray = simpleConnected(vertexArray)
    graph(vertexArray, edgeArray)
  }

  def fullyConnected(vertices: List[(Long, Long)]): List[Edge[Double]] = {
    var edges: List[Edge[Double]] = List()
    for (v <- vertices) {
      for (u <- vertices) {
        if (v != u) {
          edges ::= Edge(v._1, u._1, 1.0)
        }
      }
    }
    edges
  }


  def simpleConnected(vertices: List[(Long, Long)]): List[Edge[Double]] = {
    var edges: List[Edge[Double]] = List()
    val sorted = vertices.map(_._1).sorted
    for (v <- sorted) {
      if (v != sorted.length) {
        edges ::= Edge(v, v + 1, 1.0)
      }
    }
    edges
  }

  @Test def runWithFullPropagation() {
    val size = 100
    val spread = Simulation.run(fullyConnectedGraph(size), List(2L), 1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }

  @Test def runWithTwoConnectedComponents() {
    val size = 100
    val spread = Simulation.run(twoCCGraph(size), List(1L, 51L), 1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }

  @Test def runWithMultipleConnectedComponents() {
    val size = 1000
    val cc = 100
    val graph = nCCGraph(size, cc)
    val spread = Simulation.run(graph,
      graph.connectedComponents.vertices.map(_._2).distinct.collect.toList,
      1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }

  @Test def runWithSimpleTree() {
    val size = 10

    val spread = Simulation.run(simpleTree(size), List(1L), 1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }


}
