package com.bd.propogation.ic

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.junit.{Test, After, Before}
import org.scalatest.junit.AssertionsForJUnit

/**
 * @author Behrouz Derakhshan
 */
class SimulationTest extends AssertionsForJUnit {

  var sc: SparkContext = _

  def fullyConnectedGraph(size: Long): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val edgeArray = fullyConnected(vertexArray)
    val vertexRDD: RDD[(Long, Long)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)
    Graph(vertexRDD, edgeRDD)
  }

  def twoCCGraph(size: Long): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val firstCC = fullyConnected(vertexArray.slice(0, 50))
    val secondCC = fullyConnected(vertexArray.slice(50, 100))
    val vertexRDD: RDD[(Long, Long)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(firstCC ::: secondCC)
    Graph(vertexRDD, edgeRDD)
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
    val vertexRDD: RDD[(Long, Long)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(allEdges)
    Graph(vertexRDD, edgeRDD)
  }


  def createVertices(size: Long): List[(Long, Long)] = {
    var vertices: List[(Long, Long)] = List()
    for (i <- 1L to size) {
      vertices :+=(i, i)
    }
    vertices
  }

  def fullyConnected(vertices: List[(Long, Long)]): List[Edge[Double]] = {
    var edges: List[Edge[Double]] = List()
    for (v <- vertices) {
      for (u <- vertices) {
        if (v != u) {
          edges :+= Edge(v._1, u._1, 1.0)
        }
      }
    }
    edges
  }

  @Before def setUp() {
    sc = new SparkContext("local[*]", "Unit Tests")

  }

  @After def tearDown() {
    sc.stop
    sc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  @Test def runWithFullPropagation() {
    val size = 100
    val spread = Simulation.run(fullyConnectedGraph(size), List(2L), 1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }

  @Test def runWithMultipleConnectedComponents() {
    val size = 100
    val spread = Simulation.run(twoCCGraph(size), List(1L, 51L), 1)
    if (size != spread.toInt) {
      fail("Expected spread = " + size + ", actual = " + spread)
    }
  }

}
