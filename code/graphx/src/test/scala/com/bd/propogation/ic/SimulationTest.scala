package com.bd.propogation.ic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before, Test}
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
    nCCGraph(size, 2)
  }

  def graph(vertexArray: List[(Long, Long)],
            edgeArray: List[Edge[Double]]): Graph[Long, Double] = {
    val vertexRDD: RDD[(Long, Long)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)
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
    graph(vertexArray, allEdges)
  }

  def simpleTree(size: Long): Graph[Long, Double] = {
    val vertexArray = createVertices(size)
    val edgeArray = simpleConnected(vertexArray)
    graph(vertexArray, edgeArray)
  }


  def createVertices(size: Long): List[(Long, Long)] = {
    var vertices: List[(Long, Long)] = List()
    for (i <- 1L to size) {
      vertices ::=(i, i)
    }
    vertices
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

  @Before def setUp() {
    SparkUtil.silenceSpark()
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

  object SparkUtil {
    def silenceSpark() {
      setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
    }

    def setLogLevels(level: Level, loggers: TraversableOnce[String]) = {
      loggers.map {
        loggerName =>
          val logger = Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel()
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }
  }

}
