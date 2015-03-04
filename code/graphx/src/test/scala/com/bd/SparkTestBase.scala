package com.bd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before}
import org.scalatest.junit.AssertionsForJUnit

/**
 * @author Behrouz Derakhshan
 */
class SparkTestBase extends AssertionsForJUnit {
  var sc: SparkContext = _

  @Before def setUp() {
    SparkUtil.silenceSpark()
    sc = new SparkContext("local[*]", "Unit Tests")
    Logger.getRootLogger.setLevel(Level.ERROR)


  }

  @After def tearDown() {
    sc.stop()
    sc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  final def assertEqual(expected: Any, actual: Any, message: String = "") {
    if (expected != actual) {
      fail("Failed : expected = " + expected + ", actual = " + actual + " " + message)
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


  def createVertices(size: Int): List[(Long, Long)] = {
    var vertices: List[(Long, Long)] = List()
    for (i <- 1 to size) {
      vertices ::=(i, i)
    }
    vertices
  }

  def graph(vertexArray: List[(Long, Long)], edgeArray: List[Edge[Double]]): Graph[Long, Double] = {
    val vertexRDD: RDD[(Long, Long)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)
    Graph(vertexRDD, edgeRDD)
  }

}
