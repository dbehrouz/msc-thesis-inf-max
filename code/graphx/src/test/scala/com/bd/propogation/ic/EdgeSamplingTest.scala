package com.bd.propogation.ic

import com.bd.SparkTestBase
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.junit.{Before, Test}

/**
 * @author Behrouz Derakhshan
 */
class EdgeSamplingTest extends SparkTestBase {

  var cc: VertexRDD[VertexId] = _

  @Before override def setUp() {
    super.setUp()
    cc = VertexRDD(sc.parallelize(List((1L, 1L), (2L, 1L), (3L, 1L), (4L, 1L), (5L, 5L), (6L, 5L), (7L, 5L))))
  }

  @Test def findCCSize() {

    val mappedValues = EdgeSampling.findSizes(cc, sc)
    assert(Map(1L -> 4L, 5L -> 3L) == mappedValues.value)
  }

  @Test def mapVertices() {
    val broadcastedMap = sc.broadcast(Map(1L -> 4L, 5L -> 3L))
    val mappedValues = EdgeSampling.mapVertices(cc, broadcastedMap, sc)
    // assert(Map(1L -> 4L, 2L -> 4L, 3L -> 4L, 4L -> 4L, 5L -> 3L, 6L -> 3L, 7L -> 3L) == mappedValues)
  }

  @Test def addVertices() {
    val vertices = VertexRDD(sc.parallelize(List((1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L), (5L, 5L), (6L, 6L), (7L, 7L))))
    val newVertices = VertexRDD(sc.parallelize(List((1L, 4L), (2L, 4L), (3L, 4L), (4L, 4L), (5L, 3L), (6L, 3L), (7L, 3L))))
    val finalVertices = EdgeSampling.addVertices(vertices, newVertices)
    finalVertices.foreach(println)
    //assert(Map(1L -> 5L, 2L -> 6L, 3L -> 7L, 4L -> 8L, 5L -> 8L, 6L -> 9L, 7L -> 10L) == finalVertices)

  }

}
