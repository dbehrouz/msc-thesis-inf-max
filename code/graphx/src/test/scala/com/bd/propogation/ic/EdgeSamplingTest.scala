package com.bd.propogation.ic

import java.util.Calendar

import com.bd.SparkTestBase
import com.bd.util.GraphUtil
import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx._
import org.junit.{Before, Ignore, Test}

/**
 * @author Behrouz Derakhshan
 */
class EdgeSamplingTest extends SparkTestBase {

  var cc: VertexRDD[VertexId] = _

  @Before override def setUp() {
    super.setUp()
    cc = VertexRDD(sc.parallelize(List((1L, 1L), (2L, 1L), (3L, 1L), (4L, 1L), (5L, 5L), (6L, 5L), (7L, 5L))))
  }

  @Test def mapVertices() {
    val mappedValues = EdgeSampling.mapVertices(cc, sc)
    val expected = List((1L, 4L), (2L, 4L), (3L, 4L), (4L, 4L), (5L, 3L), (6L, 3L), (7L, 3L))
    assert(expected == mappedValues.collect.toList.sortBy(_._1))
  }

  @Test def addVertices() {
    val vertices = sc.parallelize(List((1L, 1L), (2L, 2L), (3L, 3L), (4L, 4L), (5L, 5L), (6L, 6L), (7L, 7L)))
    val newVertices = sc.parallelize(List((1L, 4L), (2L, 4L), (3L, 4L), (4L, 4L), (5L, 3L), (6L, 3L), (7L, 3L)))
    val finalVertices = EdgeSampling.addVertices(vertices, newVertices)
    assert(List((1L, 5L), (2L, 6L), (3L, 7L), (4L, 8L), (5L, 8L), (6L, 9L), (7L, 10L)) == finalVertices.collect.toList.sortBy(_._1))

  }

  @Ignore
  @Test def compareDifferentPartitioning() {
    val seedSize = 40
    val iterations = 10
    val prob = 0.01
    val input = "data/hep.txt"

    val result1 = run(seedSize, iterations, sc, prob, input, "EdgePartition2D")
    val result2 = run(seedSize, iterations, sc, prob, input, "EdgePartition2D")
    val result3 = run(seedSize, iterations, sc, prob, input, "EdgePartition2D")
    val result4 = run(seedSize, iterations, sc, prob, input, "EdgePartition2D")


    List(result1, result2, result3, result4).foreach(println)

  }

  def run(seedSize: Int, iterations: Int, sc: SparkContext
          , prob: Double, input: String, partitioningMethod: String): String = {
    val start = Calendar.getInstance().getTimeInMillis
    val ps = PartitionStrategy.fromString(partitioningMethod)
    val graph = GraphUtil.undirected(GraphUtil.mapTypes(GraphLoader.edgeListFile(sc, input)), prob).persist(StorageLevels.MEMORY_ONLY)
    val result = EdgeSampling.run(graph, seedSize, iterations, sc)
    val end = Calendar.getInstance().getTimeInMillis
    graph.unpersist(blocking = true)
    graph.vertices.unpersist(blocking = true)
    graph.edges.unpersist(blocking = true)
    partitioningMethod + ": " + (end - start)

  }

}
