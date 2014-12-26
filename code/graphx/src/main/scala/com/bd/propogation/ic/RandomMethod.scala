package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object RandomMethod extends SeedFinder {
  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    val count = graph.vertices.count
    graph.vertices.sample(false, seedSize.toFloat / count.toFloat).map(_._1)
  }
}
