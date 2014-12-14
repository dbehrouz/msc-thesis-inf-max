package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object DegreeDiscount extends SeedFinder {
  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = ???
}
