package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object Degree extends SeedFinder {
  def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    sc.parallelize(graph.degrees.top(seedSize)(Ordering.by(_._2)).map(_._1))
  }
}

