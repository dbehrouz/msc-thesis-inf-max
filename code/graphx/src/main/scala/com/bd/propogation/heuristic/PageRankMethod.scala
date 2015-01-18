package com.bd.propogation.heuristic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object PageRankMethod extends SeedFinder{
  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    sc.parallelize(graph.pageRank(0.1).vertices.top(seedSize)(Ordering.by(_._2)).map(_._1))
  }
}
