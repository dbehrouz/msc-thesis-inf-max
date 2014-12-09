package com.bd.propogation.ic

import com.bd.util.EdgeListTransformer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._

/**
 * Single Cycle IC, each vertex tries to propagate based on edge weight
 * Assume constant edge weight of 0.01 for now
 * @author Behrouz Derakhshan
 */
object SingleCycle {
  // args : graph location (edgeFileList), output location
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Single Cycle IC")
    val sc = new SparkContext(conf)
    // read graph and preprocess it
    val graph = EdgeListTransformer.transform(GraphLoader.edgeListFile(sc, args(0)))


  }

  def runIC[VD,ED](graph: Graph[VD, ED]):(VertexId , Long) = {
    (1 , 1)
  }
}
