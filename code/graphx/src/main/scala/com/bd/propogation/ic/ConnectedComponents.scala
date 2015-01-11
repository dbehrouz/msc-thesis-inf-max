package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


import scala.reflect.ClassTag

/**
 * @author Behrouz Derakhshan
 */
object ConnectedComponents extends SeedFinder {
  override def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    runCC(graph).vertices.map(_._2).distinct
  }
  // similar to connected component algorithm but on a directed graph
  def runCC[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]) = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      }  else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    Pregel(ccGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
  } // end of connectedComponents
}
