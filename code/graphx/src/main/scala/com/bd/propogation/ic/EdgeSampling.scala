package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Graph

import org.apache.spark.rdd.RDD

/**
 * Ref : Similar to Static Greedy Method
 * Sample edges based on their probability at the beginning of the
 * Then each vertex's spread is the size of its connect component
 * For now it is unified edge probability
 *
 * @author Behrouz Derakhshan
 */
object EdgeSampling extends SeedFinder {
  def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    var vs = graph.mapVertices((id, attr) => 0L).vertices
    for (i <- 1 to iterations) {
      // sample based on probability
      val sampledGraph = graph.subgraph(epred = e => math.random < e.attr).cache

      // vertex id, component id
      val cc = sampledGraph.connectedComponents().vertices.cache

      // Group by component id
      // a map of component ids to size of component
      val ccSize = cc.groupBy(_._2).map(c => (c._1, c._2.size.toLong)).collect().toList.toMap

      vs = cc.mapValues(value => ccSize.get(value).get).leftJoin(vs) {
        case (id, val1, val2) => val1 + val2.get
      }.cache

      val oldVs = vs

      println("Iteration : " + i)
      println(vs.top(seedSize)(Ordering.by(_._2)).toList)
      oldVs.unpersist(blocking = false)
      sampledGraph.unpersistVertices(blocking = false)
      sampledGraph.edges.unpersist(blocking = false)
      cc.unpersist(blocking = false)
    }
    sc.parallelize(vs.top(seedSize)(Ordering.by(_._2))).map(_._1)
  }
}
