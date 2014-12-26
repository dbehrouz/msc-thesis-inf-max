package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx.{Graph, VertexId}
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
      val sampledGraph = graph.subgraph(epred = e => math.random < e.attr)

      // vertex id, component id
      val cc = sampledGraph.connectedComponents().vertices.persist(StorageLevels.MEMORY_ONLY)

      // Group by component id
      // a map of component ids to size of component
      // broad cast this variable
      val ccSize = sc.broadcast(cc.groupBy(_._2).map(c => (c._1, c._2.size.toLong)).collect().toList.toMap)

      vs = cc.mapValues(value => ccSize.value.get(value).get).leftJoin(vs) {
        case (id, val1, val2) => val1 + val2.get
      }

      val oldVs = vs

      println("Iteration : " + i)
      oldVs.unpersist(blocking = false)
      sampledGraph.unpersistVertices(blocking = false)
      sampledGraph.edges.unpersist(blocking = false)
      cc.unpersist(blocking = false)
    }
    sc.parallelize(vs.top(seedSize)(Ordering.by(_._2))).map(_._1)
  }
}
