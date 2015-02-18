package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexRDD, Graph, VertexId}
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
      val cc = ConnectedComponents.runCC(sampledGraph).vertices

      // Group by component id
      // a map of component ids to size of component
      // broad cast this variable
      val ccSize = findSizes(cc, sc)

      val vs2 = mapVertices(cc, ccSize, sc)
      vs = addVertices(vs, vs2)

      println("Iteration : " + i)
      sampledGraph.unpersistVertices(blocking = false)
      sampledGraph.edges.unpersist(blocking = false)
      cc.unpersist(blocking = false)
    }
    graph.unpersist(blocking = false)
    sc.parallelize(vs.top(seedSize)(Ordering.by(_._2)).map(_._1))
  }

  def findSizes(cc: VertexRDD[VertexId], sc: SparkContext): Broadcast[Map[VertexId, Long]] = {
    sc.broadcast(cc.groupBy(_._2).map(c => (c._1, c._2.size.toLong)).collect().toList.toMap)
  }

  def mapVertices(cc: VertexRDD[VertexId], ccSize: Broadcast[Map[VertexId, Long]], sc: SparkContext): VertexRDD[Long] = {
    cc.mapValues(value => ccSize.value.get(value).get)
  }

  def addVertices(vs: VertexRDD[Long], vs2: VertexRDD[Long]): VertexRDD[Long] = {
    vs.innerJoin(vs2) { case (id, v1, v2) => v1 + v2}
  }
}
