package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
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
    var vs = graph.mapVertices((v, value) => 0L).vertices
    for (i <- 1 to iterations) {
      // sample based on probability
      val sampledGraph = graph.subgraph(epred = e => math.random < e.attr)

      // vertex id, component id
      val cc = ConnectedComponents.runCC(sampledGraph).vertices

      // TODO : improve by using partitioner
      val vs2 = mapVertices(cc, sc)

      vs = addVertices(vs, vs2)

      println("Iteration : " + i)
      sampledGraph.unpersistVertices(blocking = false)
      sampledGraph.edges.unpersist(blocking = false)
      cc.unpersist(blocking = false)
    }
    graph.unpersist(blocking = false)
    sc.parallelize(vs.top(seedSize)(Ordering.by(_._2)).map(_._1))
  }

  def mapVertices(cc: VertexRDD[VertexId], sc: SparkContext): VertexRDD[Long] = {
    VertexRDD(cc.groupBy(_._2).flatMap { v =>
      var res: List[(Long, Long)] = List()
      for (i <- v._2) {
        res = (i._1, v._2.size.toLong) :: res
      }
      res
    })
  }

  def addVertices(vs: VertexRDD[Long], vs2: VertexRDD[Long]): VertexRDD[Long] = {
    //    vs.join(vs2).map(joined => (joined._1, joined._2._1 + joined._2._2))
    vs.innerJoin(vs2) {
      (id, v1, v2) => v1 + v2
    }
  }
}
