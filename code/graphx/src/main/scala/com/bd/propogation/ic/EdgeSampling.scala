package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark._


/**
 * Ref : Similar to Static Greedy Method
 * Sample edges based on their probability at the beginning of the
 * Then each vertex's spread is the size of its connect component
 * For now it is unified edge probability
 *
 * @author Behrouz Derakhshan
 */
object EdgeSampling extends SeedFinder {
  val DEFAULT_NUMBER_OF_PARTITIONS = 5
  val PARTITIONER = new HashPartitioner(DEFAULT_NUMBER_OF_PARTITIONS)

  def run(graph: Graph[Long, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    val vs = graph.mapVertices((v, value) => 0L).vertices

    var vertices = vs.partitionBy(PARTITIONER).persist().setName("Vertices before loop")
    vertices.count()
    vs.unpersist(blocking = false)
    for (i <- 1 to iterations) {
      // sample based on probability
      val sampledGraph = graph.subgraph(epred = e => math.random < e.attr)

      // vertex id, component id
      val cc = ConnectedComponents.runCC(sampledGraph).vertices

      // TODO : improve by using partitioner
      val vs2 = mapVertices(cc, sc)

      val oldVs = vertices

      vertices = addVertices(vertices, vs2).persist().setName("Vertices iteration: %d ".format(i))

      val c = vertices.count()

      println("Iteration : " + i)
      oldVs.unpersist(blocking = false)
      sampledGraph.unpersistVertices(blocking = false)
      sampledGraph.edges.unpersist(blocking = false)
      sampledGraph.unpersist()
      cc.unpersist(blocking = false)
    }
    graph.unpersist(blocking = false)
    sc.parallelize(vertices.top(seedSize)(Ordering.by(_._2)).map(_._1))
  }

  def mapVertices(cc: RDD[(Long, Long)], sc: SparkContext): RDD[(Long, Long)] = {
    cc.groupBy(_._2).flatMap { v =>
      var res: List[(Long, Long)] = List()
      for (i <- v._2) {
        res = (i._1, v._2.size.toLong) :: res
      }
      res
    }
  }

  def addVertices(vs: RDD[(Long, Long)], vs2: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    vs.join(vs2, PARTITIONER).map(vertex => (vertex._1, vertex._2._1 + vertex._2._2))
  }
}
