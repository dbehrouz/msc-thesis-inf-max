package com.bd.propogation.ic

import com.bd.SeedFinder
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Behrouz Derakhshan
 */
object SingleCycle extends SeedFinder {
  val ACTIVE: Long = 1
  val VERTEX_ACTIVATED: Long = -2
  val SKIP: Long = -1

  override def run(graph: Graph[VertexId, Double], seedSize: Int, iterations: Int, sc: SparkContext): RDD[VertexId] = {
    val preGraph = graph.mapVertices {
      (id, value) => (0L, List[VertexId]())
    }

    def vertexProgram(id: VertexId, attr: (Long, List[VertexId]), msg: List[VertexId]): (Long, List[VertexId]) = {
      var tmpAtr = attr
      for (m <- msg) {
        if (m == SKIP) {
          tmpAtr = (tmpAtr._1, id :: tmpAtr._2)
        }
        else if (m == VERTEX_ACTIVATED) {
          tmpAtr = (tmpAtr._1 + 1, tmpAtr._2)
        }
        else if (!attr._2.contains(m)) {
          tmpAtr = (tmpAtr._1, m :: tmpAtr._2)
        }
        else {
          tmpAtr
        }
      }
      tmpAtr
    }

    def sendMessage(edge: EdgeTriplet[(Long, List[VertexId]), Double]) = {
      val diff = edge.srcAttr._2.filterNot(edge.dstAttr._2.toSet)
      println(diff)
      if (diff.size > 0) {
        Iterator((edge.dstId, diff))
      }
      else {
        Iterator.empty
      }
    }

    def messageCombiner(a: List[VertexId], b: List[VertexId]) = a ::: b



    val finalGraph = Pregel(preGraph, List(SKIP), activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
    sc.parallelize(finalGraph.vertices.top(seedSize)(Ordering.by(_._2._1)).map(_._1))
  }

}
