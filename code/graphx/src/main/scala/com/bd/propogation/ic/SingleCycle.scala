package com.bd.propogation.ic

/**
 * @author Behrouz Derakhshan
 */
object SingleCycle {
//  val ACTIVE: Int = 1
//  val NOT_ACTIVE: Int = 0
//  val VERTEX_ACTIVATED: Int = -2
//  val TRIED: Int = 2
//  val SKIP: Int = -1
//
//  def run(graph: Graph[Int, (Int, List[VertexId])],
//          activeNodes: List[VertexId],
//          iterations: Int,
//          prob: Double): Double = {
//
//    def vertexProgram(id: VertexId, attr: (Int, List[VertexId]), msg: List[VertexId]): (Int, List[VertexId]) = {
//      if (msg.contains(VERTEX_ACTIVATED)) {
//        attr
//      }
//      else if (attr == NOT_ACTIVE) {
//        msg
//      }
//      else if (attr == ACTIVE) {
//        TRIED
//      }
//      else {
//        attr
//      }
//
//    }
//
//    def sendMessage(edge: EdgeTriplet[(Int, List[VertexId]), Int]) = {
//      if (edge.srcAttr._1 == ACTIVE) {
//        if (math.random < prob) {
//          Iterator((edge.dstId, edge.srcId :: edge.srcAttr._2))
//        } else {
//          Iterator.empty
//        }
//      } else {
//        Iterator.empty
//      }
//    }
//
//    def messageCombiner(a: List[VertexId], b: List[VertexId]) = a ::: b
//  }
}
