package com.bd

import java.util.Calendar

import com.bd.util.{EdgeListTransformer, GraphUtil}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File


/**
 * @author Behrouz Derakhshan
 */
object Driver {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Influence Maximization")
    val sc = new SparkContext(conf)
    val algorithms = List("degree", "degreediscount", "pagerank", "random", "edgesampling");
    for (alg <- algorithms) {
      run(alg, args(0), args(1).toInt, args(2).toInt, args(3).toDouble, args(4) + "-" + alg, sc)
    }


  }

  def run(method: String, input: String, seedRange: Int, iterations: Int, prob: Double, output: String, sc: SparkContext) {
    println("Method : " + method)
    println("Seed Range :" + seedRange)
    println("Iterations :" + iterations)
    println("Propagation Probability: " + prob)
    println("Output Directory: " + output)


    for (i <- 1 to seedRange) {
      val start = Calendar.getInstance().getTimeInMillis
      val graph = GraphUtil.undirected(EdgeListTransformer.transform(GraphLoader.edgeListFile(sc, input), prob)).persist(StorageLevels.MEMORY_ONLY)
      val result = InfluenceMax.runAlgorithm(method, graph, i, iterations, sc)
      val end = Calendar.getInstance().getTimeInMillis()
      result.saveAsTextFile(output + "/" + i.toString)
      File(output + "/" + "times").appendAll(i + " : " + (end - start).toString + "\n")
      graph.unpersistVertices(blocking = false)
      graph.edges.unpersist(blocking = false)
      graph.unpersist(blocking = false)
      result.unpersist(blocking = false)
    }

  }
}
