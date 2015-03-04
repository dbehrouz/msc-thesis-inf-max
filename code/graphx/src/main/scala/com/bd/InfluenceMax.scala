package com.bd

import com.bd.propogation.heuristic.{Degree, DegreeDiscount, PageRankMethod}
import com.bd.propogation.ic._
import com.bd.util.GraphUtil
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * @author Behrouz Derakhshan
 */
object InfluenceMax extends Logging {

  def runAlgorithm(method: String,
                   graph: Graph[Long, Double],
                   seedSize: Int,
                   iterations: Int,
                   sc: SparkContext): RDD[VertexId] = {
    if ("degree".equals(method)) {
      println("Running Degree method")
      Degree.run(graph, seedSize, iterations, sc)
    } else if ("degreediscount".equals(method)) {
      println("Running Degree Discount method")
      DegreeDiscount.run(graph, seedSize, iterations, sc)
    } else if ("edgesampling".equals(method)) {
      println("Running Edge Sampling method")
      EdgeSampling.run(graph, seedSize, iterations, sc)
    } else if ("greedyic".equals(method)) {
      println("Running Greedy IC method")
      GreedyIC.run(graph, seedSize, iterations, sc)
    } else if ("random".equals(method)) {
      RandomMethod.run(graph, seedSize, iterations, sc)
    } else if ("cc".equals(method)) {
      ConnectedComponents.run(graph, seedSize, iterations, sc)
    } else if ("pagerank".equals(method)) {
      PageRankMethod.run(graph, seedSize, iterations, sc)
    } else if ("singlecycle".equals(method)){
      SingleCycle.run(graph, seedSize, iterations, sc)
    }
    else {
      throw new IllegalArgumentException
    }
  }

//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Influence Maximization")
//    val sc = new SparkContext(conf)
//    val method = args(0).toLowerCase
//    println("Method : " + method)
//    val inputGraphFile = args(1)
//    val seedSize = args(2).toInt
//    println("Seed Size :" + seedSize)
//    val iterations = args(3).toInt
//    println("Iterations :" + iterations)
//    val prob = args(4).toDouble
//    println("Propagation Probability: " + prob)
//    val output = args(5)
//    println("Output Directory: " + output)
//
//    val graph = GraphUtil.undirected(GraphLoader.edgeListFile(sc, inputGraphFile), prob).cache()
//    val result = runAlgorithm(method, graph, seedSize, iterations, sc)
//
//    result.saveAsTextFile(output)
//
//  }

}
