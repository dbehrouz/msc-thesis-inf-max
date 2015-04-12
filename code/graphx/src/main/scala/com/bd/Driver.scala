package com.bd

import java.util.Calendar

import com.bd.util._
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File


/**
 * Class that runs a set of experiments, user have to provide
 * the input graph, methods, experiment sizes and probably.
 * @author Behrouz Derakhshan
 */
object Driver {
  // List of algorithms that does not require the graph to be pre processed.
  val ALGORITHMS_WITH_RAW_GRAPH = List("degreeweighted", "random")

  // List of every method implemented, used in case user does not supply any algorithms
  val DEFAULT_METHODS = List("degreeweighted", "degreediscount", "pagerank", "random", "edgesampling", "degree")

  /**
   * If experiment size is set to be a range from 1 to X use : rX (where x is the maximum size)
   * For example : r1000 ==> range from 1 to 1000
   *
   * If experiment sizes are list of values use comma separated
   * For example : 10,100,1000 ==> input size of 10, 100 and 1000
   * @param sizeArgument argument provided through the
   * @return List of experiment sizes
   */
  def getSeedSizes(sizeArgument: String): List[Int] = {
    if (sizeArgument.startsWith("r")) {
      (1 to sizeArgument.substring(1).toInt).toList
    } else {
      sizeArgument.split(",").map(_.toInt).toList
    }
  }

  /**
   * Entry point for Driver class.
   *
   * @param args
   * args(0): input graph location (file system or hdfs)
   * args(1): experiment seed sizes
   * args(2): number of iterations
   * args(3): global probability for all the edges
   * args(4): output location (file system or hdfs)
   * (optional) args(5): list of algorithms
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Influence Maximization")
    val sc = new SparkContext(conf)

    // Input parameters
    val input = args(0)
    val seedSizes = getSeedSizes(args(1))
    val numberOfIterations = args(2).toInt
    val edgeProbability = args(3).toDouble
    val outputLocation = args(4)
    val algorithms = if (args.size > 5) args(5).split(",").toList else DEFAULT_METHODS

    println("Experiment Level Properties: ")
    println(" Input Location: " + input)
    println(" Propagation Probability: " + edgeProbability)
    println(" Iterations :" + numberOfIterations)

    val edgeListGraph = GraphUtil.mapTypes(
      GraphLoader.edgeListFile(sc, input).partitionBy(PartitionStrategy.CanonicalRandomVertexCut)
    )
    edgeListGraph.edges.count

    // First run all the methods that only need the "Raw Graph"
    for (alg <- algorithms.intersect(ALGORITHMS_WITH_RAW_GRAPH)) {
      run(edgeListGraph, alg, seedSizes, numberOfIterations, outputLocation + "-" + alg, sc)
    }
    val restOfMethods = algorithms.diff(ALGORITHMS_WITH_RAW_GRAPH)

    // Run the rest of the methods using the pre processed graph
    if (restOfMethods.size > 0) {
      val directedGraph = GraphUtil.undirected(edgeListGraph, edgeProbability).cache()
      directedGraph.edges.count
      directedGraph.vertices.count
      for (alg <- restOfMethods) {
        run(directedGraph, alg, seedSizes, numberOfIterations, outputLocation + "-" + alg, sc)
      }
    }


  }

  /**
   * Run a given method, once for every experiment size .
   *
   * @param graph input graph
   * @param method algorithm to run
   * @param experimentSeedSize list of experiment sizes
   * @param iterations number of iterations (only relevant for some of the methods)
   * @param output output directory for storing the result (initial seed)
   * @param sc spark context
   */
  def run(graph: Graph[Long, Double], method: String, experimentSeedSize: List[Int], iterations: Int, output: String, sc: SparkContext) {
    println("Method : " + method)
    println("Seed Range :" + experimentSeedSize)
    println("Output Directory: " + output)

    for (i <- experimentSeedSize) {
      val start = Calendar.getInstance().getTimeInMillis
      val result = InfluenceMax.runAlgorithm(method, graph, i, iterations, sc)
      val end = Calendar.getInstance().getTimeInMillis
      result.saveAsTextFile(output + "/" + i.toString)
      File(output + "/" + "times").appendAll(i + " : " + (end - start).toString + "\n")
      graph.unpersistVertices(blocking = false)
      graph.edges.unpersist(blocking = false)
      graph.unpersist(blocking = false)
      result.unpersist(blocking = false)
    }

  }
}
