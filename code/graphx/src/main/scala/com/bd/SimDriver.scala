package com.bd

import java.io.File

import com.bd.propogation.ic.Simulation
import com.bd.util.{EdgeListTransformer, GraphUtil}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Behrouz Derakhshan
 */
object SimDriver {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IC Simulation")
    val sc = new SparkContext(conf)
    val outputs = List("output-degreediscount/", "output-edgesampling/", "output-pagerank/", "output-random/")

    for (o <- outputs) {
      run(o, args(0), args(1).toInt, args(2).toDouble, sc)
    }
  }

  def run(activeNodesDir: String, inputGraphFile: String, iterations: Int, prob: Double, sc: SparkContext) {
    println("Number of Simulations: " + iterations)
    println("Propagation Prob : " + prob)

    for (file <- new File(activeNodesDir).listFiles()) {
      val activeNodes = sc.textFile(file.getAbsolutePath)
        .flatMap(l => l.split(','))
        .map(l => l.toLong)
        .collect()
        .toList

      println("Number of Initial Active Nodes " + activeNodes.length)
      val graph = GraphUtil.undirected(EdgeListTransformer.transform(GraphLoader.edgeListFile(sc, inputGraphFile), prob))

      val spread = Simulation.run(graph, activeNodes, iterations)
      println("Total Spread: " + spread)

      scala.reflect.io.File("finalResults/" + activeNodesDir).appendAll(activeNodes.length + "," + spread + "\n")
    }


  }

}
