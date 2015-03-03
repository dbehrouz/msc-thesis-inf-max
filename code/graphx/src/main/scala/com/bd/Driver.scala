package com.bd

import java.util.Calendar

import com.bd.util._
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
    var algorithms = List("degree", "degreediscount", "pagerank", "random", "edgesampling");
    var range:List[Int] = List()

    // arguments for input size
    // - r1000 ==> range from 1 to 1000
    // 10,100,1000 ==> input size of 10, 100 and 1000
    if(args(1).startsWith("r")){
      range = (1 to args(1).substring(1).toInt).toList
    } else {
      range = args(1).split(",").map(_.toInt).toList
    }
    if(args.size > 5){
      algorithms = args(5).split(",").toList

    }
    for (alg <- algorithms) {
      run(alg, args(0), range, args(2).toInt, args(3).toDouble, args(4) + "-" + alg, sc)
    }


  }

  def run(method: String, input: String, experimentSeedSize:List[Int], iterations: Int, prob: Double, output: String, sc: SparkContext) {
    println("Method : " + method)
    println("Seed Range :" + experimentSeedSize)
    println("Iterations :" + iterations)
    println("Propagation Probability: " + prob)
    println("Output Directory: " + output)


    for (i <- experimentSeedSize) {
      val start = Calendar.getInstance().getTimeInMillis
      val graph = GraphUtil.undirected(GraphLoader.edgeListFile(sc, input).partitionBy(PartitionStrategy.CanonicalRandomVertexCut).cache(), prob).cache()
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
