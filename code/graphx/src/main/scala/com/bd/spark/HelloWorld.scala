package com.bd.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author Behrouz Derakhshan
 */
object HelloWorld {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0)).cache()
    val counts = lines.flatMap(line => line.split(" ")).groupBy(l => l).map(w => (w._1, w._2.size))
    counts.saveAsTextFile(args(1))
  }

}
