package com.bd.util

import scala.io.Source
import scala.reflect.io.File

/**
 * @author Behrouz Derakhshan
 */
object ModifyHadoopResult {
  def main(args: Array[String]) {
    val lines = Source.fromFile("multicycle").getLines().toList
    for (i <- 1 to lines.size) {
      for (j <- 1 to i) {
        val folder = "output-multicycle/" + i + "/"
        File(folder).createDirectory(force = false, failIfExists = false)
        File(folder + "data.txt").appendAll(lines(j-1))
        if(j != i) File(folder + "data.txt").appendAll("\n")
      }
    }
  }

}
