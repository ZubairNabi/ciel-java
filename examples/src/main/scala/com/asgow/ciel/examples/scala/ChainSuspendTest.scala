package com.asgow.ciel.examples.scala {

import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._
import scala.util.continuations._

class ChainSuspendTest extends SkylaThread[Int] {

  override def run = {
    var iterations = Ciel.args(0).toInt
    val tasks = Ciel.args(1).toInt

    var taskResults = new Array[Reference](0)
    var j = 0

    taskResults = new Array[Reference](tasks)
    j = 0
    while (j < tasks) {
      taskResults(j) = Skyla.spawn { _ => 
        Thread.sleep(10)
        var i = 0
        while (i < iterations) {

	  Skyla.suspendTask
	  i = i + 1	  

        }
	j
      }
      j = j + 1
    }
   
    Skyla.blockOnAll(taskResults)

    iterations * tasks
  }
}

}