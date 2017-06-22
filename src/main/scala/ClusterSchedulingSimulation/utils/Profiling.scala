package ClusterSchedulingSimulation.utils

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable


object Profiling extends LazyLogging {

  var blocks: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()

  def time[R](block: => R, id: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val elapsedTime  = System.nanoTime() - t0
//    val blackToString = block.hashCode()
    blocks(id) = elapsedTime + blocks.getOrElse(id, 0L)
//    logger.info("Elapsed time: " + elapsedTime + "ns")
    result
  }

  def print(): Unit = {
    blocks.foreach{case (block, time) => logger.info("Block: " + block + " Time: " + (time / Math.pow(10,9)) + "s")}
  }

}
