package ClusterSchedulingSimulation.utils

import scala.collection.mutable.ArrayBuffer


class StatisticsArray {
  private [this] val _array: ArrayBuffer[Long] =  ArrayBuffer[Long]()

  private [this] var _sum: Long = 0L

  private [this] var _max: Long = 0L


  def addValue(value:Long):Unit = {
    _array.append(value)
    _sum += value
    _max = Math.max(_max, value)
  }

  def getMean: Double = {
    _sum / _array.length.toDouble
  }
  def getMean(windowSize: Int): Double = {
    _array.takeRight(windowSize).sum / windowSize.toDouble
  }

  def getMax: Long = {
    _max
  }

  def getMax(windowSize: Int): Long = {
    _array.takeRight(windowSize).max
  }


}
