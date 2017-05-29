package ClusterSchedulingSimulation.utils


class StatisticsArray {
  private [this] val _windowsSize: Int = 15
  private [this] val _array: Array[Long] =  Array.fill(_windowsSize){0L}

  private [this] var _sum: Long = 0L
  private [this] var _sumWindow: Long = 0L

  private [this] var _max: Long = 0L

  private [this] var _index: Int = 0
  private [this] var _len: Int = 0


  def addValue(value:Long):Unit = {
    _sumWindow -= _array(_index)

    _array(_index) = value

    _index = (_index + 1) % _windowsSize
    _len += 1

    _sum += value
    _sumWindow += value
    _max = Math.max(_max, value)
  }

  def getMean(value: Long): Double = {
    addValue(value)
    getMean
  }

  def getMeanWindow(value: Long): Double = {
    addValue(value)
    getMeanWindow
  }

  def getMax(value: Long): Long = {
    addValue(value)
    getMax
  }

  def getMaxWindow(value: Long): Long = {
    addValue(value)
    getMaxWindow
  }

  def getMean: Double = {
    _sum / _len
  }

  def getMeanWindow: Double = {
    _sumWindow / _windowsSize.toDouble
  }

  def getMax: Long = {
    _max
  }

  def getMaxWindow: Long = {
    _array.max
  }


}
