package ClusterSchedulingSimulation.utils


object UniqueIDGenerator {
  var counter = 0
  def generateUniqueID: Int = {
    counter += 1
    counter
  }
}
