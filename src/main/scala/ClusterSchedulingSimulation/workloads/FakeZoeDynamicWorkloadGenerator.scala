package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{Job, Workload, WorkloadGenerator}
import ClusterSchedulingSimulation.utils.Constant

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class FakeZoeDynamicWorkloadGenerator(
                                val workloadName: String
                              )
  extends WorkloadGenerator {
  logger.info("Generating " + workloadName + " Workload...")

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    assert(timeWindow >= 0)
    val workload = new Workload(workloadName)

    val jobA = Job(1, 0, 0, 6000, workloadName, 100, 137438953472L, numCoreTasks = Option(1))
    jobA.cpuUtilization = Array.fill(100)(100L)
    val memoryUtilization = Array(
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
      10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
      10, 20, 15, 30, 15, 20, 20, 15, 10, 5,
      6, 7, 8, 9, 10, 10, 10, 10, 10, 10,
      10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
      10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
      10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
      50, 60, 70, 80, 90, 100, 110, 120, 120, 120,
      110, 100, 90, 80, 70, 60, 50, 40, 30, 20,
      10, 9, 8, 7, 6, 5, 4, 3, 2, 1
    )
    jobA.memoryUtilization = memoryUtilization.map(_ * Constant.GiB)
    workload.addJob(jobA)

    val jobB = Job(2, 0, 0, 6000, workloadName, 100, 137438953472L / 2, numCoreTasks = Option(1))
    jobB.cpuUtilization = Array.fill(100)(100L)
    jobB.memoryUtilization = memoryUtilization.map(_ * Constant.GiB / 2)
    workload.addJob(jobB)

    workload
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
