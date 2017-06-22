package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{Job, Workload, WorkloadGenerator}

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class FakeZoeWorkloadGenerator(
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

    val jobA = Job(1, 0, 2, 20, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobA.cpuUtilization = Array.fill(100)(0)
    jobA.memoryUtilization = Array.fill(100)(0L)
    workload.addJob(jobA)

    val jobB = Job(2, 0, 5, 5, workloadName, 100, 13743895347L, numCoreTasks = Option(5))
    jobB.cpuUtilization = Array.fill(100)(0)
    jobB.memoryUtilization = Array.fill(100)(0L)
    workload.addJob(jobB)

    val jobC = Job(3, 0, 4, 15, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobC.cpuUtilization = Array.fill(100)(0)
    jobC.memoryUtilization = Array.fill(100)(0L)
    workload.addJob(jobC)

    val jobD = Job(4, 10, 8, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(2))
    jobD.cpuUtilization = Array.fill(100)(0)
    jobD.memoryUtilization = Array.fill(100)(0L)
    workload.addJob(jobD)

    workload
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
