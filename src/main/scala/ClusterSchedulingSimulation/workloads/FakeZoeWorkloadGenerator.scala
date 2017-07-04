package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{Job, Workload, WorkloadGenerator}

/**
  * Generate the toy example presented in the CCGrid'17 paper
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

    val jobA = Job(1, 0, 4, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobA.cpuUtilization = Array.fill(10)(0F)
    jobA.memoryUtilization = Array.fill(10)(0F)
    workload.addJob(jobA)

    val jobB = Job(2, 0, 3, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobB.cpuUtilization = Array.fill(10)(0F)
    jobB.memoryUtilization = Array.fill(10)(0F)
    workload.addJob(jobB)

    val jobC = Job(3, 0, 5, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobC.cpuUtilization = Array.fill(10)(0F)
    jobC.memoryUtilization = Array.fill(10)(0F)
    workload.addJob(jobC)

    val jobD = Job(4, 0, 2, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobD.cpuUtilization = Array.fill(10)(0F)
    jobD.memoryUtilization = Array.fill(10)(0F)
    workload.addJob(jobD)

    workload
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
