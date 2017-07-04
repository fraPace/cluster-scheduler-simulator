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

    val jobA = Job(1, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobA.cpuUtilization = Array.fill(10)(0F)
    jobA.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 0.79F)
    workload.addJob(jobA)

    val jobB = Job(2, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobB.cpuUtilization = Array.fill(10)(0F)
    jobB.memoryUtilization = Array(0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F)
    workload.addJob(jobB)

    val jobC = Job(3, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobC.cpuUtilization = Array.fill(10)(0F)
    jobC.memoryUtilization = Array(0.49F, 0.39F, 0.29F, 0.04F, 0.29F, 0.39F, 0.49F, 0.04F, 0.39F, 0.04F)
    workload.addJob(jobC)

    val jobD = Job(4, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    jobD.cpuUtilization = Array.fill(10)(0F)
    jobD.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 0.79F)
    workload.addJob(jobD)

    workload
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
