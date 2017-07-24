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

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    logger.info("Generating " + workloadName + " Workload...")

    assert(timeWindow >= 0)
    val workload = new Workload(workloadName)

    var job = Job(1, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    job.cpuUtilization = Array.fill(10)(0F)
    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 0.79F)
//    jobA.memoryUtilization = Array.fill(10)(1.0F)
    workload.addJob(job)

    job = Job(2, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    job.cpuUtilization = Array.fill(10)(0F)
    job.memoryUtilization = Array(0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F)
//    jobB.memoryUtilization = Array.fill(10)(1.0F)
    workload.addJob(job)

    job = Job(3, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    job.cpuUtilization = Array.fill(10)(0F)
    job.memoryUtilization = Array(0.49F, 0.39F, 0.29F, 0.04F, 0.29F, 0.39F, 0.49F, 0.04F, 0.39F, 0.04F)
//    jobC.memoryUtilization = Array.fill(10)(1.0F)
    workload.addJob(job)

    job = Job(4, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
    job.cpuUtilization = Array.fill(10)(0F)
    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 0.79F)
//    jobD.memoryUtilization = Array.fill(10)(1.0F)
    workload.addJob(job)



//    job = Job(5, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 1F) // 0.79F)
//    //    jobA.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(6, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 1F) // 0.09F)
//    //    jobB.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(7, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.49F, 0.39F, 0.29F, 0.04F, 0.29F, 0.39F, 0.49F, 0.04F, 0.39F, 1F) // 0.04F)
//    //    jobC.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(8, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 1F) // 0.79F)
//    //    jobD.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//
//
//    job = Job(9, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 1F) // 0.79F)
//    //    jobA.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(10, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 0.09F, 1F) // 0.09F)
//    //    jobB.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(11, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.49F, 0.39F, 0.29F, 0.04F, 0.29F, 0.39F, 0.49F, 0.04F, 0.39F, 1F) // 0.04F)
//    //    jobC.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)
//
//    job = Job(12, 0, 7, 10, workloadName, 100, 13743895347L, numCoreTasks = Option(3))
//    job.cpuUtilization = Array.fill(10)(0F)
//    job.memoryUtilization = Array(0.19F, 0.39F, 0.59F, 0.79F, 0.59F, 0.39F, 0.19F, 0.79F, 0.59F, 1F) // 0.79F)
//    //    jobD.memoryUtilization = Array.fill(10)(1.0F)
//    workload.addJob(job)

    logger.info("Done generating " + workloadName + " Workload.\n")

    workload
  }
}
