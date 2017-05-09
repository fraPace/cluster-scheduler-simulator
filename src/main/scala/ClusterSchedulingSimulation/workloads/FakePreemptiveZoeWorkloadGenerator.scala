package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{Job, Workload, WorkloadGenerator}

/**
  * Created by Nosfe on 16/04/2017.
  */
class FakePreemptiveZoeWorkloadGenerator(
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

    val jobA = Job(1, 0, 4, 20, workloadName, 100, 6871947673L, numCoreTasks = Option(6))
    workload.addJob(jobA)
    val jobB = Job(2, 0, 2, 25, workloadName, 100, 6871947673L, numCoreTasks = Option(6))
    workload.addJob(jobB)
    val jobC = Job(3, 0, 8, 12.5, workloadName, 100, 6871947673L, numCoreTasks = Option(8))
    workload.addJob(jobC)
    val jobD = Job(4, 5, 1, 7.5, workloadName, 100, 6871947673L, numCoreTasks = Option(19))
    workload.addJob(jobD)
    //    val jobE = Job(5,
    //      5,
    //      4,
    //      5,
    //      workloadName,
    //      0.1,
    //      6.4,
    //      isRigid = false,
    //      numMoldableTasks = Option(2)
    //    )
    //    workload.addJob(jobE)

    workload
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
