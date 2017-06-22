package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core._
import ClusterSchedulingSimulation.utils.UniqueIDGenerator

/**
  * Generates jobs at a uniform rate, of a uniform size.
  */
class UniformZoeWorkloadGenerator(
                                   val workloadName: String,
                                   initJobInterarrivalTime: Double,
                                   tasksPerJob: Int,
                                   jobDuration: Double,
                                   cpusPerTask: Long,
                                   memPerTask: Long,
                                   numMoldableTasks: Integer = null,
                                   jobsPerWorkload: Int = 10000,
                                   isRigid: Boolean = false)
  extends WorkloadGenerator {
  logger.info("Generating " + workloadName + " Workload...")

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None): Workload = this.synchronized {
    assert(timeWindow >= 0)
    val jobInterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) * initJobInterarrivalTime
    val workload = new Workload(workloadName)
    var nextJobSubmissionTime = 0.0
    var numJobs = 0
    while (numJobs < jobsPerWorkload && nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime, numJobs)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      numJobs += 1
      nextJobSubmissionTime += jobInterArrivalTime
    }

    if (numJobs < jobsPerWorkload)
      logger.warn("The number of job generated for workload " + workload.name + " is lower (" + numJobs + ") than asked (" +
        jobsPerWorkload + "). The time windows for the simulation is not large enough. Job inter-arrival time was set to " + jobInterArrivalTime + ".")
    workload
  }

  def newJob(submissionTime: Double, priority: Int): Job = {
    Job(UniqueIDGenerator.generateUniqueID, submissionTime, tasksPerJob, jobDuration, workloadName, cpusPerTask, memPerTask, isRigid = isRigid, numCoreTasks = Some(numMoldableTasks), priority = priority)
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
