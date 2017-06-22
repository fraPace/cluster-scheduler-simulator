package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{Job, Workload, WorkloadGenerator}
import ClusterSchedulingSimulation.utils.UniqueIDGenerator
import org.apache.commons.math.distribution.ExponentialDistributionImpl

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * interarrival rates, numTasks, and lengths sampled from exponential
  * distributions. Assumes that all tasks in a job are identical
  * (and so no per-task data is required).
  *
  * @param workloadName               the name that will be assigned to Workloads produced by
  *                                   this factory, and to the tasks they contain.
  * @param initAvgJobInterarrivalTime initial average inter-arrival time in
  *                                   seconds. This can be overridden by passing
  *                                   a non None value for
  *                                   updatedAvgJobInterarrivalTime to
  *                                   newWorkload().
  */
class ExpExpExpWorkloadGenerator(val workloadName: String,
                                 initAvgJobInterarrivalTime: Double,
                                 avgTasksPerJob: Double,
                                 avgJobDuration: Double,
                                 avgCpusPerTask: Long,
                                 avgMemPerTask: Long)
  extends WorkloadGenerator {
  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)

  /**
    * Synchronized so that Experiments, which can share this WorkloadGenerator,
    * can safely call newWorkload concurrently.
    */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    assert(timeWindow >= 0)
    // Create the job-interarrival-time number generator using the
    // parameter passed in, if any, else use the default parameter.
    val avgJobInterarrivalTime =
    updatedAvgJobInterarrivalTime.getOrElse(initAvgJobInterarrivalTime)
    val interarrivalTimeGenerator =
      new ExponentialDistributionImpl(avgJobInterarrivalTime)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = interarrivalTimeGenerator.sample()
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += interarrivalTimeGenerator.sample()
    }
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    Job(UniqueIDGenerator.generateUniqueID, submissionTime, math.ceil(numTasksGenerator.sample().toFloat).toInt, dur, workloadName, avgCpusPerTask, avgMemPerTask)
  }
}
