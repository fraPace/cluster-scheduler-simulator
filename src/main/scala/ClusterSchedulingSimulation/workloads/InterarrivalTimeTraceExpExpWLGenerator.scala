package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{DistCache, Job, Workload, WorkloadGenerator}
import ClusterSchedulingSimulation.utils.{Seed, UniqueIDGenerator}
import org.apache.commons.math.distribution.ExponentialDistributionImpl

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * sizes and lengths sampled from exponential distributions. Assumes that
  * all tasks in a job are identical, so no per-task data is required.
  * Generates interarrival times by sampling from an empirical distribution
  * built from a tracefile containing the interarrival times of jobs
  * in a real cluster.
  */
class InterarrivalTimeTraceExpExpWLGenerator(val workloadName: String,
                                             traceFileName: String,
                                             avgTasksPerJob: Double,
                                             avgJobDuration: Double,
                                             avgCpusPerTask: Long,
                                             avgMemPerTask: Long,
                                             maxCpusPerTask: Long,
                                             maxMemPerTask: Long)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  // Build the distribution from the input trace textfile that we'll
  // use to generate random job interarrival times.
  val interarrivalTimes = new collection.mutable.ListBuffer[Double]()
  val numTasksGenerator =
    new ExponentialDistributionImpl(avgTasksPerJob.toFloat)
  val durationGenerator = new ExponentialDistributionImpl(avgJobDuration)
  val cpusPerTaskGenerator =
    new ExponentialDistributionImpl(avgCpusPerTask)
  val memPerTaskGenerator = new ExponentialDistributionImpl(avgMemPerTask)
  val randomNumberGenerator = new util.Random(Seed())
  var refDistribution: Array[Double] =
    DistCache.getDistribution(workloadName, traceFileName)

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = DistCache.getQuantile(refDistribution, randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += DistCache.getQuantile(refDistribution, randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = durationGenerator.sample()
    while (dur <= 0.0)
      dur = durationGenerator.sample()
    // Sample until we get task cpu and mem sizes that are small enough.
    var cpusPerTask = cpusPerTaskGenerator.sample()
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = cpusPerTaskGenerator.sample()
    }
    var memPerTask = memPerTaskGenerator.sample()
    while (memPerTask >= maxMemPerTask) {
      memPerTask = memPerTaskGenerator.sample()
    }
    Job(UniqueIDGenerator.generateUniqueID, submissionTime, math.ceil(numTasksGenerator.sample().toFloat).toInt, dur, workloadName, cpusPerTask.toLong, memPerTask.toLong)
  }
}
