package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{DistCache, Job, Workload, WorkloadGenerator}
import ClusterSchedulingSimulation.utils.{Seed, UniqueIDGenerator}
import org.apache.commons.math.distribution.ExponentialDistributionImpl

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from exponential distributions.
  * All tasks in a job are identical, so no per-task data is required.
  */
class TraceWLGenerator(val workloadName: String,
                       interarrivalTraceFileName: String,
                       tasksPerJobTraceFileName: String,
                       jobDurationTraceFileName: String,
                       avgCpusPerTask: Long,
                       avgMemPerTask: Long,
                       maxCpusPerTask: Long,
                       maxMemPerTask: Long)
  extends WorkloadGenerator {
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  val cpusPerTaskGenerator =
    new ExponentialDistributionImpl(avgCpusPerTask)
  val memPerTaskGenerator = new ExponentialDistributionImpl(avgMemPerTask)
  val randomNumberGenerator = new util.Random(Seed())
  // Build the distributions from the input trace textfile that we'll
  // use to generate random job interarrival times.
  var interarrivalDist: Array[Double] =
  DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)
  var jobDurationDist: Array[Double] =
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)
    // Reset the randomNumberGenerator using the global seed so that
    // the same workload will be generated each time newWorkload is
    // called with the same parameters. This will ensure that
    // Experiments run in different threads will get the same
    // workloads and be a bit more fair to compare to each other.
    randomNumberGenerator.setSeed(Seed())
    val workload = new Workload(workloadName)
    // create a new list of jobs for the experiment runTime window
    // using the current WorkloadGenerator.
    var nextJobSubmissionTime = DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (nextJobSubmissionTime < timeWindow) {
      val job = newJob(nextJobSubmissionTime)
      assert(job.workloadName == workload.name)
      workload.addJob(job)
      nextJobSubmissionTime += DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
      numJobs += 1
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with zero tasks.
    var dur = 0.0
    while (dur <= 0.0)
      dur = DistCache.getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks =
      math.ceil(DistCache.getQuantile(tasksPerJobDist, randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, {
      "Jobs must have at least one task."
    })
    // Sample until we get task cpu and mem sizes that are small enough.
    var cpusPerTask = cpusPerTaskGenerator.sample()
    while (cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = cpusPerTaskGenerator.sample()
    }
    var memPerTask = memPerTaskGenerator.sample()
    while (memPerTask >= maxMemPerTask) {
      memPerTask = memPerTaskGenerator.sample()
    }
    Job(UniqueIDGenerator.generateUniqueID, submissionTime, numTasks, dur, workloadName, cpusPerTask.toLong, memPerTask.toLong)
  }
}
