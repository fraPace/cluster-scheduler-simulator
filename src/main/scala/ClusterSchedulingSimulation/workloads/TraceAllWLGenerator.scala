package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core._
import ClusterSchedulingSimulation.utils.{Seed, UniqueIDGenerator}

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from empirical distributions
  * built from a prefill trace file. All tasks in a job are identical, so no
  * per-task data is required.
  */
class TraceAllWLGenerator(val workloadName: String,
                          interarrivalTraceFileName: String,
                          tasksPerJobTraceFileName: String,
                          jobDurationTraceFileName: String,
                          prefillTraceFileName: String,
                          maxCpusPerTask: Long,
                          maxMemPerTask: Long,
                          maxJobsPerWorkload: Int = 10000)
  extends WorkloadGenerator {
  logger.info("Generating " + workloadName + " Workload...")
  assert(workloadName.equals("Batch") || workloadName.equals("Service"))
  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName,
      prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName,
      prefillTraceFileName)

  //  var tasksPerJobDist: Array[Double] =
  //      (1 until Workloads.maxTasksPerJob).toArray.map(_.toDouble)
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
    var nextJobSubmissionTime = DistCache.getQuantile(interarrivalDist,
      randomNumberGenerator.nextFloat)
    var numJobs = 0
    while (numJobs < maxJobsPerWorkload) {
      if (nextJobSubmissionTime < timeWindow) {
        val job = newJob(nextJobSubmissionTime)
        assert(job.workloadName == workload.name)
        workload.addJob(job)
        numJobs += 1
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        DistCache.getQuantile(interarrivalDist,
          randomNumberGenerator.nextFloat)
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow)
        nextJobSubmissionTime += newinterArrivalTime
    }
    assert(numJobs == workload.numJobs)
    workload
  }

  def newJob(submissionTime: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0)
      dur = DistCache.getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
    // Use ceil to avoid jobs with 0 tasks.
    val numTasks = math.ceil(DistCache.getQuantile(tasksPerJobDist,
      randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, {
      "Jobs must have at least one task."
    })
    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask: Long = (0.7 * DistCache.getQuantile(cpusPerTaskDist,
      randomNumberGenerator.nextFloat)).toLong
    while (cpusPerTask.isNaN || cpusPerTask >= maxCpusPerTask) {
      cpusPerTask = (0.7 * DistCache.getQuantile(cpusPerTaskDist,
        randomNumberGenerator.nextFloat)).toLong
    }
    // The memory in the distribution is in GB, we need it in bytes
    var memPerTask = (0.7 * DistCache.getQuantile(memPerTaskDist,
      randomNumberGenerator.nextFloat)).toLong
    while (memPerTask.isNaN || memPerTask >= maxMemPerTask) {
      memPerTask = (0.7 * DistCache.getQuantile(memPerTaskDist,
        randomNumberGenerator.nextFloat)).toLong
    }
    logger.debug("New Job with " + cpusPerTask + " cpusPerTask and " + memPerTask + " memPerTask")
    Job(UniqueIDGenerator.generateUniqueID, submissionTime, numTasks, dur, workloadName, cpusPerTask, memPerTask)
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
