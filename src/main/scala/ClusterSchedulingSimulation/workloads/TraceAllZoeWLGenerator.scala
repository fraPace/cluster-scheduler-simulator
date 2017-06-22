package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core._
import ClusterSchedulingSimulation.utils.{Seed, UniqueIDGenerator}
import org.apache.commons.math3.distribution.LogNormalDistribution

/**
  * A thread-safe Workload factory. Generates workloads with jobs that have
  * numTasks, duration, and interarrival_time set by sampling from an empirical
  * distribution built from a tracefile containing the interarrival times of
  * jobs in a real cluster. Task shapes are drawn from empirical distributions
  * built from a prefill trace file. All tasks in a job are identical, so no
  * per-task data is required.
  *
  * In particular in this generator it is implemented the applications that are
  * used in Zoe. Thus a job is actually a Zoe Applications and the task are Zoe
  * Services.
  */
class TraceAllZoeWLGenerator(val workloadName: String,
                             interarrivalTraceFileName: String,
                             tasksPerJobTraceFileName: String,
                             jobDurationTraceFileName: String,
                             prefillTraceFileName: String,
                             cpuSlackTraceFileName: String,
                             memorySlackTraceFileName: String,
                             maxCpusPerTask: Long,
                             maxMemPerTask: Long,
                             jobsPerWorkload: Int = 10000,
                             scaleFactor: Int = 1,
                             allCore: Boolean = false,
                             introduceError: Boolean = false)
  extends WorkloadGenerator {
  logger.info("Generating " + workloadName + " Workload...")
  assert(workloadName.equals("Batch") || workloadName.equals("Service") || workloadName.equals("Interactive") || workloadName.equals("Batch-MPI"))

  val cpuSlackPerTaskDist: Array[Double] =
    DistCache.getDistribution(workloadName, cpuSlackTraceFileName)
  val memSlackPerTaskDist: Array[Double] =
    DistCache.getDistribution(workloadName, memorySlackTraceFileName)

  val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName, prefillTraceFileName)
  val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName, prefillTraceFileName)
  val randomNumberGenerator = new util.Random(Seed())
  /**
    * Build distributions
    */
  var interarrivalDist: Array[Double] =
    DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  var tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)
  var jobDurationDist: Array[Double] = if (!workloadName.equals("Interactive")) {
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  } else {
    DistCache.getDistribution(workloadName, "interactive-runtime-dist")
  }

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
    var nextJobSubmissionTime = DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
    var numJobs = 0
    // We will fill the workload with the number of job asked
    while (numJobs < jobsPerWorkload) {
      if (nextJobSubmissionTime < timeWindow) {
        var moldableTasks: Option[Int] = Some(randomNumberGenerator.nextInt(5))
        //        if(workloadName.equals("Interactive"))
        //          moldableTasks = randomNumberGenerator.nextInt(3)
        if (allCore)
          moldableTasks = None
        for (_ <- 1 to scaleFactor) {
          val job = newJob(nextJobSubmissionTime, moldableTasks, timeWindow)
          assert(job.workloadName == workload.name)
          workload.addJob(job)
        }
        numJobs += 1
      }
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      val newinterArrivalTime = updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat) * 100
      if (newinterArrivalTime + nextJobSubmissionTime < timeWindow * 0.1)
        nextJobSubmissionTime += newinterArrivalTime
      else {
        logger.warn("[" + workloadName + "] job submission going outside the time window. Resetting it.")
        nextJobSubmissionTime = DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
      }

    }
    //    assert(numJobs == workload.numJobs, "Num Jobs generated differs from what has been asked")
    workload.sortJobs()
    val lastJob = workload.getJobs.last
    logger.info("[" + workloadName + "] The last job arrives at " + lastJob.submitted + " and will finish at " + (lastJob.submitted + lastJob.jobDuration))

    workload
  }

  def newJob(submissionTime: Double, numMoldableTasks: Option[Int], timeWindow: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0 || dur >= timeWindow * 0.1) {
      dur = DistCache.getQuantile(jobDurationDist, randomNumberGenerator.nextFloat)
      if (!workloadName.equals("Interactive"))
        dur *= 30
    }

    // Use ceil to avoid jobs with 0 tasks.
    var numTasks = math.ceil(DistCache.getQuantile(tasksPerJobDist, randomNumberGenerator.nextFloat).toFloat).toInt
    assert(numTasks != 0, {
      "Jobs must have at least one task."
    })
    if (workloadName.equals("Batch"))
      numTasks *= 10
    if (workloadName.equals("Interactive"))
      numTasks -= 1

    // Sample from the empirical distribution until we get task
    // cpu and mem sizes that are small enough.
    var cpusPerTask: Long = (0.7 * DistCache.getQuantile(cpusPerTaskDist, randomNumberGenerator.nextFloat)).toLong
    while (cpusPerTask.isNaN || cpusPerTask >= maxCpusPerTask || cpusPerTask == 0) {
      cpusPerTask = (0.7 * DistCache.getQuantile(cpusPerTaskDist, randomNumberGenerator.nextFloat)).toLong
    }
    var memPerTask = (0.7 * DistCache.getQuantile(memPerTaskDist, randomNumberGenerator.nextFloat)).toLong
    while (memPerTask.isNaN || memPerTask >= maxMemPerTask || memPerTask == 0) {
      memPerTask = (0.7 * DistCache.getQuantile(memPerTaskDist, randomNumberGenerator.nextFloat)).toLong
    }


    logger.debug("New Job with " + cpusPerTask + " cpusPerTask and " + memPerTask + " memPerTask")
    val newJob: Job = Job(UniqueIDGenerator.generateUniqueID,
      submissionTime, numTasks, dur, workloadName, cpusPerTask, memPerTask,
      numCoreTasks = numMoldableTasks, priority = randomNumberGenerator.nextInt(1000),
      error = generateError())

    val arraySize: Int = newJob.jobDuration.toInt

    val cpuUtilization = new Array[Int](arraySize)
    val memUtilization = new Array[Long](arraySize)
    for (i <- List.range(0, arraySize)) {
      var cpuQuantile: Double = DistCache.getQuantile(cpuSlackPerTaskDist, randomNumberGenerator.nextFloat)
      while (cpuQuantile > 1.0) {
        cpuQuantile = DistCache.getQuantile(cpuSlackPerTaskDist, randomNumberGenerator.nextFloat)
      }

      var memQuantile: Double = DistCache.getQuantile(cpuSlackPerTaskDist, randomNumberGenerator.nextFloat)
      while (memQuantile > 1.0) {
        memQuantile = DistCache.getQuantile(memSlackPerTaskDist, randomNumberGenerator.nextFloat)
      }

      val _cpu: Int = (cpuQuantile * cpusPerTask).toInt
      val _mem: Long = (memQuantile * memPerTask).toLong

      assert(_cpu.toDouble <= cpusPerTask, {
        "CPU Utilization (" + _cpu + ") is higher than allocated (" + cpusPerTask + ")."
      })
      assert(_mem <= memPerTask, {
        "Memory Utilization (" + _mem + ") is higher than allocated (" + memPerTask + ")."
      })

      cpuUtilization(i) = _cpu
      memUtilization(i) = _mem
    }
    newJob.cpuUtilization = cpuUtilization
    newJob.memoryUtilization = memUtilization

    newJob
  }

  def generateError(mu: Double = 0, sigma: Double = 0.5, factor: Double = 1): Double = {
    var error: Double = 1
    if (introduceError) {
      val logNormal: LogNormalDistribution = new LogNormalDistribution(mu, sigma)
      error = factor * logNormal.sample()
    }
    error

    //    val randomNumberGenerator = util.Random
    //    var error: Double = 1 - (randomNumberGenerator.nextDouble() * (randomNumberGenerator.nextInt(2) - 1))
    //    while(error <= 0 && error >= 2)
    //      error = 1 - (randomNumberGenerator.nextDouble() * (randomNumberGenerator.nextInt(2) - 1))
    //    error
  }

  logger.info("Done generating " + workloadName + " Workload.\n")
}
