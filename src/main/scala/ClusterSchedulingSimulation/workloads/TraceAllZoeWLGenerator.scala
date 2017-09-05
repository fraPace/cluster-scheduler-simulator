package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core._
import ClusterSchedulingSimulation.utils.{Seed, UniqueIDGenerator}
import breeze.linalg._
import breeze.plot._
import breeze.signal.OptOverhang
import breeze.stats.distributions.{Bernoulli, Gaussian, RandBasis}
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
  assert(workloadName.equals("Batch") || workloadName.equals("Service") || workloadName.equals("Interactive") || workloadName.equals("Batch-MPI"))

  lazy val cpuSlackPerTaskDist: Array[Double] =
    DistCache.getDistribution(workloadName, cpuSlackTraceFileName)
  lazy val memSlackPerTaskDist: Array[Double] =
    DistCache.getDistribution(workloadName, memorySlackTraceFileName)

  lazy val cpusPerTaskDist: Array[Double] =
    PrefillJobListsCache.getCpusPerTaskDistribution(workloadName, prefillTraceFileName)
  lazy val memPerTaskDist: Array[Double] =
    PrefillJobListsCache.getMemPerTaskDistribution(workloadName, prefillTraceFileName)
  lazy val randomNumberGenerator = new util.Random(Seed())
  /**
    * Build distributions
    */
  lazy val interarrivalDist: Array[Double] =
    DistCache.getDistribution(workloadName, interarrivalTraceFileName)
  lazy val tasksPerJobDist: Array[Double] =
    DistCache.getDistribution(workloadName, tasksPerJobTraceFileName)
  lazy val jobDurationDist: Array[Double] = if (!workloadName.equals("Interactive")) {
    DistCache.getDistribution(workloadName, jobDurationTraceFileName)
  } else {
    DistCache.getDistribution(workloadName, "interactive-runtime-dist")
  }

  lazy val normalDistribution: Gaussian = new Gaussian(0, 1)(RandBasis.withSeed(0))
  lazy val bernoulliDistribution: Bernoulli = new  Bernoulli(0.01)(RandBasis.withSeed(0))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    logger.info("Generating " + workloadName + " Workload...")

    assert(timeWindow >= 0)
    assert(maxCpus.isEmpty)
    assert(maxMem.isEmpty)

    val _timeWindow = timeWindow * 0.1
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
      while(nextJobSubmissionTime >= _timeWindow){
        logger.warn("[" + workloadName + "] job submission going outside the time window. Resetting it.")
        nextJobSubmissionTime = DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat)
      }
      var moldableTasks: Option[Int] = Some(randomNumberGenerator.nextInt(5))
      //        if(workloadName.equals("Interactive"))
      //          moldableTasks = randomNumberGenerator.nextInt(3)
      if (allCore)
        moldableTasks = None
      for (_ <- 1 to scaleFactor) {
        val job = newJob(nextJobSubmissionTime, moldableTasks, _timeWindow)
        assert(job.workloadName == workload.name)
        workload.addJob(job)
      }
      numJobs += scaleFactor
      // For this type of WorkloadGenerator in which interarrival rate is
      // sampled from an empirical distribution, the
      // updatedAvgJobInterarrivalTime parameter represents a scaling factor
      // for the value sampled from the distribution.
      nextJobSubmissionTime += updatedAvgJobInterarrivalTime.getOrElse(1.0) *
        DistCache.getQuantile(interarrivalDist, randomNumberGenerator.nextFloat) * 100
    }
    //    assert(numJobs == workload.numJobs, "Num Jobs generated differs from what has been asked")
    workload.sortJobs()
    val lastJob = workload.getJobs.last
    logger.info("The last job arrives at " + lastJob.submitted + " and will finish at " + (lastJob.submitted + lastJob.jobDuration))

    logger.info("Done generating " + workloadName + " Workload.\n")

    workload
  }

  def newJob(submissionTime: Double, numMoldableTasks: Option[Int], timeWindow: Double): Job = {
    // Don't allow jobs with duration 0.
    var dur = 0.0
    while (dur <= 0.0 || dur >= timeWindow) {
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


    logger.debug("New Job with " + cpusPerTask + " cpusPerTask and " + memPerTask + " memPerTask and lasting " + dur + " seconds")
    val newJob: Job = Job(UniqueIDGenerator.generateUniqueID,
      submissionTime, numTasks, dur, workloadName, cpusPerTask, memPerTask,
      numCoreTasks = numMoldableTasks, priority = randomNumberGenerator.nextInt(1000),
      error = generateError())

    val arraySize: Int = newJob.jobDuration.toInt
    var cpuMean: Double = DistCache.getQuantile(cpuSlackPerTaskDist, randomNumberGenerator.nextFloat)
    while (cpuMean > 1.0 || cpuMean <= 0.0) {
      cpuMean = DistCache.getQuantile(cpuSlackPerTaskDist, randomNumberGenerator.nextFloat)
    }
    var memMean: Double = DistCache.getQuantile(memSlackPerTaskDist, randomNumberGenerator.nextFloat)
    while (memMean > 1.0 || memMean <= 0.0) {
      memMean = DistCache.getQuantile(memSlackPerTaskDist, randomNumberGenerator.nextFloat)
    }
    newJob.cpuUtilization = generateResourceUtilization(cpuMean, arraySize)
    newJob.memoryUtilization = generateResourceUtilization(memMean, arraySize)

    newJob
  }

  def generateResourceUtilization(targetMean: Double, length: Int): Array[Float] = {
    def distValue: Float = {
      normalDistribution.sample().toFloat * bernoulliDistribution.sample().compareTo(false)
    }
    // Drawn values from distribution.
    // We create an array of size - 1 to compensate for the CumSUM (that will add one value)
    var resourceUtilization = Array.fill(length - 1)(distValue)
    // Apply CumSUM.
    resourceUtilization = resourceUtilization.scanLeft(0F)(_+_)
    // Apply convolution for smoothing
    val convolveWindow = 10
    resourceUtilization = breeze.signal.convolve(
      DenseVector(resourceUtilization),
      DenseVector(Array.fill(convolveWindow)(1 / convolveWindow.toFloat)),
      overhang = OptOverhang.PreserveLength).toArray

    // Normalize the curve so that the results are in the interval [-1, 1]
    val max = resourceUtilization.max
    var min = resourceUtilization.min
    val span = (max - min).toDouble
    var i = 0
    while(i < resourceUtilization.length){
      resourceUtilization(i) = (resourceUtilization(i) / span).toFloat
      i += 1
    }

    // Shift the curve so that we have values in the interval [0, 1]
    min = resourceUtilization.min
    val y_offset = if(min < 0) -min else 0
    i = 0
    while(i < resourceUtilization.length){
      resourceUtilization(i) = resourceUtilization(i) + y_offset
      i += 1
    }

    // Adjust the curve so that it meets the desired mean
    var mean = breeze.stats.mean(resourceUtilization)
    val _targetMean = Math.round(targetMean.toFloat * 1000)
    var steps = 0
    val maxSteps = 10000
    while(Math.abs(_targetMean - Math.round(mean * 1000)) > 1 && steps < maxSteps){
      val meanRatio = targetMean / mean
      i = 0
      while(i < resourceUtilization.length){
        val v = (resourceUtilization(i) * meanRatio).toFloat
        resourceUtilization(i) = if(v > 1F) 1F else if(v < 0F) 0F else v
        i += 1
      }
      // We prevent infinity loops by exiting if we could not improve the mean at this step
      val tmp_mean = breeze.stats.mean(resourceUtilization)
      if(tmp_mean == mean)
        steps = maxSteps
      mean = tmp_mean
      steps += 1
    }

    if(Math.abs(_targetMean - Math.round(mean * 1000)) > 1){
      logger.warn("Curve cannot converge to desired mean, trying with a new one. Target Mean: " + targetMean + " Current Mean: " + mean)
      resourceUtilization = generateResourceUtilization(targetMean, length)
    }

    resourceUtilization
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
}
