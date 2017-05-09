package ClusterSchedulingSimulation.workloads

import ClusterSchedulingSimulation.core.{PrefillJobListsCache, Workload, WorkloadGenerator}

/**
  * Generates a pre-fill workload based on an input trace from a cluster.
  * Given a workloadName, it has hard coded rules to split up jobs
  * in the input file according to the PBB style split based on
  * the jobs' priority level and schedulingClass.
  */
class PrefillPbbTraceWorkloadGenerator(val workloadName: String,
                                       traceFileName: String)
  extends WorkloadGenerator {
  assert(workloadName.equals("PrefillBatch") || workloadName.equals("PrefillService") || workloadName.equals("PrefillBatchService"))

  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload = this.synchronized {
    assert(updatedAvgJobInterarrivalTime.isEmpty)
    assert(timeWindow >= 0)

    val joblist = PrefillJobListsCache.getJobs(workloadName,
      traceFileName,
      timeWindow)
    val workload = new Workload(workloadName)

    //TODO(andyk): Make this more functional.
    def reachedMaxCpu(currCpus: Double) = maxCpus.exists(currCpus >= _)

    def reachedMaxMem(currMem: Long) = maxMem.exists(currMem >= _)

    var iter = joblist.toIterator
    var numCpus = 0.0
    var numMem: Long = 0
    var counter = 0
    while (iter.hasNext) {
      counter += 1
      val nextJob = iter.next
      numCpus += nextJob.numTasks * nextJob.cpusPerTask.toDouble
      numMem += nextJob.numTasks * nextJob.memPerTask
      if (reachedMaxCpu(numCpus) || reachedMaxMem(numMem)) {
        logger.debug("reachedMaxCpu = " + reachedMaxCpu(numCpus) + ", reachedMaxMem = " + reachedMaxMem(numMem))
        iter = Iterator()
      } else {
        // Use copy to be sure we don't share state between runs of the
        // simulator.
        workload.addJob(nextJob.copyEverything())
        iter = iter.drop(0)
      }
    }
    logger.debug("Returning workload with " + workload.getJobs.map(j => {
      j.numTasks * j.cpusPerTask
    }).sum + " cpus and " +
      workload.getJobs.map(j => {
        j.numTasks * j.memPerTask
      }).sum + " mem in total.")
    workload
  }
}
