/**
  * Copyright (c) 2016 Eurecom
  * All rights reserved.
  **
  *Redistribution and use in source and binary forms, with or without
  *modification, are permitted provided that the following conditions are met:
  **
  *Redistributions of source code must retain the above copyright notice, this
  *list of conditions and the following disclaimer. Redistributions in binary
  *form must reproduce the above copyright notice, this list of conditions and the
  *following disclaimer in the documentation and/or other materials provided with
  *the distribution.
  **
  *Neither the name of Eurecom nor the names of its contributors may be used to
  *endorse or promote products derived from this software without specific prior
  *written permission.
  **
  *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  *ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  *WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  *DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  *FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  *DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  *SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  *CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  *OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  *OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package ClusterSchedulingSimulation.schedulers

import ClusterSchedulingSimulation.{CellState, Scheduler, _}
import org.apache.log4j.Logger
import sun.font.TrueTypeFont

import scala.collection.mutable.ListBuffer
import scala.collection.{Iterator, mutable}

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class ZoeSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                       runTime: Double,
                       val allocationMode: AllocationModes.Value,
                       val policyMode: PolicyModes.Value)
  extends ClusterSimulatorDesc(runTime){
  override
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator = {
    val schedulers = mutable.HashMap[String, Scheduler]()
    // Create schedulers according to experiment parameters.
    schedulerDescs.foreach(schedDesc => {
      // If any of the scheduler-workload pairs we're sweeping over
      // are for this scheduler, then apply them before
      // registering it.
      val constantThinkTimes = mutable.HashMap[String, Double](
        schedDesc.constantThinkTimes.toSeq: _*)
      val perTaskThinkTimes = mutable.HashMap[String, Double](
        schedDesc.perTaskThinkTimes.toSeq: _*)
      var newBlackListPercent = 0.0
      if (schedulerWorkloadsToSweepOver
        .contains(schedDesc.name)) {
        newBlackListPercent = blackListPercent
        schedulerWorkloadsToSweepOver(schedDesc.name)
          .foreach(workloadName => {
            constantThinkTimes(workloadName) = constantThinkTime
            perTaskThinkTimes(workloadName) = perTaskThinkTime
          })
      }
      schedulers(schedDesc.name) =
        new ZoeScheduler(schedDesc.name,
          constantThinkTimes.toMap,
          perTaskThinkTimes.toMap,
          math.floor(newBlackListPercent *
            cellStateDesc.numMachines.toDouble).toInt,
          allocationMode,
          policyMode)
    })

    val cellState = new CellState(cellStateDesc.numMachines,
      cellStateDesc.cpusPerMachine,
      cellStateDesc.memPerMachine,
      conflictMode = "resource-fit",
      transactionMode = "all-or-nothing")




    new ClusterSimulator(cellState,
      schedulers.toMap,
      workloadToSchedulerMap,
      workloads,
      prefillWorkloads,
      logging,
      prefillScheduler = new ZoePrefillScheduler(cellState))
  }
}

class ZoePrefillScheduler(cellState: CellState)
  extends PrefillScheduler(cellState = cellState) {

  override
  def scheduleWorkloads(workloads: Seq[Workload]): Unit ={
    // Prefill jobs that exist at the beginning of the simulation.
    // Setting these up is similar to loading jobs that are part
    // of the simulation run; they need to be scheduled onto machines
    simulator.logger.info("Prefilling cell-state with %d workloads."
      .format(workloads.length))
    workloads.foreach(workload => {
      simulator.logger.info("Prefilling cell-state with %d jobs from workload %s."
        .format(workload.numJobs, workload.name))
      //var i = 0
      workload.getJobs.foreach(job => {
        //i += 1
        // println("Prefilling %d %s job id - %d."
        //         .format(i, workload.name, job.id))
        if (job.cpusPerTask > cellState.cpusPerMachine ||
          job.memPerTask > cellState.memPerMachine) {
          simulator.logger.warn(("IGNORING A JOB REQUIRING %f CPU & %f MEM PER TASK " +
            "BECAUSE machines only have %f cpu / %f mem.")
            .format(job.cpusPerTask, job.memPerTask,
              cellState.cpusPerMachine, cellState.memPerMachine))
        } else if((job.cpusPerTask * job.numTasks) > (cellState.cpusPerMachine * cellState.numMachines) ||
          (job.memPerTask * job.numTasks) > (cellState.memPerMachine * cellState.numMachines)){
          simulator.logger.warn(("IGNORING A JOB REQUIRING %f CPU & %f MEM PER %d TASK " +
            "BECAUSE THE TOTAL RESOURCES ARE NOT ENOUGH")
            .format(job.cpusPerTask, job.memPerTask, job.numTasks))
        } else {
          val claimDeltas = scheduleJob(job, cellState)
          // assert(job.numTasks == claimDeltas.length,
          //        "Prefill job failed to schedule.")
          cellState.scheduleEndEvents(claimDeltas)

          simulator.logger.info(("After prefill, common cell state now has %.2f%% (%.2f) " +
            "cpus and %.2f%% (%.2f) mem occupied.")
            .format(cellState.totalOccupiedCpus / cellState.totalCpus * 100.0,
              cellState.totalOccupiedCpus,
              cellState.totalOccupiedMem / cellState.totalMem * 100.0,
              cellState.totalOccupiedMem))
        }
      })
    })
  }
}

class ZoeScheduler(name: String,
                   constantThinkTimes: Map[String, Double],
                   perTaskThinkTimes: Map[String, Double],
                   numMachinesToBlackList: Double = 0,
                   allocationMode: AllocationModes.Value,
                   policyMode: PolicyModes.Value)
  extends Scheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode) {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.debug("scheduler-id-info: %d, %s, %d, %s, %s"
    .format(Thread.currentThread().getId,
      name,
      hashCode(),
      constantThinkTimes.mkString(";"),
      perTaskThinkTimes.mkString(";")))

  var pendingQueueAsList = new ListBuffer[Job]()
  override def jobQueueSize = pendingQueueAsList.count(_ != null)
//  var pendingQueueIterator = pendingQueueAsList.iterator
  var numJobsInQueue: Int = 0

  var runningQueueAsList = new ListBuffer[Job]()
  override def runningJobQueueSize: Long = runningQueueAsList.count(_ != null)

  var numRunningJobs: Int = 0

  //  var moldableQueueLength: Int = 0

  //  var elasticPendingQueue = new collection.mutable.ListBuffer[Job]()
  //  var elasticQueueIterator = elasticPendingQueue.iterator
  //  var numElasticJobsInQueue: Int = 0
  //  var schedulingElastic: Boolean = false
  //  var interruptElasticScheduling: Boolean = false
  //  var numElasticJobsSeen: Int = 0
//  var numJobsSeen: Int = 0
  var jobAttempt: Int = 0

  var privateCellState: CellState = _

  val schedulerPrefix = "[%s]".format(name)

  var previousJob: Job = _

  def getJobs(queue: ListBuffer[Job], currentTime:Double): ListBuffer[Job] ={
    val jobs = PolicyModes.getJobsWithSamePriority(queue, policyMode, currentTime)
    jobs.foreach(job => {
      removePendingJob(job)
    })
    jobs
  }

  /**
    * This function gives the next job in the queue
    *
    * @param iterator the iterator of the queue
    * @return  A tuple with:
    *          the next job in the queue or null if the end of the queue has been reached
    *          a counter with the number of elements skipped
    */
  def getNextJobInQueue(iterator: Iterator[Job]): (Job, Long) ={
    var elementsSkipped: Long = 0
    while(iterator.hasNext){
      val result = iterator.next()
      if(result != null)
        return (result, elementsSkipped)
      elementsSkipped += 1
    }
    (null, elementsSkipped)
  }

  def removePendingJob(job: Job): Unit = {
    val idx = pendingQueueAsList.indexOf(job)
    if(idx != -1 && pendingQueueAsList(idx) != null){
      numJobsInQueue -= 1
      pendingQueueAsList(idx) = null
    }

    //    pendingQueueAsList -= job
    //    moldableQueueIterator = pendingQueueAsList.iterator
    //    for (i <- 0 until currentIndex)
    //        moldableQueueIterator.next()
  }

  def removeRunningJob(job: Job): Unit = {
    val idx = runningQueueAsList.indexOf(job)
    if (idx != -1 && runningQueueAsList(idx) != null) {
      numRunningJobs -= 1
      runningQueueAsList(idx) = null
    }
  }


  //  def removeElasticJob(job: Job): Unit = {
  //    val idx = elasticPendingQueue.indexOf(job)
  //    if(idx != -1 && elasticPendingQueue(idx) != null){
  //      numElasticJobsInQueue -= 1
  //      elasticPendingQueue(idx) = null
  //    }
  ////    elasticPendingQueue -= job
  ////    elasticQueueIterator = elasticPendingQueue.iterator
  ////    for (i <- 0 until currentIndex)
  ////      elasticQueueIterator.next()
  //  }

  //  def addElasticJob(job: Job): Unit = {
  //    numElasticJobsInQueue += 1
  //    elasticPendingQueue += job
  ////    elasticPendingQueue = applyPolicy(elasticPendingQueue)
  //    elasticQueueIterator = elasticPendingQueue.iterator
  //  }

  def addPendingJob(job: Job): Unit = {
    numJobsInQueue += 1
    pendingQueueAsList += job
    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    //    moldableQueueIterator = pendingQueueAsList.iterator
  }

  def addRunningJob(job: Job): Unit = {
    numRunningJobs += 1
    runningQueueAsList += job
    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    //    moldableQueueIterator = pendingQueueAsList.iterator
  }


  def isAllocationSuccessfully(claimDeltas: Seq[ClaimDelta], job: Job): Boolean = {
    allocationMode match {
      case AllocationModes.Incremental => claimDeltas.nonEmpty
      case AllocationModes.All => claimDeltas.size == job.moldableTasks
      case _ => false
    }
  }

  def syncCellState() {
    privateCellState = simulator.cellState.copy
    simulator.logger.debug(schedulerPrefix + " Scheduler %s (%d) has new private cell state %d"
      .format(name, hashCode, privateCellState.hashCode))
  }

  override
  def wakeUp(): Unit = {
    simulator.logger.trace("wakeUp method called.")
//    simulator.logger.warn("%f - Jobs in Queue: %d | Jobs Running: %d ".format(simulator.currentTime, numJobsInQueue, numRunningJobs))
    //    pendingQueueAsList = applyPolicy(pendingQueueAsList)
    // This is necessary when we order the list, because the job can be put at the begin on it
    // Caused by the async nature of this call
    //    moldableQueueIterator = pendingQueueAsList.iterator

    //    interruptElasticScheduling = true
    //    if(policyMode == PolicyModes.Fifo || policyMode == PolicyModes.PriorityFifo)
    //      numJobsSeen = 0

    scheduleNextJob()
  }

  var firstTime = true
  override
  def addJob(job: Job) = {
    simulator.logger.info(schedulerPrefix + " Enqueued job %d of workload type %s."
      .format(job.id, job.workloadName))
    super.addJob(job)

    addPendingJob(job)
//    simulator.logger.warn("%f - Added a new Job (%s) in the queue. Num Tasks: %d (%d/%d) | Cpus: %f | Mem: %f | Job Runtime: %f"
//      .format(simulator.currentTime, job.workloadName, job.numTasks, job.moldableTasks, job.elasticTasks, job.cpusPerTask, job.memPerTask, job.jobDuration ))
    if(firstTime){
      if (numJobsInQueue == 3){
        wakeUp()
        firstTime = false
      }
    }else
      wakeUp()
  }

  /**
    * Checks to see if there is currently a job in this scheduler's job queue.
    * If there is, and this scheduler is not currently scheduling a job, then
    * pop that job off of the queue and "begin scheduling it". Scheduling a
    * job consists of setting this scheduler's state to scheduling = true, and
    * adding a finishSchedulingJobAction to the simulators event queue by
    * calling afterDelay().
    */
  def scheduleNextJob(): Unit = {
    if (!scheduling) {
      val jobCompleted: ListBuffer[Job] = new ListBuffer[Job]()
      pendingQueueAsList.foreach(job => {
        if (job != null && job.finalStatus == JobStates.Completed){
          jobCompleted += job
        }
      })
      jobCompleted.foreach(job => {
        // Remove the Job from the pending queue and running queue, if it was present.
        removePendingJob(job)
        removeRunningJob(job)
      })
      if (numJobsInQueue > 0) {
        scheduling = true

        var jobsToAttemptScheduling: ListBuffer[Job] = getJobs(pendingQueueAsList, simulator.currentTime)
        if(PolicyModes.myPolicies.contains(policyMode)){
          if (jobsToAttemptScheduling.size > 1)
            jobsToAttemptScheduling = jobsToAttemptScheduling.sortWith(_.numTasks > _.numTasks)
        }

        if (previousJob != null && previousJob == jobsToAttemptScheduling.head) {
          jobAttempt += 1
          if(jobAttempt == 2){
            jobAttempt = 0
            scheduling = false
            jobsToAttemptScheduling.foreach(job => {
              addPendingJob(job)
            })
            simulator.logger.info(schedulerPrefix + " Exiting because we are trying to schedule the same job that failed before.")
            return
          }

        }
        previousJob = jobsToAttemptScheduling.head

        totalQueueSize += numJobsInQueue
        numSchedulingCalls += 1

        syncCellState()
        simulator.logger.info(schedulerPrefix + " The cell state is (%f cpus, %f mem)".format(privateCellState.availableCpus, privateCellState.availableMem))

        var jobThinkTime: Double = 0
        var allJobPrefix = "[Job"
        jobsToAttemptScheduling.foreach(job => {
          jobThinkTime += getThinkTime(job)
          allJobPrefix += " %d (%s)".format(job.id, job.workloadName)
        })
        allJobPrefix += "] "
        val elasticPrefix = "[Elastic]"

        simulator.logger.info(schedulerPrefix + allJobPrefix + "Started %f seconds of scheduling thinktime."
          .format(jobThinkTime))
        simulator.afterDelay(jobThinkTime) {
          simulator.logger.info((schedulerPrefix + allJobPrefix + "Finished %f seconds of scheduling " +
            "thinktime.").format(jobThinkTime))


          /*
           * Let's perform a simulation of the jobs, with the same priority, that could be allocated on the cluster
           */
          val jobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()
          val tmpJobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()

          val jobsCannotFit: ListBuffer[Job] = new ListBuffer[Job]()

          var stop:Boolean = false
          var clusterFreeResources: Double = privateCellState.availableCpus * privateCellState.availableMem

          jobsToAttemptScheduling.foreach(job => {
            val jobPrefix = "[Job %d (%s)] ".format(job.id, job.workloadName)
            var claimDelta_inelastic = new ListBuffer[ClaimDelta]()
            var claimDelta_elastic = new ListBuffer[ClaimDelta]()

            job.numSchedulingAttempts += 1

            if(PolicyModes.myPolicies.contains(policyMode)){
              if(!stop){
                tmpJobsToLaunch.clear()
                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  var tmpClaimDelta_inelastic = new ListBuffer[ClaimDelta]()
                  var tmpClaimDelta_elastic = new ListBuffer[ClaimDelta]()
                  elastic.foreach(claimDelta => {
                    tmpClaimDelta_elastic += claimDelta
                  })
                  inelastic.foreach(claimDelta => {
                    tmpClaimDelta_inelastic += claimDelta
                  })
                  tmpJobsToLaunch += ((job1, tmpClaimDelta_inelastic, tmpClaimDelta_elastic))
                }

                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  elastic.foreach(claimDelta => {
                    claimDelta.unApply(privateCellState)
                  })
                  elastic.clear()
                }

                claimDelta_inelastic = scheduleJob(job, privateCellState)
                if (!isAllocationSuccessfully(claimDelta_inelastic, job)) {
                  claimDelta_inelastic.foreach(claimDelta => {
                    claimDelta.unApply(privateCellState)
                  })
                  claimDelta_inelastic.clear()
                }
                if (claimDelta_inelastic.nonEmpty || job.finalStatus == JobStates.Fully_Scheduled) {
                  jobsToLaunch += ((job, claimDelta_inelastic, claimDelta_elastic))
                }
                jobsToLaunch.foreach { case (job1, inelastic, elastic) =>
                  claimDelta_elastic = scheduleJob(job1, privateCellState, elastic = true)
                  if (claimDelta_elastic.nonEmpty) {
                    elastic.clear()
                    elastic ++= claimDelta_elastic
                  }
                }
                val currentFreeResource: Double = privateCellState.availableCpus * privateCellState.availableMem
                if(currentFreeResource >= clusterFreeResources){
                  stop = true
                  jobsToLaunch.clear()
                  jobsToLaunch ++= tmpJobsToLaunch
                }
                clusterFreeResources = currentFreeResource
              }
            }else{
              claimDelta_inelastic = scheduleJob(job, privateCellState)
              if (!isAllocationSuccessfully(claimDelta_inelastic, job)) {
                claimDelta_inelastic.foreach(claimDelta => {
                  claimDelta.unApply(privateCellState)
                })
              }else {
                claimDelta_elastic = scheduleJob(job, privateCellState, elastic = true)
                if(claimDelta_elastic.size == job.elasticTasks){
                  jobsToLaunch += ((job, claimDelta_inelastic, claimDelta_elastic))
                }else{
                  simulator.logger.info(schedulerPrefix + jobPrefix + " Not all services scheduled (%d/%d). Rolling back. The cellstate had (%f cpus, %f mem) free."
                    .format(claimDelta_elastic.size, job.elasticTasks, privateCellState.availableCpus, privateCellState.availableMem))
                  simulator.logger.info(schedulerPrefix + jobPrefix + " Total resources requested: (%f cpus, %f mem)"
                    .format(job.cpusPerTask * job.elasticTasks, job.memPerTask * job.elasticTasks))
                  claimDelta_elastic.foreach(claimDelta => {
                    claimDelta.unApply(privateCellState)
                  })
                  simulator.logger.info(schedulerPrefix + jobPrefix + " The cellstate now have (%f cpu, %f mem) free."
                    .format(privateCellState.availableCpus, privateCellState.availableMem))

                  val taskCanFitPerCpus = Math.floor(privateCellState.cpusPerMachine / job.cpusPerTask) * privateCellState.numMachines
                  val taskCanFitPerMem = Math.floor(privateCellState.memPerMachine / job.memPerTask) * privateCellState.numMachines
                  if(taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
                    simulator.logger.warn((schedulerPrefix + jobPrefix +  "The cell (%f cpus, %f mem) is not big enough " +
                      "to hold this job all at once which requires %d tasks for %f cpus " +
                      "and %f mem in total.").format(privateCellState.totalCpus,
                      privateCellState.totalMem,
                      job.numTasks,
                      job.cpusPerTask * job.numTasks,
                      job.memPerTask * job.numTasks))
                    jobsCannotFit += job
                  }
                }
              }
            }
          })
          jobsCannotFit.foreach(job => {
            jobsToAttemptScheduling -= job
          })

          /*
           * After the simulation, we will deploy the allocations on the real cluster
           */
          simulator.logger.info(schedulerPrefix + allJobPrefix + "There are %d jobs that can be allocated.".format(jobsToLaunch.size))
          jobsToLaunch.foreach { case (job, inelastic, elastic) =>
            val jobPrefix = "[Job %d (%s)] ".format(job.id, job.workloadName)
            var inelasticTasksUnscheduled: Int = job.unscheduledTasks
            var elasticTasksUnscheduled: Int = job.elasticTasksUnscheduled

            if (!runningQueueAsList.contains(job)) {
//              job.numTaskSchedulingAttempts += inelasticTasksUnscheduled
              if (inelastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(inelastic)
                if (commitResult.committedDeltas.nonEmpty) {
//                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)

                  inelasticTasksUnscheduled -= commitResult.committedDeltas.size
                  job.claimInelasticDeltas ++= commitResult.committedDeltas
                  job.unscheduledTasks = inelasticTasksUnscheduled
//                  numSuccessfulTransactions += 1
                  job.finalStatus = JobStates.Partially_Scheduled

                  if (job.firstScheduled) {
                    job.timeInQueueTillFirstScheduled = simulator.currentTime - job.submitted
                    job.firstScheduled = false
                  }

                  simulator.logger.info(schedulerPrefix + jobPrefix + "Scheduled %d tasks, %d remaining."
                    .format(inelastic.size, job.unscheduledTasks))
                } else {
//                  numFailedTransactions += 1
                  simulator.logger.info(schedulerPrefix + jobPrefix + "There was a conflict when committing the task allocation to the real cell.")
//                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }
              } else {
//                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
//                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info((schedulerPrefix + jobPrefix + "No tasks scheduled (%f cpu %f mem per task) " +
                  "during this scheduling attempt, recording wasted time.")
                  .format(job.cpusPerTask,
                    job.memPerTask))
              }
              if (inelasticTasksUnscheduled == 0) {
                val jobDuration: Double = job.estimateJobDuration()
                job.jobStartedWorking = simulator.currentTime
                job.jobFinishedWorking = simulator.currentTime + jobDuration

                simulator.afterDelay(jobDuration, eventType = EventTypes.Remove, itemId = job.id) {
                  simulator.logger.info(schedulerPrefix + jobPrefix + "Completed after %fs.".format(simulator.currentTime - job.jobStartedWorking))
                  job.finalStatus = JobStates.Completed
                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(schedulerPrefix + jobPrefix + "Adding finished event after %f seconds to wake up scheduler.".format(jobDuration))
                simulator.cellState.scheduleEndEvents(job.claimInelasticDeltas, delay = jobDuration, jobId = job.id)

                // All tasks in job scheduled so don't put it back in pendingQueueAsList.
                job.finalStatus = JobStates.Fully_Scheduled
                job.timeInQueueTillFullyScheduled = simulator.currentTime - job.submitted

                addRunningJob(job)

                simulator.logger.info((schedulerPrefix + jobPrefix + "Fully-Scheduled (%f cpu %f mem per task), " +
                  "after %d scheduling attempts.").format(job.cpusPerTask,
                  job.memPerTask,
                  job.numSchedulingAttempts))
              } else {
                simulator.logger.info((schedulerPrefix + jobPrefix + "Not fully scheduled, %d / %d tasks remain " +
                  "(shape: %f cpus, %f mem per task). Leaving it " +
                  "in the queue.").format(inelasticTasksUnscheduled,
                  job.moldableTasks,
                  job.cpusPerTask,
                  job.memPerTask))
              }
            }

            if (job.finalStatus == JobStates.Completed) {
              simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Finished during the thinking time. " +
                "Do not process it.")
            } else if (elasticTasksUnscheduled > 0){
              var elasticTasksLaunched = 0
              if (elastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(elastic)
                if (commitResult.committedDeltas.nonEmpty) {
//                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)

                  elasticTasksLaunched = commitResult.committedDeltas.size
                  elasticTasksUnscheduled -= elasticTasksLaunched
                  job.claimElasticDeltas ++= commitResult.committedDeltas
                  job.elasticTasksUnscheduled = elasticTasksUnscheduled
//                  numSuccessfulTransactions += 1

                  simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Scheduled %d tasks, %d remaining."
                    .format(elasticTasksLaunched, job.elasticTasksUnscheduled))
                } else {
//                  numFailedTransactions += 1
                  simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "There was a conflict when committing the task allocation to the real cell.")
//                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }

              } else if (job.elasticTasks > 0){
//                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
//                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info((schedulerPrefix + elasticPrefix + jobPrefix + "No tasks scheduled (%f cpu %f mem per task) " +
                  "during this scheduling attempt, recording " +
                  "wasted time. %d unscheduled tasks remaining.")
                  .format(job.cpusPerTask,
                    job.memPerTask,
                    elasticTasksUnscheduled))
              }
              if (elasticTasksLaunched > 0) {
                val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, newTasksAllocated = elasticTasksLaunched)

                job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

                // We have to remove all the incoming simulation events that work on this job.
                simulator.removeIf(x => x.itemId == job.id &&
                  (x.eventType == EventTypes.Remove || x.eventType == EventTypes.Trigger))

                simulator.afterDelay(jobLeftDuration, eventType = EventTypes.Remove, itemId = job.id) {
                  simulator.logger.info(schedulerPrefix + jobPrefix + "Completed after %fs.".format(simulator.currentTime - job.jobStartedWorking))
                  job.finalStatus = JobStates.Completed
                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(schedulerPrefix + elasticPrefix + jobPrefix + "Adding finished event after %f seconds to wake up scheduler.".format(jobLeftDuration))
                simulator.cellState.scheduleEndEvents(job.allClaimDeltas, delay = jobLeftDuration, jobId = job.id)
              }
            }

            if (job.finalStatus == JobStates.Completed || (inelasticTasksUnscheduled == 0 && elasticTasksUnscheduled == 0)) {
              jobsToAttemptScheduling -= job
            }
          }

          jobsToAttemptScheduling.foreach(job => {
            addPendingJob(job)
          })

          scheduling = false
          scheduleNextJob()
        }
      }
    }
  }
}

object PolicyModes extends Enumeration {
  val Fifo, PriorityFifo, SJF, LJF, HRRN, SRPT, Size,
  MySJF, MySRPT,  MySize, MyFifo, MySize2, MySize3, MySize4, MySize5, MySize6, MySize7, MySize8,
  MySizeError, SRPTError = Value

  val myPolicies = List[PolicyModes.Value](PolicyModes.MyFifo, PolicyModes.MySJF, PolicyModes.MySRPT, PolicyModes.MySize, PolicyModes.MySizeError,
    PolicyModes.MySize2, PolicyModes.MySize3, PolicyModes.MySize4, PolicyModes.MySize5, PolicyModes.MySize6, PolicyModes.MySize7, PolicyModes.MySize8)

  val logger = Logger.getLogger(this.getClass.getName)

  def applyPolicy(queue: ListBuffer[Job], policy: PolicyModes.Value, currentTime:Double = 0): ListBuffer[Job] = {
    policy match {
      case PolicyModes.Fifo | PolicyModes.MyFifo => queue
      case PolicyModes.PriorityFifo => queue.sortWith(PolicyModes.comparePriority(_,_) > 0)
      case PolicyModes.SJF | PolicyModes.MySJF => queue.sortWith(PolicyModes.compareJobTime(_,_) < 0)
      case PolicyModes.LJF => queue.sortWith(PolicyModes.compareJobTime(_,_) > 0)
      case PolicyModes.HRRN => queue.sortWith(PolicyModes.compareResponseRatio(_,_, currentTime) > 0)
      case PolicyModes.SRPT | PolicyModes.MySRPT => queue.sortWith(PolicyModes.compareJobRemainingTime(_,_) < 0)
      case PolicyModes.MySize | PolicyModes.Size => queue.sortWith(PolicyModes.compareSize(_,_) < 0)
      case PolicyModes.MySize2 => queue.sortWith(PolicyModes.compareSize2(_,_) < 0)
      case PolicyModes.MySize3 => queue.sortWith(PolicyModes.compareSize3(_,_) < 0)
      case PolicyModes.MySize4 => queue.sortWith(PolicyModes.compareSize4(_,_) < 0)
      case PolicyModes.MySize5 => queue.sortWith(PolicyModes.compareSize5(_,_) < 0)
      case PolicyModes.MySize6 => queue.sortWith(PolicyModes.compareSize6(_,_) < 0)
      case PolicyModes.MySize7 => queue.sortWith(PolicyModes.compareSize7(_,_) < 0)
      case PolicyModes.MySize8 => queue.sortWith(PolicyModes.compareSize8(_,_) < 0)
      case PolicyModes.SRPTError => queue.sortWith(PolicyModes.compareJobRemainingTime(_,_) < 0)
      case PolicyModes.MySizeError => queue.sortWith(PolicyModes.compareSize(_,_) < 0)
    }
  }

  def getPriority(job: Job, policy: PolicyModes.Value, currentTime:Double = 0): Double = {
    policy match {
      case PolicyModes.Fifo | PolicyModes.MyFifo => -1.0
      case PolicyModes.PriorityFifo => job.priority
      case PolicyModes.SJF | PolicyModes.MySJF | PolicyModes.LJF => jobDuration(job)
      case PolicyModes.HRRN => job.responseRatio(currentTime)
      case PolicyModes.SRPT | PolicyModes.MySRPT => remainingTime(job)
      case PolicyModes.MySize | PolicyModes.Size => size(job)
      case PolicyModes.MySize2 => size2(job)
      case PolicyModes.MySize3 => size3(job)
      case PolicyModes.MySize4 => size4(job)
      case PolicyModes.MySize5 => size5(job)
      case PolicyModes.MySize6 => size6(job)
      case PolicyModes.MySize7 => size7(job)
      case PolicyModes.MySize8 => size8(job)
      case PolicyModes.SRPTError => remainingTimeWithError(job)
      case PolicyModes.MySizeError => sizeWithError(job)
    }
  }

  def getJobsWithSamePriority(queue: ListBuffer[Job], policy: PolicyModes.Value, currentTime:Double = 0) : ListBuffer[Job] = {
    val sortedQueue = applyPolicy(queue, policy)
    val jobs: ListBuffer[Job] = new ListBuffer[Job]()
    var previousPriority: Double = -1.0

    for(job:Job <- sortedQueue){
      if(job != null){
        val priority: Double = getPriority(job, policy, currentTime)
        if(previousPriority == -1 || previousPriority == priority){
          jobs += job
          previousPriority = priority
        }
        if(policy == PolicyModes.Fifo || policy == PolicyModes.MyFifo)
          return jobs
      }
    }
    jobs
  }

  def getJobsWithLowerPriority(queue: ListBuffer[Job], policy: PolicyModes.Value, targetJob: Job,
                               currentTime:Double = 0, threshold: Double = 1) : ListBuffer[Job] = {
    val sortedQueue = queue//applyPolicy(queue, policy)
    val jobs: ListBuffer[Job] = new ListBuffer[Job]()
    val targetPriority: Double = getPriority(targetJob, policy, currentTime)

    for(job:Job <- sortedQueue){
      if(job != null){
        val priority: Double = getPriority(job, policy, currentTime)
//        if(priority > targetPriority && job.scheduledElasticTasks > 0){
        if(targetPriority / priority < threshold){
          logger.debug("Added job with lower priority. Priority: %f / TargetPriority: %f".format(priority, targetPriority))
          jobs += job
        }
        if(policy == PolicyModes.Fifo || policy == PolicyModes.MyFifo )
          return jobs
      }
    }
    jobs
  }

  def comparePriority(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    o1.priority.compareTo(o2.priority)
  }

  def compareJobTime(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    jobDuration(o1).compareTo(jobDuration(o2))
  }

  def compareJobRemainingTime(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    remainingTime(o1).compareTo(remainingTime(o2))
  }

  def compareJobRemainingTimeWithError(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    remainingTimeWithError(o1).compareTo(remainingTimeWithError(o2))
  }

  def compareResponseRatio(o1: Job, o2: Job, currentTime: Double): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    o1.responseRatio(currentTime).compareTo(o2.responseRatio(currentTime))
  }

  def compareSize(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size(o1).compareTo(size(o2))
  }

  def compareSizeWithError(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    sizeWithError(o1).compareTo(sizeWithError(o2))
  }

  def compareSize2(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size2(o1).compareTo(size2(o2))
  }

  def compareSize3(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size3(o1).compareTo(size3(o2))
  }

  def compareSize4(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size4(o1).compareTo(size4(o2))
  }

  def compareSize5(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size5(o1).compareTo(size5(o2))
  }

  def compareSize6(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size6(o1).compareTo(size6(o2))
  }

  def compareSize7(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size7(o1).compareTo(size7(o2))
  }


  def compareSize8(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    size8(o1).compareTo(size8(o2))
  }

  def jobDuration(job: Job): Double = {
    job.jobDuration * job.sizeAdjustment
  }

  def remainingTime(job: Job): Double = {
    job.remainingTime * job.sizeAdjustment
  }

  def size(job:Job): Double = {
    job.remainingTime * (job.numTasks * job.memPerTask * job.cpusPerTask) * job.sizeAdjustment
  }

  def remainingTimeWithError(job: Job): Double = {
    job.remainingTime * job.sizeAdjustment * job.error
  }

  def sizeWithError(job:Job): Double = {
    job.remainingTime * job.numTasks * job.sizeAdjustment * job.error
  }

  def size2(job:Job): Double = {
    (job.numTasks / job.moldableTasks.toDouble) * job.jobDuration * job.sizeAdjustment
  }

  def size3(job:Job): Double = {
    job.remainingTime * (job.unscheduledTasks + job.elasticTasksUnscheduled) * job.sizeAdjustment
  }

  def size4(job:Job): Double = {
    (job.numTasks / job.moldableTasks.toDouble) * job.remainingTime * job.sizeAdjustment
  }

  def size5(job:Job): Double = {
    job.jobDuration * job.numTasks * job.sizeAdjustment
  }

  def size6(job:Job): Double = {
    job.jobDuration * job.moldableTasks * job.sizeAdjustment
  }

  def size7(job:Job): Double = {
    job.remainingTime * job.moldableTasks * job.sizeAdjustment
  }

  def size8(job:Job): Double = {
    job.remainingTime * job.numTasks * job.sizeAdjustment
  }
}
