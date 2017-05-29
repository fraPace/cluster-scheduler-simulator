/**
  * Copyright (c) 2016 Eurecom
  * All rights reserved.
  * *
  * Redistribution and use in source and binary forms, with or without
  * modification, are permitted provided that the following conditions are met:
  * *
  * Redistributions of source code must retain the above copyright notice, this
  * list of conditions and the following disclaimer. Redistributions in binary
  * form must reproduce the above copyright notice, this list of conditions and the
  * following disclaimer in the documentation and/or other materials provided with
  * the distribution.
  * *
  * Neither the name of Eurecom nor the names of its contributors may be used to
  * endorse or promote products derived from this software without specific prior
  * written permission.
  * *
  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  *DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package ClusterSchedulingSimulation.schedulers

import ClusterSchedulingSimulation.core._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.collection.{Iterator, mutable}

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class ZoeSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                       runTime: Double,
                       val allocationMode: AllocationMode.Value,
                       val policyMode: PolicyModes.Value)
  extends ClusterSimulatorDesc(runTime) {
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
      logging)
  }
}

class ZoeScheduler(name: String,
                   constantThinkTimes: Map[String, Double],
                   perTaskThinkTimes: Map[String, Double],
                   numMachinesToBlackList: Double = 0,
                   allocationMode: AllocationMode.Value,
                   policyMode: PolicyModes.Value)
  extends Scheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode) with LazyLogging{
  logger.debug("scheduler-id-info: " + Thread.currentThread().getId + ", " + name + ", " + hashCode() + ", " + constantThinkTimes.mkString(";")
    + ", " + perTaskThinkTimes.mkString(";"))
  
  var pendingQueueAsList = new ListBuffer[Job]()
  var numJobsInQueue: Int = 0

  var runningQueueAsList = new ListBuffer[Job]()
  var numRunningJobs: Int = 0
  var jobAttempt: Int = 0
  var privateCellState: CellState = _
  var firstTime = true

  override def jobQueueSize: Long = pendingQueueAsList.count(_ != null)

  //  var previousJob: Job = _

  override def runningJobQueueSize: Long = runningQueueAsList.count(_ != null)

  def getJobs(queue: ListBuffer[Job], currentTime: Double): ListBuffer[Job] = {
    //    val jobs = PolicyModes.getJobsWithSamePriority(queue, policyMode, currentTime)
    val jobs = PolicyModes.getJobs(queue, policyMode)
    //    jobs.foreach(job => {
    //      removePendingJob(job)
    //    })
    jobs
  }

  /**
    * This function gives the next job in the queue
    *
    * @param iterator the iterator of the queue
    * @return A tuple with:
    *         the next job in the queue or null if the end of the queue has been reached
    *         a counter with the number of elements skipped
    */
  def getNextJobInQueue(iterator: Iterator[Job]): (Job, Long) = {
    var elementsSkipped: Long = 0
    while (iterator.hasNext) {
      val result = iterator.next()
      if (result != null)
        return (result, elementsSkipped)
      elementsSkipped += 1
    }
    (null, elementsSkipped)
  }

  def removePendingJob(job: Job): Unit = {
    val idx = pendingQueueAsList.indexOf(job)
    if (idx != -1 && pendingQueueAsList(idx) != null) {
      numJobsInQueue -= 1
      pendingQueueAsList(idx) = null
    }
  }

  def removeRunningJob(job: Job): Unit = {
    val idx = runningQueueAsList.indexOf(job)
    if (idx != -1 && runningQueueAsList(idx) != null) {
      numRunningJobs -= 1
      runningQueueAsList(idx) = null
    }
  }

  def addPendingJob(job: Job, prepend: Boolean = false): Unit = {
    numJobsInQueue += 1
    if (!prepend)
      pendingQueueAsList += job
    else
      pendingQueueAsList.prepend(job)
  }

  def addPendingJobs(jobs: ListBuffer[Job], prepend: Boolean = false): Unit = {
    numJobsInQueue += jobs.length
    if (!prepend)
      pendingQueueAsList ++= jobs
    else
      pendingQueueAsList.prependAll(jobs)
  }

  def addRunningJob(job: Job): Unit = {
    numRunningJobs += 1
    runningQueueAsList += job
  }

  def isAllocationSuccessfully(claimDeltas: Seq[ClaimDelta], job: Job): Boolean = {
    allocationMode match {
      case AllocationMode.Incremental => claimDeltas.nonEmpty
      case AllocationMode.All => claimDeltas.size == job.numTasks
      case AllocationMode.AllCore => claimDeltas.size == job.coreTasks
      case _ => false
    }
  }

  def syncCellState() {
    privateCellState = simulator.cellState.copy
    simulator.logger.debug(loggerPrefix + " Scheduler " + name + " (" + hashCode + ") has new private cell state " + privateCellState.hashCode)
  }

  override
  def wakeUp(): Unit = {
    simulator.logger.trace("wakeUp method called.")
    //    simulator.logger.warn("%f - Jobs in Queue: %d | Jobs Running: %d ".format(simulator.currentTime, numJobsInQueue, numRunningJobs))

    scheduleNextJob()
  }

  override
  def addJob(job: Job): Unit = {
    simulator.logger.info(loggerPrefix + " Enqueued job " + job.id + " of workload type " + job.workloadName + ".")
    super.addJob(job)

    addPendingJob(job)
    //    simulator.logger.warn("%f - Added a new Job (%s) in the queue. Num Tasks: %d (%d/%d) | Cpus: %f | Mem: %f | Job Runtime: %f"
    //      .format(simulator.currentTime, job.workloadName, job.numTasks, job.moldableTasks, job.elasticTasks, job.cpusPerTask, job.memPerTask, job.jobDuration ))
//    if (firstTime) {
//      if (numJobsInQueue == 3) {
//        wakeUp()
//        firstTime = false
//      }
//    } else
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
        if (job != null && job.finalStatus == JobStatus.Completed) {
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

        simulator.logger.info(loggerPrefix + " There are " + numJobsInQueue + " jobs in queue and " + numRunningJobs + " running.")
        simulator.logger.info(loggerPrefix + " The global cell state is (" + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

        val jobsToAttemptScheduling: ListBuffer[Job] = getJobs(pendingQueueAsList, simulator.currentTime)
        if (policyMode == PolicyModes.Fifo || policyMode == PolicyModes.eFifo)
          assert(jobsToAttemptScheduling.length == 1, {
            "For Fifo and eFifo policy the jobsToAttemptScheduling length must be 1 (" + jobsToAttemptScheduling.length + ")"
          })
        //        if(PolicyModes.myPolicies.contains(policyMode)){
        //          if (jobsToAttemptScheduling.size > 1)
        //            jobsToAttemptScheduling = jobsToAttemptScheduling.sortWith(_.numTasks > _.numTasks)
        //        }

        totalQueueSize += numJobsInQueue
        numSchedulingCalls += 1

        syncCellState()
        simulator.logger.info(loggerPrefix + " The private cell state is (" + privateCellState.availableCpus + " CPUs, " + privateCellState.availableMem + " mem)")

        val jobThinkTime: Double = jobsToAttemptScheduling.map(getThinkTime).sum
        lazy val allJobPrefix = "[" + jobsToAttemptScheduling.length + " | Job" + jobsToAttemptScheduling.foldLeft("")((b, a) => b + " " + a.id + " (" + a.workloadName + ")") + "]"
        lazy val elasticPrefix = "[Elastic]"

        simulator.logger.info(loggerPrefix + allJobPrefix + " Started " + jobThinkTime + " seconds of scheduling thinktime.")
        simulator.afterDelay(jobThinkTime) {
          simulator.logger.info(loggerPrefix + allJobPrefix + " Finished " + jobThinkTime + " seconds of scheduling thinktime.")

          /*
           * Let's perform a simulation of the jobs, with the same priority, that could be allocated on the cluster
           */
          val jobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()
          val tmpJobsToLaunch: ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])] = new ListBuffer[(Job, ListBuffer[ClaimDelta], ListBuffer[ClaimDelta])]()

          val jobsCannotFit: ListBuffer[Job] = new ListBuffer[Job]()

          var stop: Boolean = false
          var clusterFreeResources: Long = privateCellState.availableCpus * privateCellState.availableMem

          jobsToAttemptScheduling.foreach(job => {
            var claimDelta_core = new ListBuffer[ClaimDelta]()
            var claimDelta_elastic = new ListBuffer[ClaimDelta]()
            job.numSchedulingAttempts += 1

            if (PolicyModes.myPolicies.contains(policyMode)) {
              if (!stop) {
                // Create a copy of the jobsToLaunch in tmpJobsToLaunch
                tmpJobsToLaunch.clear()
                jobsToLaunch.foreach { case (job1, core, elastic) =>
                  tmpJobsToLaunch += ((job1, new ListBuffer[ClaimDelta]() ++= core, new ListBuffer[ClaimDelta]() ++= elastic))
                }

                // START ALGORITHM PRESENTED IN THE PAPER
                //Remove all elastic components from the cell
                jobsToLaunch.foreach { case (_, _, elastic) =>
                  elastic.foreach(_.unApply(privateCellState))
                  elastic.clear()
                }

                val isJobRunning = runningQueueAsList.contains(job)
                if (!isJobRunning) {
                  // If the job is not already running, try to scheduled it
                  claimDelta_core = scheduleJob(job, privateCellState, taskType = TaskType.Core)
                  // Check if the core components that can be scheduled satisfy the allocation policy of this scheduler
                  if (!isAllocationSuccessfully(claimDelta_core, job)) {
                    // If not, remove them from the cell
                    claimDelta_core.foreach(_.unApply(privateCellState))
                    claimDelta_core.clear()
                  }
                }
                // If we succeeded in scheduling the core components or if the job is already running
                //    try to schedule the as many elastic components as we can for the jobs in the list
                if (claimDelta_core.nonEmpty || isJobRunning) {
                  jobsToLaunch += ((job, claimDelta_core, claimDelta_elastic))
                }
                jobsToLaunch.foreach { case (job1, _, elastic) =>
                  claimDelta_elastic = scheduleJob(job1, privateCellState, taskType = TaskType.Elastic)
                  if (claimDelta_elastic.nonEmpty) {
                    elastic.clear()
                    elastic ++= claimDelta_elastic
                  }
                }
                val currentFreeResource: Long = privateCellState.availableCpus * privateCellState.availableMem
                if (currentFreeResource >= clusterFreeResources) {
                  stop = true
                  jobsToLaunch.clear()
                  jobsToLaunch ++= tmpJobsToLaunch
                }
                clusterFreeResources = currentFreeResource
                // FINISH ALGORITHM PRESENTED IN THE PAPER
              }
            } else if (PolicyModes.elasticPolicies.contains(policyMode)) {
              claimDelta_core = scheduleJob(job, privateCellState, taskType = TaskType.Core)
              if (!isAllocationSuccessfully(claimDelta_core, job)) {
                claimDelta_core.foreach(_.unApply(privateCellState))
                claimDelta_core.clear()
              }
              if (runningQueueAsList.contains(job) || claimDelta_core.nonEmpty)
                claimDelta_elastic = scheduleJob(job, privateCellState, taskType = TaskType.Elastic)
              if (claimDelta_core.nonEmpty || claimDelta_elastic.nonEmpty)
                jobsToLaunch += ((job, claimDelta_core, claimDelta_elastic))
//                simulator.logger.info(loggerPrefix + job.loggerPrefix + " The cellstate now have (%f cpu, %f mem) free."
//                  .format(privateCellState.availableCpus, privateCellState.availableMem))
              val taskCanFitPerCpus = Math.floor(privateCellState.cpusPerMachine.toDouble / job.cpusPerTask.toDouble) * privateCellState.numMachines
              val taskCanFitPerMem = Math.floor(privateCellState.memPerMachine.toDouble / job.memPerTask) * privateCellState.numMachines
              if (taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
                simulator.logger.warn(loggerPrefix + job.loggerPrefix + " The cell (" + privateCellState.totalCpus + " cpus, " + privateCellState.totalMem +
                  " mem) is not big enough to hold this job all at once which requires " + job.numTasks + " tasks for " + (job.cpusPerTask * job.numTasks) +
                  " cpus and " + (job.memPerTask * job.numTasks) + " mem in total.")
                jobsCannotFit += job
              }
            } else if (PolicyModes.rigidPolicies.contains(policyMode)) {
              claimDelta_core = scheduleJob(job, privateCellState, taskType = TaskType.Core)
              if (!isAllocationSuccessfully(claimDelta_core, job)) {
                claimDelta_core.foreach(_.unApply(privateCellState))
                claimDelta_core.clear()
              } else {
                claimDelta_elastic = scheduleJob(job, privateCellState, taskType = TaskType.Elastic)
                if (claimDelta_elastic.size == job.elasticTasks) {
                  jobsToLaunch += ((job, claimDelta_core, claimDelta_elastic))
                } else {
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " Not all services scheduled (" + claimDelta_elastic.size + "/" + job.elasticTasks +
                    "). Rolling back. The cellstate had (" + privateCellState.availableCpus + " cpus, " + privateCellState.availableMem + " mem) free.")
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " Total resources requested: (" + (job.cpusPerTask * job.elasticTasks) +
                    " cpus, " + (job.memPerTask * job.elasticTasks) + " mem)")
                  claimDelta_elastic.foreach(_.unApply(privateCellState))
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " The cellstate now have (" + privateCellState.availableCpus + " cpu, " + privateCellState.availableMem + " mem) free.")

                  val taskCanFitPerCpus = Math.floor(privateCellState.cpusPerMachine.toDouble / job.cpusPerTask.toDouble) * privateCellState.numMachines
                  val taskCanFitPerMem = Math.floor(privateCellState.memPerMachine.toDouble / job.memPerTask) * privateCellState.numMachines
                  if (taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
                    simulator.logger.warn(loggerPrefix + job.loggerPrefix + " The cell (" + privateCellState.totalCpus + " cpus, " + privateCellState.totalMem +
                      " mem) is not big enough to hold this job all at once which requires " + job.numTasks + " tasks for " + (job.cpusPerTask * job.numTasks) +
                      " cpus and " + (job.memPerTask * job.numTasks) + " mem in total.")
                    jobsCannotFit += job
                  }
                }
              }
            }
          })
          jobsCannotFit.foreach(job => {
            removePendingJob(job)
          })

          /*
           * After the simulation, we will deploy the allocations on the real cluster
           */
          simulator.logger.info(loggerPrefix + allJobPrefix + " There are " + jobsToLaunch.size + " jobs that can be allocated.")
          var serviceDeployed = 0
          jobsToLaunch.foreach { case (job, inelastic, elastic) =>
//            var inelasticTasksUnscheduled: Int = job.tasksUnscheduled
//            var elasticTasksUnscheduled: Int = job.elasticTasksUnscheduled

            if (!runningQueueAsList.contains(job)) {
              //              job.numTaskSchedulingAttempts += inelasticTasksUnscheduled
              if (inelastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(inelastic)
                if (commitResult.committedDeltas.nonEmpty) {
                  //                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                  serviceDeployed += commitResult.committedDeltas.size

//                  inelasticTasksUnscheduled -= commitResult.committedDeltas.size
                  job.addCoreClaimDeltas(commitResult.committedDeltas)
//                  job.tasksUnscheduled = inelasticTasksUnscheduled
                  //                  numSuccessfulTransactions += 1
                  job.finalStatus = JobStatus.Partially_Scheduled

                  if (job.firstScheduled) {
                    job.timeInQueueTillFirstScheduled = simulator.currentTime - job.submitted
                    job.firstScheduled = false
                  }

                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " Scheduled " + inelastic.size + " tasks, " + job.coreTasksUnscheduled + " remaining.")
                } else {
                  //                  numFailedTransactions += 1
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " There was a conflict when committing the task allocation to the real cell.")
                  //                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }
              } else {
                //                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                //                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info(loggerPrefix + job.loggerPrefix + " No tasks scheduled (" + job.cpusPerTask +
                  " cpu " + job.memPerTask + "mem per task) during this scheduling attempt, recording wasted time.")
              }
              if (job.coreTasksUnscheduled == 0) {
                job.jobStartedWorking = simulator.currentTime
                val jobDuration: Double = job.estimateJobDuration(simulator.currentTime)
                job.jobFinishedWorking = simulator.currentTime + jobDuration

                simulator.afterDelay(jobDuration, eventType = EventType.Remove, itemId = job.id) {
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " Completed after " + (simulator.currentTime - job.jobStartedWorking) + "s. It had " + job.claimDeltas.size + " tasks allocated.")
                  job.finalStatus = JobStatus.Completed
                  //                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(loggerPrefix + job.loggerPrefix + " Adding finished event after " + jobDuration + " seconds to wake up scheduler.")
                simulator.cellState.scheduleEndEvents(job.coreClaimDeltas.toSeq, delay = jobDuration)

                // All tasks in job scheduled so don't put it back in pendingQueueAsList.
                job.finalStatus = JobStatus.Fully_Scheduled
                job.timeInQueueTillFullyScheduled = simulator.currentTime - job.submitted

                addRunningJob(job)

                simulator.logger.info(loggerPrefix + job.loggerPrefix + " Fully-Scheduled (" + job.cpusPerTask + " cpu " + job.memPerTask +
                  " mem per task), after " + job.numSchedulingAttempts + " scheduling attempts.")
              } else {
                simulator.logger.info(loggerPrefix + job.loggerPrefix + " Not fully scheduled, " + job.coreTasksUnscheduled + " / " + job.coreTasks +
                  " tasks remain (shape: " + job.cpusPerTask + " cpus, " + job.memPerTask + " mem per task). Leaving it in the queue.")
              }
            }

            if (job.finalStatus == JobStatus.Completed) {
              simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Finished during the thinking time. Do not process it.")
            } else if (job.elasticTasksUnscheduled > 0) {
              var elasticTasksLaunched = 0
              if (elastic.nonEmpty) {
                val commitResult = simulator.cellState.commit(elastic)
                if (commitResult.committedDeltas.nonEmpty) {
                  //                  recordUsefulTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                  serviceDeployed += commitResult.committedDeltas.size

                  elasticTasksLaunched = commitResult.committedDeltas.size
//                  elasticTasksUnscheduled -= elasticTasksLaunched
                  job.addElasticClaimDeltas(commitResult.committedDeltas)
//                  job.elasticTasksUnscheduled = elasticTasksUnscheduled
                  //                  numSuccessfulTransactions += 1

                  simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Scheduled " + elasticTasksLaunched + " tasks, " + job.elasticTasksUnscheduled + " remaining.")
                } else {
                  //                  numFailedTransactions += 1
                  simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " There was a conflict when committing the task allocation to the real cell.")
                  //                  recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                }

              } else if (job.elasticTasks > 0) {
                //                recordWastedTimeScheduling(job, getThinkTime(job), job.numSchedulingAttempts == 1)
                //                numNoResourcesFoundSchedulingAttempts += 1

                simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " No tasks scheduled (" + job.cpusPerTask + " cpu " + job.memPerTask +
                  " mem per task) during this scheduling attempt, recording wasted time. " + job.elasticTasksUnscheduled + " unscheduled tasks remaining.")
              }
              if (elasticTasksLaunched > 0) {
                val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, newTasksAllocated = elasticTasksLaunched)

                job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

                // We have to remove all the incoming simulation events that work on this job.
                simulator.removeIf(x => x.eventID == job.id &&
                  (x.eventType == EventType.Remove || x.eventType == EventType.Trigger))

                simulator.afterDelay(jobLeftDuration, eventType = EventType.Remove, itemId = job.id) {
                  simulator.logger.info(loggerPrefix + job.loggerPrefix + " Completed after " + (simulator.currentTime - job.jobStartedWorking) + "s. It had " + job.claimDeltas.size + " tasks allocated.")
                  job.finalStatus = JobStatus.Completed
                  //                  previousJob = null
                  removePendingJob(job)
                  removeRunningJob(job)
                }
                simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Adding finished event after " + jobLeftDuration + " seconds to wake up scheduler.")
                simulator.cellState.scheduleEndEvents(job.claimDeltas.toSeq, delay = jobLeftDuration)
              }
            }

            if (job.finalStatus == JobStatus.Completed || (job.coreTasksUnscheduled == 0 && job.elasticTasksUnscheduled == 0)) {
              removePendingJob(job)
            }
          }

          scheduling = false
          if (serviceDeployed == 0) {
            jobAttempt += 1
          }
          if (jobAttempt == 2) {
            jobAttempt = 0
            simulator.logger.info(loggerPrefix + " Exiting because we could not schedule the job. This means that not enough resources were available.")
          } else
            scheduleNextJob()
        }
      }
    }
  }


}

object PolicyModes extends Enumeration with LazyLogging {
  val
  Fifo, PSJF, SRPT, HRRN,
  PSJF2D, SRPT2D1, SRPT2D2, HRRN2D,
  PSJF3D, SRPT3D1, SRPT3D2, HRRN3D,

  eFifo, ePSJF, eSRPT, eHRRN,
  ePSJF2D, eSRPT2D1, eSRPT2D2, eHRRN2D,
  ePSJF3D, eSRPT3D1, eSRPT3D2, eHRRN3D,

  hFifo, hPSJF, hSRPT, hHRRN,
  hPSJF2D, hSRPT2D1, hSRPT2D2, hHRRN2D,
  hPSJF3D, hSRPT3D1, hSRPT3D2, hHRRN3D,

  PriorityFifo, LJF = Value

  val myPolicies: List[PolicyModes.Value] = List[PolicyModes.Value](
    PolicyModes.hFifo, PolicyModes.hPSJF, PolicyModes.hSRPT, PolicyModes.hHRRN,
    PolicyModes.hPSJF2D, PolicyModes.hSRPT2D1, PolicyModes.hSRPT2D2, PolicyModes.hHRRN2D,
    PolicyModes.hPSJF3D, PolicyModes.hSRPT3D1, PolicyModes.hSRPT3D2, PolicyModes.hHRRN3D
  )

  val elasticPolicies: List[PolicyModes.Value] = List[PolicyModes.Value](
    PolicyModes.eFifo, PolicyModes.ePSJF, PolicyModes.eSRPT, PolicyModes.eHRRN,
    PolicyModes.ePSJF2D, PolicyModes.eSRPT2D1, PolicyModes.eSRPT2D2, PolicyModes.eHRRN2D,
    PolicyModes.ePSJF3D, PolicyModes.eSRPT3D1, PolicyModes.eSRPT3D2, PolicyModes.eHRRN3D
  )

  val rigidPolicies: List[PolicyModes.Value] = List[PolicyModes.Value](
    PolicyModes.Fifo, PolicyModes.PSJF, PolicyModes.SRPT, PolicyModes.HRRN,
    PolicyModes.PSJF2D, PolicyModes.SRPT2D1, PolicyModes.SRPT2D2, PolicyModes.HRRN2D,
    PolicyModes.PSJF3D, PolicyModes.SRPT3D1, PolicyModes.SRPT3D2, PolicyModes.HRRN3D
  )

  val noPriorityPolicies: List[PolicyModes.Value] = List[PolicyModes.Value](
    PolicyModes.hFifo, PolicyModes.eFifo, PolicyModes.Fifo
  )

  def getJobs(queue: ListBuffer[Job], policy: PolicyModes.Value): ListBuffer[Job] = {
    val sortedQueue = applyPolicy(queue, policy)
    val jobs: ListBuffer[Job] = new ListBuffer[Job]()

    for (job: Job <- sortedQueue) {
      if (job != null) {
        jobs += job
        if (!myPolicies.contains(policy))
          return jobs
      }
    }
    jobs
  }

  def applyPolicy(queue: ListBuffer[Job], policy: PolicyModes.Value, currentTime: Double = 0): ListBuffer[Job] = {
    policy match {
      case PolicyModes.PriorityFifo => queue.sortWith(PolicyModes.comparePriority(_, _) > 0)
      case PolicyModes.LJF => queue.sortWith(PolicyModes.compareJobTime(_, _) > 0)

      case PolicyModes.Fifo | PolicyModes.hFifo | PolicyModes.eFifo => queue
      case PolicyModes.PSJF | PolicyModes.hPSJF | PolicyModes.ePSJF => queue.sortWith(PolicyModes.compareJobTime(_, _) < 0)
      case PolicyModes.HRRN | PolicyModes.hHRRN | PolicyModes.eHRRN => queue.sortWith(PolicyModes.compareResponseRatio(_, _, currentTime) < 0)
      case PolicyModes.SRPT | PolicyModes.hSRPT | PolicyModes.eSRPT => queue.sortWith(PolicyModes.compareJobRemainingTime(_, _) < 0)
      case PolicyModes.PSJF2D | PolicyModes.hPSJF2D | PolicyModes.ePSJF2D => queue.sortWith(PolicyModes.comparePSJF2D(_, _) < 0)
      case PolicyModes.SRPT2D1 | PolicyModes.hSRPT2D1 | PolicyModes.eSRPT2D1 => queue.sortWith(PolicyModes.compareSRPT2D1(_, _) < 0)
      case PolicyModes.SRPT2D2 | PolicyModes.hSRPT2D2 | PolicyModes.eSRPT2D2 => queue.sortWith(PolicyModes.compareSRPT2D2(_, _) < 0)
      case PolicyModes.HRRN2D | PolicyModes.hHRRN2D | PolicyModes.eHRRN2D => queue.sortWith(PolicyModes.compareHRRN2D(_, _, currentTime) < 0)
      case PolicyModes.PSJF3D | PolicyModes.hPSJF3D | PolicyModes.ePSJF3D => queue.sortWith(PolicyModes.comparePSJF3D(_, _) < 0)
      case PolicyModes.SRPT3D1 | PolicyModes.hSRPT3D1 | PolicyModes.eSRPT3D1 => queue.sortWith(PolicyModes.compareSRPT3D1(_, _) < 0)
      case PolicyModes.SRPT3D2 | PolicyModes.hSRPT3D2 | PolicyModes.eSRPT3D2 => queue.sortWith(PolicyModes.compareSRPT3D2(_, _) < 0)
      case PolicyModes.HRRN3D | PolicyModes.hHRRN3D | PolicyModes.eHRRN3D => queue.sortWith(PolicyModes.compareHRRN3D(_, _, currentTime) < 0)
    }
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

  def jobDuration(job: Job): Double = {
    job.jobDuration * job.sizeAdjustment * job.error
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

  def remainingTime(job: Job): Double = {
    job.remainingTime * job.sizeAdjustment * job.error
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

  def comparePSJF2D(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    pSJF2D(o1).compareTo(pSJF2D(o2))
  }

  def pSJF2D(job: Job): Double = {
    job.jobDuration * job.numTasks * job.sizeAdjustment * job.error
  }

  def compareSRPT2D1(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    sRPT2D1(o1).compareTo(sRPT2D1(o2))
  }

  def sRPT2D1(job: Job): Double = {
    job.remainingTime * job.numTasks * job.sizeAdjustment * job.error
  }

  def compareSRPT2D2(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    sRPT2D2(o1).compareTo(sRPT2D2(o2))
  }

  def sRPT2D2(job: Job): Double = {
    job.remainingTime * (job.elasticTasksUnscheduled + job.tasksUnscheduled) * job.sizeAdjustment * job.error
  }

  def compareHRRN2D(o1: Job, o2: Job, currentTime: Double): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    hRRN2D(o1, currentTime).compareTo(hRRN2D(o2, currentTime))
  }

  def hRRN2D(job: Job, currentTime: Double): Double = {
    job.responseRatio(currentTime = currentTime) * job.numTasks * job.sizeAdjustment * job.error
  }

  def comparePSJF3D(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    pSJF3D(o1).compareTo(pSJF3D(o2))
  }

  def pSJF3D(job: Job): Double = {
    job.jobDuration * (job.numTasks * job.memPerTask * job.cpusPerTask.toDouble) * job.sizeAdjustment * job.error
  }

  def compareSRPT3D1(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    sRPT3D1(o1).compareTo(sRPT3D1(o2))
  }

  def sRPT3D1(job: Job): Double = {
    job.remainingTime * (job.numTasks * job.memPerTask * job.cpusPerTask.toDouble) * job.sizeAdjustment * job.error
  }

  def compareSRPT3D2(o1: Job, o2: Job): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    sRPT3D2(o1).compareTo(sRPT3D2(o2))
  }

  def sRPT3D2(job: Job): Double = {
    job.remainingTime * ((job.elasticTasksUnscheduled + job.tasksUnscheduled) * (job.memPerTask * job.cpusPerTask.toDouble)) * job.sizeAdjustment * job.error
  }

  def compareHRRN3D(o1: Job, o2: Job, currentTime: Double): Int = {
    if (o1 == null && o2 == null)
      return 0
    if (o1 == null)
      return 1
    if (o2 == null)
      return -1
    hRRN3D(o1, currentTime).compareTo(hRRN3D(o2, currentTime))
  }

  def hRRN3D(job: Job, currentTime: Double): Double = {
    job.responseRatio(currentTime = currentTime) * job.numTasks * job.memPerTask * job.cpusPerTask.toDouble * job.sizeAdjustment * job.error
  }

  def getJobsWithSamePriority(queue: ListBuffer[Job], policy: PolicyModes.Value, currentTime: Double = 0): ListBuffer[Job] = {
    val sortedQueue = applyPolicy(queue, policy)
    val jobs: ListBuffer[Job] = new ListBuffer[Job]()
    var previousPriority: Double = -1.0

    for (job: Job <- sortedQueue) {
      if (job != null) {
        val priority: Double = getPriority(job, policy, currentTime)
        if (previousPriority == -1 || previousPriority == priority) {
          jobs += job
          previousPriority = priority
          if (noPriorityPolicies.contains(policy))
            return jobs
        }
      }
    }
    jobs
  }

  def getPriority(job: Job, policy: PolicyModes.Value, currentTime: Double = 0): Double = {
    policy match {
      case PolicyModes.PriorityFifo => job.priority

      case PolicyModes.Fifo | PolicyModes.hFifo | PolicyModes.eFifo => -1.0
      case PolicyModes.PSJF | PolicyModes.hPSJF | PolicyModes.LJF | PolicyModes.ePSJF => jobDuration(job)
      case PolicyModes.HRRN | PolicyModes.hHRRN | PolicyModes.eHRRN => responseRatio(job, currentTime)
      case PolicyModes.SRPT | PolicyModes.hSRPT | PolicyModes.eSRPT => remainingTime(job)
      case PolicyModes.PSJF2D | PolicyModes.hPSJF2D | PolicyModes.ePSJF2D => pSJF2D(job)
      case PolicyModes.SRPT2D1 | PolicyModes.hSRPT2D1 | PolicyModes.eSRPT2D1 => sRPT2D1(job)
      case PolicyModes.SRPT2D2 | PolicyModes.hSRPT2D2 | PolicyModes.eSRPT2D2 => sRPT2D2(job)
      case PolicyModes.HRRN2D | PolicyModes.hHRRN2D | PolicyModes.eHRRN2D => hRRN2D(job, currentTime)
      case PolicyModes.PSJF3D | PolicyModes.hPSJF3D | PolicyModes.ePSJF3D => pSJF3D(job)
      case PolicyModes.SRPT3D1 | PolicyModes.hSRPT3D1 | PolicyModes.eSRPT3D1 => sRPT3D1(job)
      case PolicyModes.SRPT3D2 | PolicyModes.hSRPT3D2 | PolicyModes.eSRPT3D2 => sRPT3D2(job)
      case PolicyModes.HRRN3D | PolicyModes.hHRRN3D | PolicyModes.eHRRN3D => hRRN3D(job, currentTime)
    }
  }

  def responseRatio(job: Job, currentTime: Double): Double = {
    job.responseRatio(currentTime) * job.error
  }

  def getJobsWithLowerPriority(queue: ListBuffer[Job], policy: PolicyModes.Value, targetJob: Job,
                               currentTime: Double = 0, threshold: Double = 1): ListBuffer[Job] = {
    val sortedQueue = queue
    //applyPolicy(queue, policy)
    val jobs: ListBuffer[Job] = new ListBuffer[Job]()
    val targetPriority: Double = getPriority(targetJob, policy, currentTime)

    for (job: Job <- sortedQueue) {
      if (job != null) {
        val priority: Double = getPriority(job, policy, currentTime)
        //        if(priority > targetPriority && job.scheduledElasticTasks > 0){
        if (targetPriority / priority < threshold) {
          logger.debug("Added job with lower priority. Priority: " + priority + " / TargetPriority: " + targetPriority)
          jobs += job
          if (noPriorityPolicies.contains(policy))
            return jobs
        }
      }
    }
    jobs
  }


  //  def compareJobRemainingTimeWithError(o1: Job, o2: Job): Int = {
  //    if (o1 == null && o2 == null)
  //      return 0
  //    if (o1 == null)
  //      return 1
  //    if (o2 == null)
  //      return -1
  //    remainingTimeWithError(o1).compareTo(remainingTimeWithError(o2))
  //  }

  //  def compareSizeWithError(o1: Job, o2: Job): Int = {
  //    if (o1 == null && o2 == null)
  //      return 0
  //    if (o1 == null)
  //      return 1
  //    if (o2 == null)
  //      return -1
  //    sizeWithError(o1).compareTo(sizeWithError(o2))
  //  }

  //  def remainingTimeWithError(job: Job): Double = {
  //    job.remainingTime * job.sizeAdjustment * job.error
  //  }
  //
  //  def sizeWithError(job:Job): Double = {
  //    job.remainingTime * job.numTasks * job.sizeAdjustment * job.error
  //  }
}
