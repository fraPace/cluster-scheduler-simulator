/**
  * Copyright (c) 2013, Regents of the University of California
  * All rights reserved.
  *
  * Redistribution and use in source and binary forms, with or without
  * modification, are permitted provided that the following conditions are met:
  *
  * Redistributions of source code must retain the above copyright notice, this
  * list of conditions and the following disclaimer.  Redistributions in binary
  * form must reproduce the above copyright notice, this list of conditions and the
  * following disclaimer in the documentation and/or other materials provided with
  * the distribution.  Neither the name of the University of California, Berkeley
  * nor the names of its contributors may be used to endorse or promote products
  * derived from this software without specific prior written permission.  THIS
  * SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
  * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
  * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
  * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
  * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
  * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  */

package ClusterSchedulingSimulation.schedulers

import ClusterSchedulingSimulation.core._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class MonolithicSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                              runTime: Double,
                              val allocationMode: AllocationMode.Value)
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
        new MonolithicScheduler(schedDesc.name,
          constantThinkTimes.toMap,
          perTaskThinkTimes.toMap,
          math.floor(newBlackListPercent *
            cellStateDesc.numMachines.toDouble).toInt,
          allocationMode)
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

class MonolithicScheduler(name: String,
                          constantThinkTimes: Map[String, Double],
                          perTaskThinkTimes: Map[String, Double],
                          numMachinesToBlackList: Double = 0,
                          allocationMode: AllocationMode.Value)
  extends Scheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode) {
  lazy val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
  logger.debug("scheduler-id-info: %d, %s, %d, %s, %s"
    .format(Thread.currentThread().getId,
      name,
      hashCode(),
      constantThinkTimes.mkString(";"),
      perTaskThinkTimes.mkString(";")))

  override
  def addJob(job: Job) = {
    super.addJob(job)
    pendingQueue.enqueue(job)
    simulator.logger.info("Scheduler %s enqueued job %d of workload type %s."
      .format(name, job.id, job.workloadName))
    scheduleNextJobAction()
  }

  /**
    * Checks to see if there is currently a job in this scheduler's job queue.
    * If there is, and this scheduler is not currently scheduling a job, then
    * pop that job off of the queue and "begin scheduling it". Scheduling a
    * job consists of setting this scheduler's state to scheduling = true, and
    * adding a finishSchedulingJobAction to the simulators event queue by
    * calling afterDelay().
    */
  def scheduleNextJobAction(): Unit = {
    if (!scheduling && pendingQueue.nonEmpty) {
      scheduling = true
      val job = pendingQueue.dequeue
      job.updateTimeInQueueStats(simulator.currentTime)
      job.lastSchedulingStartTime = simulator.currentTime
      val jobThinkTime = getThinkTime(job)
      simulator.afterDelay(jobThinkTime) {
        simulator.logger.info(("Job %d (%s) finished %f seconds of scheduling " +
          "thinktime; now trying to claim resources for %d " +
          "tasks with %f cpus and %f mem each.")
          .format(job.id,
            job.workloadName,
            jobThinkTime,
            job.numTasks,
            job.cpusPerTask,
            job.memPerTask))
        job.numSchedulingAttempts += 1
        job.numTaskSchedulingAttempts += job.tasksUnscheduled

        assert(job.tasksUnscheduled > 0)
        val claimDeltas = scheduleJob(job, simulator.cellState)
        if (claimDeltas.nonEmpty) {
          job.jobStartedWorking = simulator.currentTime
          simulator.cellState.scheduleEndEvents(claimDeltas)
//          job.tasksUnscheduled -= claimDeltas.length
          job.claimDeltas ++= claimDeltas
          simulator.logger.info("scheduled %d tasks of job %d's, %d remaining."
            .format(claimDeltas.length, job.id, job.tasksUnscheduled))
          numSuccessfulTransactions += 1
          job.finalStatus = JobStatus.Partially_Scheduled
          recordUsefulTimeScheduling(job,
            jobThinkTime,
            job.numSchedulingAttempts == 1)
        } else {
          simulator.logger.info(("No tasks scheduled for job %d (%f cpu %f mem) " +
            "during this scheduling attempt, not recording " +
            "any busy time. %d unscheduled tasks remaining.")
            .format(job.id,
              job.cpusPerTask,
              job.memPerTask,
              job.tasksUnscheduled))
          numNoResourcesFoundSchedulingAttempts += 1
        }
        // If the job isn't yet fully scheduled, put it back in the queue.
        if (job.tasksUnscheduled > 0) {
          simulator.logger.info(("Job %s didn't fully schedule, %d / %d tasks remain " +
            "(shape: %f cpus, %f mem). Putting it " +
            "back in the queue").format(job.id,
            job.tasksUnscheduled,
            job.numTasks,
            job.cpusPerTask,
            job.memPerTask))
          // Give up on a job if (a) it hasn't scheduled a single task in
          // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
          if ((job.numSchedulingAttempts > 100 &&
            job.tasksUnscheduled == job.numTasks) ||
            job.numSchedulingAttempts > 1000) {
            simulator.logger.info(("Abandoning job %d (%f cpu %f mem) with %d/%d " +
              "remaining tasks, after %d scheduling " +
              "attempts.").format(job.id,
              job.cpusPerTask,
              job.memPerTask,
              job.tasksUnscheduled,
              job.numTasks,
              job.numSchedulingAttempts))
            numJobsTimedOutScheduling += 1
            job.finalStatus = JobStatus.TimedOut
          } else {
            simulator.afterDelay(1) {
              addJob(job)
            }
          }
        } else {
          job.jobFinishedWorking = simulator.currentTime + job.taskDuration
          // All tasks in job scheduled so don't put it back in _pendingQueue.
          job.finalStatus = JobStatus.Fully_Scheduled
        }
        if (job.finalStatus != JobStatus.Not_Scheduled) {
          simulator.logger.info("%s %s %d %s %d %d %f"
            .format(Thread.currentThread().getId,
              name,
              hashCode(),
              job.finalStatus,
              job.id,
              job.numSchedulingAttempts,
              simulator.currentTime - job.submitted))
        }

        scheduling = false
        scheduleNextJobAction()
      }
      simulator.logger.info("Scheduler '%s' started scheduling job %d "
        .format(name, job.id))
    }
  }
}
