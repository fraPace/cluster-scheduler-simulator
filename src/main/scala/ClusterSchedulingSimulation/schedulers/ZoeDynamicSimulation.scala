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

import ClusterSchedulingSimulation.core.ClaimDelta.ResizePolicy
import ClusterSchedulingSimulation.core.ClaimDelta.ResizePolicy.ResizePolicy
import ClusterSchedulingSimulation.core._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class ZoeDynamicSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
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
        new ZoeDynamicScheduler(schedDesc.name,
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

class ZoeDynamicScheduler (name: String,
                          constantThinkTimes: Map[String, Double],
                          perTaskThinkTimes: Map[String, Double],
                          numMachinesToBlackList: Double = 0,
                          allocationMode: AllocationMode.Value,
                          policyMode: PolicyModes.Value)
  extends ZoeScheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode,
    policyMode) with LazyLogging {
  logger.debug("scheduler-id-info: " + Thread.currentThread().getId + ", " + name + ", " + hashCode() + ", " + constantThinkTimes.mkString(";")
    + ", " + perTaskThinkTimes.mkString(";"))

  val reclaimResourcesPeriod: Double = 15.0
  val crashedAllowed = 1
  var checkingResourceUtilization: Boolean = false
  val resizePolicy: ResizePolicy = ResizePolicy.Instant

  override def addRunningJob(job: Job): Unit = {
    super.addRunningJob(job)
    if (reclaimResourcesPeriod > 0) {
      simulator.afterDelay(reclaimResourcesPeriod) {
        checkResourceUtilization()
      }
    }
  }

  def killJob(job: Job): Unit = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " A core component crashed. We are killing the application and rescheduling.")
    job.finalStatus = JobStatus.Crashed
    job.numJobCrashes += 1

    job.claimDeltas.foreach(_.unApply(simulator.cellState))

    // We have to remove all the incoming simulation events that work on this job.
    simulator.removeIf(x => x.eventID == job.id &&
      (x.eventType == EventType.Remove || x.eventType == EventType.Trigger))

    removePendingJob(job)
    removeRunningJob(job)

    job.reset()
    if (job.numJobCrashes >= crashedAllowed){
      job.disableResize = true
      simulator.logger.info(loggerPrefix + job.loggerPrefix + " This job crashed more than " + crashedAllowed + " times. Disabling resize.")
    }
    addPendingJob(job, prepend = true)

    wakeUp()
  }

  def killElasticClaimDeltas(job: Job, elastic: mutable.HashSet[ClaimDelta]): Unit = {
    lazy val elasticPrefix = "[Elastic]"
    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + elastic.size + " elastic component crashed.")

    elastic.foreach(_.unApply(simulator.cellState))
    job.removeElasticClaimDeltas(elastic.toSeq)
    // We have to reinsert the job in queue if this had all it's elastic services allocated
    if (job.elasticTasksUnscheduled == 0) {
      addPendingJob(job, prepend = true)
    }

    val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, tasksRemoved = elastic.size)

    job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

    // We have to remove all the incoming simulation events that work on this job.
    simulator.removeIf(x => x.eventID == job.id &&
      (x.eventType == EventType.Remove || x.eventType == EventType.Trigger))

    simulator.afterDelay(jobLeftDuration, eventType = EventType.Remove, itemId = job.id) {
      simulator.logger.info(loggerPrefix + job.loggerPrefix + " Completed after " + (simulator.currentTime - job.jobStartedWorking) + "s.It had " + job.claimDeltas.size + " tasks allocated.")
      job.finalStatus = JobStatus.Completed
      removePendingJob(job)
      removeRunningJob(job)
    }
    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Adding finished event after " + jobLeftDuration + " seconds to wake up scheduler.")
    simulator.cellState.scheduleEndEvents(job.claimDeltas.toSeq, delay = jobLeftDuration)
  }

  def checkResourceUtilization(): Unit = {
    if (!checkingResourceUtilization & numRunningJobs > 0) {
      checkingResourceUtilization = true

      var jobsClaimDeltasOOMKilled: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
      val claimDeltasOOMRisk: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()

      var cpuVariation: Long = 0
      var memVariation: Long = 0

      for (i <- 0 until simulator.cellState.numMachines) {
        simulator.cellState.claimDeltasPerMachine(i).foreach(claimDelta => {
          claimDelta.job match {
            case Some(job) =>
              if (!job.disableResize &&
                job.finalStatus != JobStatus.Crashed && job.finalStatus != JobStatus.Completed && job.finalStatus != JobStatus.Not_Scheduled) {

                val futureCpus = job.cpuUtilization(simulator.currentTime + reclaimResourcesPeriod)
                val futureMem = job.memoryUtilization(simulator.currentTime + reclaimResourcesPeriod)
                val currentCpus = job.cpuUtilization(simulator.currentTime)
                val currentMem = job.memoryUtilization(simulator.currentTime)

                val (slackCpu, slackMem): (Long, Long) = claimDelta.resize(simulator.cellState,
                  Math.max(futureCpus, currentCpus), Math.max(futureMem, currentMem), resizePolicy = resizePolicy)
                cpuVariation += slackCpu
                memVariation += slackMem

                if(resizePolicy != ClaimDelta.ResizePolicy.None) {
                  claimDelta.checkResourcesLimits(currentCpus, currentMem) match {
                    case ClaimDeltaStatus.OOMRisk =>
                      if(claimDelta.taskType == TaskType.Core) {
                        var memToFree: Long = claimDelta.memStillNeeded
                        val jobsClaimDeltasToKill: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
                        simulator.cellState.claimDeltasPerMachine(i).foreach( cd =>
                          if(memToFree > 0 && cd.taskType == TaskType.Elastic){
                            memToFree -= cd.currentMem
                            jobsClaimDeltasToKill(cd.job.get) = jobsClaimDeltasToKill.getOrElse(cd.job.get, mutable.HashSet[ClaimDelta]()) + cd
                          }
                        )
                        if(memToFree <= 0){
                          claimDeltasOOMRisk += claimDelta
                          jobsClaimDeltasToKill.foreach{case(job1, elastics) =>
                            jobsClaimDeltasOOMKilled(job1) = jobsClaimDeltasOOMKilled.getOrElse(job1, mutable.HashSet[ClaimDelta]()) ++ elastics
                          }
                        }
                      }
                    case ClaimDeltaStatus.OOMKilled =>
                      if (claimDelta.taskType == TaskType.Elastic) {
                        jobsClaimDeltasOOMKilled(job) = jobsClaimDeltasOOMKilled.getOrElse(job, mutable.HashSet[ClaimDelta]()) + claimDelta
                      } else {
                        jobsClaimDeltasOOMKilled -= job
                        killJob(job)
                      }
                    case _ => None
                  }
                }
              }
            case None => None
          }
        })
      }

      jobsClaimDeltasOOMKilled.foreach { case (job, elastic) => killElasticClaimDeltas(job, elastic)}
      claimDeltasOOMRisk.foreach(claimDelta => {
        claimDelta.job match {
          case Some(job) =>
            if (job.finalStatus != JobStatus.Crashed && job.finalStatus != JobStatus.Completed &&
              job.finalStatus != JobStatus.Not_Scheduled) {

              val futureCpus = job.cpuUtilization(simulator.currentTime + reclaimResourcesPeriod)
              val futureMem = job.memoryUtilization(simulator.currentTime + reclaimResourcesPeriod)
              val currentCpus = job.cpuUtilization(simulator.currentTime)
              val currentMem = job.memoryUtilization(simulator.currentTime)

              val (slackCpu, slackMem): (Long, Long) = claimDelta.resize(simulator.cellState,
                Math.max(futureCpus, currentCpus), Math.max(futureMem, currentMem), resizePolicy = resizePolicy)
              cpuVariation += slackCpu
              memVariation += slackMem

              if(resizePolicy != ClaimDelta.ResizePolicy.None) {
                claimDelta.checkResourcesLimits(currentCpus, currentMem) match {
                  case ClaimDeltaStatus.OOMKilled =>
                    simulator.logger.warn(loggerPrefix + " How is it possible that the claimDelta is in OOMKilled if we just freed the resources for it?")
                  case _ => None
                }
              }
            }

          case None => None
        }
      })

//        simulator.logger.info(loggerPrefix + " Variation of " + cpuVariation + " CPUs and " + memVariation +
//          " memory. The global cell state now is (" + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

      // This control checks if we claimed some resources (negative values)
      if (cpuVariation < 0 || memVariation < 0) {
        simulator.logger.debug(loggerPrefix + " Getting up to check if I can schedule some jobs with the reclaimed resources")
        wakeUp()
      }

      checkingResourceUtilization = false
      if (reclaimResourcesPeriod > 0) {
        simulator.afterDelay(reclaimResourcesPeriod) {
          checkResourceUtilization()
        }
      }
    }
  }

}
