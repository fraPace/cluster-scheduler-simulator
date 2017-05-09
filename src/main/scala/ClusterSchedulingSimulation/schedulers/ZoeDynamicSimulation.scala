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
import ClusterSchedulingSimulation.utils.{Constant, Profiling}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.math

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

  val wakeUpInterval: Double = 60.0
  var checkingResourceUtilization: Boolean = false

  override def addRunningJob(job: Job): Unit = {
    super.addRunningJob(job)
    Profiling.time(checkResourceUtilization(), "checkResourceUtilization()")
//    checkResourceUtilization()
  }

  def checkResourceUtilization(): Unit = {
    if (!checkingResourceUtilization & numRunningJobs > 0) {
      checkingResourceUtilization = true

      val jobsClaimDeltasOOMKilled: mutable.HashMap[Job, (Int, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Int, mutable.HashSet[ClaimDelta])]()

      val (claimDeltasOOMRisk, claimDeltasOOMKilled,  cpuVariation, memVariation, cpuConflicts, memConflicts):
        (Set[ClaimDelta], Set[ClaimDelta], Long, Long, Long, Long) = reclaimNotUsedResources(simulator.cellState)

      val claimDeltasToSkip: mutable.HashMap[ClaimDelta, Int] = mutable.HashMap[ClaimDelta, Int]()

      if(claimDeltasOOMKilled.nonEmpty)
        simulator.logger.info(loggerPrefix + " Killed " + claimDeltasOOMKilled.size + " claim deltas because of OOM.")
      claimDeltasOOMKilled.foreach(claimDelta => {
        if(claimDelta.job.isDefined){
          claimDeltasToSkip(claimDelta) = 1
          val job = claimDelta.job.get
          var (inelastic, elastic) = jobsClaimDeltasOOMKilled.getOrElse(job, (0, mutable.HashSet[ClaimDelta]()))
          if(claimDelta.taskType == TaskType.Elastic) {
            elastic += claimDelta
          }else {
            inelastic += 1
          }
          jobsClaimDeltasOOMKilled(job) = (inelastic, elastic)
        }
      })
      if(claimDeltasOOMRisk.nonEmpty)
        simulator.logger.info(loggerPrefix + " " + claimDeltasOOMKilled.size + " claim deltas might be incur into a OOM kill.")
      claimDeltasOOMRisk.foreach(claimDelta => {
        if(claimDelta.job.isDefined && !claimDeltasToSkip.contains(claimDelta)){
          val job = claimDelta.job.get
          val (inelastic, elastic) = jobsClaimDeltasOOMKilled.getOrElse(job, (0, mutable.HashSet[ClaimDelta]()))
          if(claimDelta.taskType == TaskType.Elastic) {
            claimDeltasToSkip(claimDelta) = 1
            elastic += claimDelta
          }else{
            var memToBeFreed: Long = claimDelta.lastSlackMem.get
            simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).foreach(machineClaimDelta => {
              if(memToBeFreed > 0 && machineClaimDelta != claimDelta && !claimDeltasToSkip.contains(machineClaimDelta) && machineClaimDelta.taskType == TaskType.Elastic){
                claimDeltasToSkip(machineClaimDelta) = 1

                val job1 = machineClaimDelta.job.get
                val (inelastic1, elastic1) = jobsClaimDeltasOOMKilled.getOrElse(job1, (0, mutable.HashSet[ClaimDelta]()))
                elastic1 += machineClaimDelta
                jobsClaimDeltasOOMKilled(job1) = (inelastic1, elastic1)

                memToBeFreed -= machineClaimDelta.currentMem
              }
            })
            if(memToBeFreed > 0){
              simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).foreach(machineClaimDelta => {
                if(memToBeFreed > 0 && machineClaimDelta != claimDelta && !claimDeltasToSkip.contains(machineClaimDelta) && machineClaimDelta.taskType == TaskType.Core){
                  claimDeltasToSkip(machineClaimDelta) = 1

                  val job1 = machineClaimDelta.job.get
                  var (inelastic1, elastic1) = jobsClaimDeltasOOMKilled.getOrElse(job1, (0, mutable.HashSet[ClaimDelta]()))
                  inelastic1 += 1
                  jobsClaimDeltasOOMKilled(job1) = (inelastic1, elastic1)

                  memToBeFreed -= machineClaimDelta.currentMem
                }
              })
            }
          }
          jobsClaimDeltasOOMKilled(job) = (inelastic, elastic)
        }
      })


      jobsClaimDeltasOOMKilled.foreach{case (job, (inelastic, elastic)) =>
        lazy val jobPrefix = "[Job " + job.id + " (" + job.workloadName + ")] "
        lazy val elasticPrefix = "[Elastic]"
        if(inelastic > 0){
          if (job.finalStatus != JobStatus.Completed) {
            simulator.logger.info(loggerPrefix + jobPrefix + " A core component crashed. We are killing the application and rescheduling.")
            job.finalStatus = JobStatus.Crashed
            job.numJobCrashes += 1

            job.claimDeltas.foreach(_.unApply(simulator.cellState))

            // We have to remove all the incoming simulation events that work on this job.
            simulator.removeIf(x => x.itemId == job.id &&
              (x.eventType == EventType.Remove || x.eventType == EventType.Trigger))

            removePendingJob(job)
            removeRunningJob(job)

//            if (job.numJobCrashes < 100){
//              job.reset()
//              addPendingJob(job, prepend = true)
//            }else{
//              simulator.logger.warn(loggerPrefix + jobPrefix + " This job crashed more than 100 times. Not rescheduling it.")
//            }
            
            wakeUp()
          }
        }else if (elastic.nonEmpty) {
          simulator.logger.info(loggerPrefix + elasticPrefix + jobPrefix + " " + elastic.size + " elastic component crashed.")
          job.removeElasticClaimDeltas(elastic.toSeq)
          elastic.foreach(_.unApply(simulator.cellState))
          // We have to reinsert the job in queue if this had all it's elastic services allocated
          if (job.elasticTasksUnscheduled == 0) {
            addPendingJob(job, prepend = true)
          }

          val jobLeftDuration: Double = job.estimateJobDuration(currTime = simulator.currentTime, tasksRemoved = elastic.size)

          job.jobFinishedWorking = simulator.currentTime + jobLeftDuration

          // We have to remove all the incoming simulation events that work on this job.
          simulator.removeIf(x => x.itemId == job.id &&
            (x.eventType == EventType.Remove || x.eventType == EventType.Trigger))

          simulator.afterDelay(jobLeftDuration, eventType = EventType.Remove, itemId = job.id) {
            simulator.logger.info(loggerPrefix + jobPrefix + " Completed after " + (simulator.currentTime - job.jobStartedWorking) + "s.")
            job.finalStatus = JobStatus.Completed
            //                previousJob = null
            removePendingJob(job)
            removeRunningJob(job)
          }
          simulator.logger.info(loggerPrefix + elasticPrefix + jobPrefix + " Adding finished event after " + jobLeftDuration + " seconds to wake up scheduler.")
          simulator.cellState.scheduleEndEvents(job.claimDeltas.toSeq, delay = jobLeftDuration)
        }
      }


//      simulator.logger.info(loggerPrefix + " Variation of " + cpuVariation + " CPUs and " + memVariation +
//        " memory. The global cell state now is (" + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

      // This control checks if we claimed some resources (negative values)
      if (cpuVariation < 0 || memVariation < 0) {
//        simulator.logger.info(loggerPrefix + " Getting up to check if I can schedule some jobs with the reclaimed resources")
        wakeUp()
      }

      checkingResourceUtilization = false
      if (wakeUpInterval > 0) {
        simulator.afterDelay(wakeUpInterval) {
          Profiling.time(checkResourceUtilization(), "checkResourceUtilization()")
//          checkResourceUtilization()
        }
      }
    }
  }

}
