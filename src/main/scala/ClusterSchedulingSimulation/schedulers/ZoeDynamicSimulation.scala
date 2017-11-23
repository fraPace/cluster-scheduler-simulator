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

import javagp4zoe.{Prediction, TimeseriesPredictor}

import ClusterSchedulingSimulation.core.ClaimDelta.ResizePolicy
import ClusterSchedulingSimulation.core.ClaimDelta.ResizePolicy.ResizePolicy
import ClusterSchedulingSimulation.core.{TaskType, _}
import com.typesafe.scalalogging.LazyLogging
import gp.kernels.{KernelExp, KernelFunction}
import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.commons.math3.exception.NoDataException
import org.apache.commons.math3.random.Well19937c

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/* This class and its subclasses are used by factory method
 * ClusterSimulator.newScheduler() to determine which type of Simulator
 * to create and also to carry any extra fields that the factory needs to
 * construct the simulator.
 */
class ZoeDynamicSimulatorDesc(schedulerDescs: Seq[SchedulerDesc],
                              runTime: Double,
                              val allocationMode: AllocationMode.Value,
                              val policyMode: Policy.Modes.Value)
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
                          policyMode: Policy.Modes.Value)
  extends ZoeScheduler(name,
    constantThinkTimes,
    perTaskThinkTimes,
    numMachinesToBlackList,
    allocationMode,
    policyMode) with LazyLogging {
  logger.debug("scheduler-id-info: " + Thread.currentThread().getId + ", " + name + ", " + hashCode() + ", " + constantThinkTimes.mkString(";")
    + ", " + perTaskThinkTimes.mkString(";"))

  lazy val fclLoggerPrefix: String = "[FCL]"

  // This variable is used to checked if loops are running correctly at the given period
  // and to prevent running two events at the same time
  var previousCheckingResourceUtilizationTime: Double = -1

  val resizePolicy: ResizePolicy = ResizePolicy.Instant
  override val enableCellStateSnapshot: Boolean = false
  val reclaimResourcesPeriod: Int = 60
  val crashedAllowed = 5
//  val predictor: Oracle = Oracle(window = reclaimResourcesPeriod * 1, introduceError = false)
  val predictor: RealPredictor = RealPredictor(window = reclaimResourcesPeriod * 1)

  override def printExtraStats(prefix: String): Unit = {
    super.printExtraStats(prefix)
    logger.info(prefix + "[Stats][Oracle] Average Error: " + predictor.averageError + " Max Error: " + predictor.maxError + " Min Error: " + predictor.minError)
  }

  override def addJob(job: Job): Unit = {
//    val maxMemNeeded: Long = job.memoryUtilization.max
//    if(maxMemNeeded < job.memPerTask){
//      simulator.logger.info(loggerPrefix + job.loggerPrefix + " This job will use a lower memory (" + maxMemNeeded + ") than requested (" + job.memPerTask + ")")
//      job.memPerTask = maxMemNeeded
//    }

    super.addJob(job)
  }

  override def addRunningJob(job: Job): Unit = {
    super.addRunningJob(job)
    if (reclaimResourcesPeriod > 0 && previousCheckingResourceUtilizationTime == -1 && !job.disableResize) {
      previousCheckingResourceUtilizationTime = simulator.currentTime
      simulator.afterDelay(reclaimResourcesPeriod) {
        checkResourceUtilization()
      }
    }
  }

  def jobCrashed(job: Job): Unit = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " A core component crashed. We are killing the application and rescheduling.")
    job.numJobCrashes += 1
//    job.finalStatus = JobStatus.Crashed
    job.numTasksCrashed += job.claimDeltas.size

    if (job.numJobCrashes >= crashedAllowed){
      job.disableResize = true
      simulator.logger.info(loggerPrefix + job.loggerPrefix + " This job crashed more than " + crashedAllowed + " times (" + job.numJobCrashes + "). Disabling resize.")
    }

    killJob(job)
  }

  def killJob(job: Job): Unit = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " Killing the entire job.")

    //    val originalMemoryAvailable: Long = simulator.cellState.availableMem
//    var memoryFreed: Long = 0
    job.claimDeltas.foreach(cl => {
      cl.unApply(simulator.cellState)
      simulator.recordWastedResources(cl)
//      memoryFreed += cl.currentMem
    })
//    assert(originalMemoryAvailable + memoryFreed == simulator.cellState.availableMem, {
//      "The memory freed (" + memoryFreed + ") is not consistent with the memory that we have now in the cell (" +
//        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
//    })
    job.numTasksKilled += job.claimDeltas.size

    removeAllUpcomingEventsForJob(job)
    removePendingJob(job)
    removeRunningJob(job)
    job.reset()
    addPendingJob(job)
  }

  def elasticsClaimDeltasCrashed(job: Job, elastic: mutable.HashSet[ClaimDelta]): Unit = {
    if(elastic.isEmpty)
      return

    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Some (" + elastic.size + ") elastic components crashed.")

    job.numTasksCrashed += elastic.size
    killElasticClaimDeltas(job, elastic)
  }

  def killElasticClaimDeltas(job: Job, elastic: mutable.HashSet[ClaimDelta]): Unit = {
    if(elastic.isEmpty)
      return

    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Killing elastic components for this job: " +
      elastic.foldLeft("")((b, a) => b + " " + a.id))

    val addToPendingQueue: Boolean = job.elasticTasksUnscheduled == 0
//    val originalMemoryAvailable: Long = simulator.cellState.availableMem
//    var memoryFreed: Long = 0
    job.removeElasticClaimDeltas(elastic)
    elastic.foreach(cl => {
      cl.unApply(simulator.cellState)
      simulator.recordWastedResources(cl)
//      memoryFreed += cl.currentMem
    })
//    assert(originalMemoryAvailable + memoryFreed == simulator.cellState.availableMem, {
//      "The memory freed (" + memoryFreed + ") is not consistent with the memory that we have now in the cell (" +
//        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
//    })
    job.numTasksKilled += elastic.size

    updateJobFinishingEvents(job, elasticsRemoved = elastic)

    // We have to reinsert the job in queue if this had all it's elastic services allocated
    if (addToPendingQueue)
      addPendingJob(job)
  }

  def checkResourceUtilization(): Unit = {
    def maxLeft(job: Job): (Long, Long) = {
      var cpus: Long = 0
      var mem: Long = 0

      val windowTime = simulator.currentTime - reclaimResourcesPeriod + 1
      // We check if the oracle prediction is not going to be outside the job execution time
      var startTime = if(windowTime < job.jobStartedWorking) job.jobStartedWorking else windowTime

      while(startTime <= simulator.currentTime){
        var v = job.cpuUtilization(startTime)
        if(v > cpus)
          cpus = v

        v = job.memoryUtilization(startTime)
        if(v > mem)
          mem = v
        startTime += 1
      }

      (cpus, mem)
    }
    simulator.logger.debug(loggerPrefix + fclLoggerPrefix + " checkResourceUtilization called. The global cell state is ("
      + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

    assert(Math.round(simulator.currentTime - previousCheckingResourceUtilizationTime).toInt == reclaimResourcesPeriod, {
      "The difference (" + Math.round(simulator.currentTime - previousCheckingResourceUtilizationTime).toInt +
        ") between loops is not equal to the time interval set (" + reclaimResourcesPeriod + ")"
    })
    previousCheckingResourceUtilizationTime = simulator.currentTime

    // This data structure holds the job that should be killed because they died
    // job -> (toKillAll?, elasticComponentsToKill)
    val jobsClaimDeltasOOMKilled: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()

//    val jobsClaimDeltasOOMRisk_naive: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
//    val jobsClaimDeltasOOMRisk_lesserNaive: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
//    val machineIDsWithClaimDeltasOOMRisk: mutable.LinkedHashSet[Int] = mutable.LinkedHashSet[Int]()

    val claimDeltasOOMRiskPerMachine: Array[(mutable.LinkedHashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])] =
      Array.fill(simulator.cellState.numMachines)((mutable.LinkedHashSet[ClaimDelta](), mutable.LinkedHashSet[ClaimDelta]()))

    val machinesToCheckForOOMRisk: Array[Boolean] = Array.fill(simulator.cellState.numMachines)(false)

    var cpuVariation: Long = 0
    var memVariation: Long = 0
    val originalMemoryAvailable: Long = simulator.cellState.availableMem
    val originalCpusAvailable: Long = simulator.cellState.availableCpus

    var machineID: Int = 0
    while (machineID < simulator.cellState.numMachines) {
      var j: Int = 0
      while(j < simulator.cellState.claimDeltasPerMachine(machineID).length) {
        val claimDelta: ClaimDelta = simulator.cellState.claimDeltasPerMachine(machineID)(j)
        assert(simulator.cellState.claimDeltas.contains(claimDelta), {
          claimDelta.loggerPrefix + " This claimDelta is not inside the cluster. Then why are we processing it?"
        })
        claimDelta.job match {
          case Some(job) =>
            if (!job.disableResize &&
              job.finalStatus != JobStatus.Crashed && job.finalStatus != JobStatus.Completed && job.finalStatus != JobStatus.Not_Scheduled) {

              val (currentCpus, currentMem) = maxLeft(job)
              if(claimDelta.checkResourcesLimits(currentCpus, currentMem) == ClaimDeltaStatus.OOMKilled){
                val (core, elastics) = jobsClaimDeltasOOMKilled.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
                if (claimDelta.taskType == TaskType.Elastic) {
                  jobsClaimDeltasOOMKilled(job) = (core, elastics + claimDelta)
                } else {
                  jobsClaimDeltasOOMKilled(job) = (true, elastics)
                }
              }else{
                val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
                val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
                simulator.logger.debug(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix + " This claimDelta will use " +
                  resizeMem + " memory, %.2f".format(resizeMem / claimDelta.requestedMem.toDouble) + "% than originally requested")
                val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
                cpuVariation += slackCpu
                memVariation += slackMem

                if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
//                  machineIDsWithClaimDeltasOOMRisk.add(machineID)
                  machinesToCheckForOOMRisk(machineID) = true
//                  jobsClaimDeltasOOMRisk_lesserNaive(job) = jobsClaimDeltasOOMRisk_lesserNaive.getOrElse(job, mutable.HashSet[ClaimDelta]()) + claimDelta
                  val (cores, elastics) = claimDeltasOOMRiskPerMachine(machineID)
                  if(claimDelta.taskType == TaskType.Core) {
//                    jobsClaimDeltasOOMRisk_naive(job) = jobsClaimDeltasOOMRisk_naive.getOrElse(job, mutable.HashSet[ClaimDelta]()) + claimDelta
                    cores += claimDelta
                  }else{
                    elastics += claimDelta
                  }
                }
              }
            }
          case None => None
        }
        j += 1
      }
      machineID += 1
    }

    // Consistency checks to stop if something is wrong in the simulator
    assert(originalMemoryAvailable - memVariation == simulator.cellState.availableMem, {
      "The memory variation (" + memVariation + ") is not consistent with the memory that we have now in the cell (" +
        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
    })
    assert(originalCpusAvailable - cpuVariation == simulator.cellState.availableCpus, {
      "The cpus variation (" + cpuVariation + ") is not consistent with the cpus that we have now in the cell (" +
        simulator.cellState.availableCpus + ") considering that originally was " + originalCpusAvailable
    })

    jobsClaimDeltasOOMKilled.foreach { case (job, (coreDied, elastics)) =>
      if(coreDied){
        jobCrashed(job)
//        jobsClaimDeltasOOMRisk_lesserNaive.remove(job)
//        jobsClaimDeltasOOMRisk_naive.remove(job)
      } else if (elastics.nonEmpty){
        elasticsClaimDeltasCrashed(job, elastics)
      }
    }

//    val (slackCpu, slackMem) = preventCrashes_naiveApproach(jobsClaimDeltasOOMRisk_naive)
//    val (slackCpu, slackMem) = preventCrashes_naiveApproach(jobsClaimDeltasOOMRisk_naive, takeWhile = true)
//    val (slackCpu, slackMem) = preventCrashes_lesserNaiveApproach(jobsClaimDeltasOOMRisk_lesserNaive)
//    val (slackCpu, slackMem) = preventCrashes_lesserNaiveApproach(jobsClaimDeltasOOMRisk_lesserNaive, takeWhile = true)
//    val (slackCpu, slackMem) = preventCrashes_baseApproach(claimDeltasOOMRiskPerMachine)
//    val (slackCpu, slackMem) = preventCrashes_smarterApproach(machinesToCheckForOOMRisk)
//    val (slackCpu, slackMem) = preventCrashes_base3Approach(machinesToCheckForOOMRisk)

//    val (slackCpu, slackMem) = preventCrashes_finalApproach(machinesToCheckForOOMRisk)
//    cpuVariation += slackCpu
//    memVariation += slackMem

    // Consistency checks to stop if something is wrong in the simulator
    val memoryOccupied: Long = simulator.cellState.claimDeltas.foldLeft(0L)(_ + _.currentMem)
    assert(simulator.cellState.totalMem - memoryOccupied == simulator.cellState.availableMem, {
      "The available memory (" + (simulator.cellState.totalMem - memoryOccupied) + ") from all the allocated claimDeltas does not correspond to the one in the cell (" + simulator.cellState.availableMem + ")"
    })
    val cpuOccupied: Long = simulator.cellState.claimDeltas.foldLeft(0L)(_ + _.currentCpus)
    assert(simulator.cellState.totalCpus - cpuOccupied == simulator.cellState.availableCpus, {
      "The available cpus (" + (simulator.cellState.totalCpus - cpuOccupied) + ") from all the allocated claimDeltas does not correspond to the one in the cell (" + simulator.cellState.availableCpus + ")"
    })

    simulator.logger.debug(loggerPrefix + fclLoggerPrefix + " Variation of " + cpuVariation + " CPUs and " + memVariation +
      " memory. The global cell state now is (" + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

    // This control checks if we claimed some resources (negative values)
//    if (cpuVariation < 0 || memVariation < 0 || forceWakeUpScheduler) {
    if(pendingQueueSize > 0 && (originalCpusAvailable != simulator.cellState.availableCpus || originalMemoryAvailable != simulator.cellState.availableMem)){
      simulator.logger.info(loggerPrefix + fclLoggerPrefix + " Getting up to check if I can schedule some jobs with the reclaimed resources")
      wakeUp()
//      forceWakeUpScheduler = false
    }

    if (runningQueueSize > 0) {
      simulator.afterDelay(reclaimResourcesPeriod) {
        checkResourceUtilization()
      }
    } else previousCheckingResourceUtilizationTime = -1
  }

  def preventCrashes_baseApproach(claimDeltasOOMRiskPerMachine: Array[(mutable.LinkedHashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    var machineID: Int  = 0
    val claimDeltasKilledToSaveOOMRisk: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
    claimDeltasOOMRiskPerMachine.foreach{ case(cores, elastics) =>
      (cores ++ elastics).foreach(claimDelta => {
        if(!claimDeltasKilledToSaveOOMRisk.contains(claimDelta)){
          assert(machineID == claimDelta.machineID)
          val job: Job = claimDelta.job.get

          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem

          if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
//            lazy val allClaimDeltaPrefix = "[Machine: " + machineID + " | Size: " + simulator.cellState.claimDeltasPerMachine(machineID).length +
//              " | CLs:" + simulator.cellState.claimDeltasPerMachine(machineID).foldLeft("")((b, a) => b + " " + a.id) + "]"
//            simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + allClaimDeltaPrefix)
            simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix +
              " This claimDelta is STILL in OOMRisk and needs more memory (" + claimDelta.memStillNeeded + "), checking if we can find some space for it.")

            val jobsClaimDeltasToKill: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()
            var memToFree: Long = claimDelta.memStillNeeded

//            val sortedClaimDeltas = simulator.cellState.claimDeltasPerMachine(machineID).sortWith( (ob1, ob2) =>
//              Policy.Modes.compare(ob1.job.get, ob2.job.get , policyMode, simulator.currentTime) < 0)
//
//            sortedClaimDeltas.iterator.takeWhile(
//              cd => memToFree > 0 && Policy.Modes.compare(job, cd.job.get, policyMode, simulator.currentTime) <= 0
//            ).foreach(cd => {
//              memToFree -= (if (cd != claimDelta //cd.job.get.id != job.id
//                && cd.taskType == TaskType.Elastic) {
//                val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
//                jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
//                cd.currentMem
//              }else 0)
//            })
//
//            sortedClaimDeltas.iterator.takeWhile(
//              cd => memToFree > 0 && Policy.Modes.compare(job, cd.job.get, policyMode, simulator.currentTime) <= 0
//            ).foreach( cd => {
//              memToFree -= (if (cd.job.get.id != job.id
//                && cd.taskType == TaskType.Core) {
//                jobsClaimDeltasToKill(cd.job.get) = (true, mutable.HashSet[ClaimDelta]())
//                cd.currentMem
//              }else 0)
//            })

//            simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(_ => memToFree > 0).foreach(cd => {
//              memToFree -= (if (cd.job.get.id != job.id
//                && policy.compare(job, cd.job.get) <= 0
//                && cd.taskType == TaskType.Elastic) {
//                val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
//                jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
//                cd.currentMem
//              }else 0)
//            })
//
//            simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(_ => memToFree > 0).foreach( cd => {
//              memToFree -= (if (cd.job.get.id != job.id
//                && policy.compare(job, cd.job.get) <= 0
//                && cd.taskType == TaskType.Core) {
//                jobsClaimDeltasToKill(cd.job.get) = (true, mutable.HashSet[ClaimDelta]())
//                cd.currentMem
//              }else 0)
//            })

            simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(cd => memToFree > 0 && claimDelta.id != cd.id).foreach(cd => {
              memToFree -= (
//                if (policy.compare(job, cd.job.get) <= 0) {
                  if(cd.job.get.id != job.id){
                    if(cd.taskType == TaskType.Elastic){
                      val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
                      jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
                    }else if(cd.taskType == TaskType.Core){
                      jobsClaimDeltasToKill(cd.job.get) = (true, mutable.HashSet[ClaimDelta]())
                    }
                    cd.currentMem
                  }else{
                    if(cd.taskType == TaskType.Elastic){
                      val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
                      jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
                      cd.currentMem
                    }else 0
                  }
//                }else 0
              )
            })

//            if(memToFree > 0 && claimDelta.taskType == TaskType.Core){
//              simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
//                " ClaimDelta was in OOMRisk and we could NOT free some resources for it. We check if we can kill some of its elastics.")
//              simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(_ => memToFree > 0).foreach(cd => {
//                memToFree -= (if (cd.job.get.id == job.id
//                  && cd.taskType == TaskType.Elastic) {
//                  val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
//                  jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
//                  cd.currentMem
//                }else 0)
//              })
//            }

            if(memToFree > 0){
              if (claimDelta.taskType == TaskType.Core){
                simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
                  " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Killing the entire job!")
                claimDeltasKilledToSaveOOMRisk ++= job.claimDeltas
                killJob(job)
              }else{
                simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
                  " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Killing it!")
                claimDeltasKilledToSaveOOMRisk += claimDelta
                killElasticClaimDeltas(job, mutable.HashSet[ClaimDelta]() + claimDelta)
              }
            } else {
              jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
//                var memoryFreed: Long = 0
//                val originalMemoryAvailable: Long = simulator.cellState.availableMem
                if(coreDied){
//                  memoryFreed = job1.claimDeltas.foldLeft(0L)(_ + _.currentMem)
//                  simulator.logger.info(loggerPrefix + fclLoggerPrefix + job1.loggerPrefix +
//                    " Killing this job to make some space for others claimDeltas.")
                  claimDeltasKilledToSaveOOMRisk ++= job1.claimDeltas
                  killJob(job1)
                } else if (elastics1.nonEmpty){
//                  memoryFreed = elastics1.foldLeft(0L)(_ + _.currentMem)
//                  simulator.logger.info(loggerPrefix + fclLoggerPrefix + job1.loggerPrefix +
//                    " Killing elastic components of this job to make some space for others claimDeltas." +
//                    elastics1.foldLeft("")((b, a) => b + " " + a.id))
                  claimDeltasKilledToSaveOOMRisk ++= elastics1
                  killElasticClaimDeltas(job1, elastics1)
                }
//                assert(originalMemoryAvailable + memoryFreed == simulator.cellState.availableMem, {
//                  "The memory freed (" + memoryFreed + ") is not consistent with the memory that we have now in the cell (" +
//                    simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
//                })
              }

              val (slackCpu, slackMem): (Long, Long) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
              cpuVariation += slackCpu
              memVariation += slackMem
              assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
                "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it?"
              })
              simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix +
                " ClaimDelta was in OOMRisk and we could free some resources for it.")
            }
          } else {
            simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix +
              " ClaimDelta was in OOMRisk and we could free some resources for it.")
          }
        }
      })
      machineID += 1
    }

    (cpuVariation, memVariation)
  }

  def preventCrashes_finalApproach(machineIDsWithClaimDeltasOOMRisk: Array[Boolean]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    var machineID: Int  = 0
    while (machineID < simulator.cellState.numMachines){
      if(machineIDsWithClaimDeltasOOMRisk(machineID)){
        val jobsClaimDeltasToKill: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()

        // Create a data structure for faster access to a job and its running claimDelta on that machine
        val sortedJobs: mutable.TreeSet[Job] = mutable.TreeSet[Job]()(policy)
        val jobsClaimDeltasRunningOnMachine = mutable.HashMap[Job, (mutable.HashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])]()
        simulator.cellState.claimDeltasPerMachine(machineID).foreach(claimDelta => {
          val job = claimDelta.job.get
          sortedJobs.add(job)
          val (cores, elastics) = jobsClaimDeltasRunningOnMachine.getOrElse(job, (mutable.HashSet[ClaimDelta](), mutable.LinkedHashSet[ClaimDelta]()))
          if (claimDelta.taskType == TaskType.Core)
            cores.add(claimDelta)
          else if (claimDelta.taskType == TaskType.Elastic)
            elastics.add(claimDelta)
          jobsClaimDeltasRunningOnMachine(job) = (cores, elastics)
        })

        var memFree: Long = simulator.cellState.memPerMachine
        sortedJobs.foreach(job => {
          val (cores, elastics) = jobsClaimDeltasRunningOnMachine(job)
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)

          var tmpMemFree = memFree
          cores.foreach(cd => {
            val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
            tmpMemFree -= resizeMem
          })
          if(tmpMemFree < 0){
            jobsClaimDeltasToKill(job) = (true, mutable.HashSet[ClaimDelta]())
          }else{
            memFree = tmpMemFree
            elastics.foreach(cd => {
              val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
              tmpMemFree = memFree - resizeMem
              if(tmpMemFree < 0){
                val (c, e) = jobsClaimDeltasToKill.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
                jobsClaimDeltasToKill(job) = (c, e + cd)
              }else{
                memFree = tmpMemFree
              }
            })
          }
        })

        jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
          if(coreDied){
            killJob(job1)
          } else if (elastics1.nonEmpty){
            killElasticClaimDeltas(job1, elastics1)
          }
        }

        simulator.cellState.claimDeltasPerMachine(machineID).iterator.foreach(cd => {
          val job: Job = cd.job.get
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (resizeCpus, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem): (Long, Long) = cd.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem
          assert(cd.status != ClaimDeltaStatus.OOMRisk, {
            "How can the claimDelta be in OOMRisk(" + cd.memStillNeeded + ") if we just freed up the resources for it?"
          })
        })
      }

      machineID += 1
    }

    (cpuVariation, memVariation)
  }

  def preventCrashes_base3Approach(machineIDsWithClaimDeltasOOMRisk: Array[Boolean]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    var machineID: Int  = 0
    while (machineID < simulator.cellState.numMachines){
      if(machineIDsWithClaimDeltasOOMRisk(machineID)){
        val jobsClaimDeltasToKill: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()

        var memFree: Long = simulator.cellState.memPerMachine
        simulator.cellState.claimDeltasPerMachine(machineID).iterator.foreach(cd => {
          val job: Job = cd.job.get
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)

          val tmpMemFree: Long = memFree - resizeMem
          if(tmpMemFree >= 0){
            memFree = tmpMemFree
          }else{
            if(cd.taskType == TaskType.Elastic){
              val (c, e) = jobsClaimDeltasToKill.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
              jobsClaimDeltasToKill(job) = (c, e + cd)
            }else if(cd.taskType == TaskType.Core){
              jobsClaimDeltasToKill(job) = (true, mutable.HashSet[ClaimDelta]())
            }
          }
        })

        jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
          if(coreDied){
            killJob(job1)
          } else if (elastics1.nonEmpty){
            killElasticClaimDeltas(job1, elastics1)
          }
        }

        simulator.cellState.claimDeltasPerMachine(machineID).iterator.foreach(cd => {
          val job: Job = cd.job.get
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (resizeCpus, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem): (Long, Long) = cd.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem
          assert(cd.status != ClaimDeltaStatus.OOMRisk, {
            "How can the claimDelta be in OOMRisk(" + cd.memStillNeeded + ") if we just freed up the resources for it?"
          })
        })
      }

      machineID += 1
    }

    (cpuVariation, memVariation)
  }

  def preventCrashes_naiveApproach(jobsClaimDeltasOOMRisk: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]], takeWhile: Boolean = false): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    val claimDeltasKilledToSaveOOMRisk: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
    jobsClaimDeltasOOMRisk.foreach{case (job, elastics) =>
      val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
      elastics.foreach(claimDelta => {
        if(!claimDeltasKilledToSaveOOMRisk.contains(claimDelta)){
          val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem

          if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
            if(claimDelta.taskType == TaskType.Core) {
              var memToFree: Long = claimDelta.memStillNeeded
              val jobsClaimDeltasToKill: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
              lazy val allClaimDeltaPrefix = "[Machine: " + claimDelta.machineID + " | Size: " + simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).length +
                " | CLs:" + simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).foldLeft("")((b, a) => b + " " + a.id) + "]"
              simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + allClaimDeltaPrefix)
              (if(takeWhile)
                simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).reverseIterator.takeWhile(_ != claimDelta)
              else
                simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).reverseIterator
              ).foreach( cd => {
                if (cd != claimDelta && memToFree > 0 && cd.taskType == TaskType.Elastic) {
                  memToFree -= cd.currentMem
                  jobsClaimDeltasToKill(cd.job.get) = jobsClaimDeltasToKill.getOrElse(cd.job.get, mutable.HashSet[ClaimDelta]()) + cd
                }
              })
              if(memToFree <= 0){
                jobsClaimDeltasToKill.foreach { case (job1, elastics1) =>
                  killElasticClaimDeltas(job1, elastics1)
                  claimDeltasKilledToSaveOOMRisk ++= elastics1
                }
                assert(!claimDeltasKilledToSaveOOMRisk.contains(claimDelta), {
                  "I killed myself while I was trying to find resources to stay alive? It makes no sense human!"
                })
                val (slackCpu, slackMem): (Long, Long) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
                cpuVariation += slackCpu
                memVariation += slackMem
                assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
                  "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it?"
                })
                simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + " Claim Delta was in OOMRisk and we could free some resources for it")
              }else if (claimDelta.taskType == TaskType.Core)
                simulator.logger.warn(loggerPrefix + claimDelta.job.get.loggerPrefix + " ClaimDelta was in OOMRisk and we could NOT free some resources for it. This job will soon crash!")
              else
                simulator.logger.warn(loggerPrefix + claimDelta.job.get.loggerPrefix + " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Fortunately it was an elastic component, so the job will not crash.")
            }
          }
        }
      })
    }
    (cpuVariation, memVariation)
  }

  def preventCrashes_lesserNaiveApproach(jobsClaimDeltasOOMRisk: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]], takeWhile: Boolean = false): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    val claimDeltasKilledToSaveOOMRisk: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
    jobsClaimDeltasOOMRisk.foreach{case (job, elastics) =>
      val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
      elastics.foreach(claimDelta => {
        if(!claimDeltasKilledToSaveOOMRisk.contains(claimDelta)){
          val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem

          if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
            lazy val allClaimDeltaPrefix = "[Machine: " + claimDelta.machineID + " | Size: " + simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).length +
              " | CLs:" + simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).foldLeft("")((b, a) => b + " " + a.id) + "]"
            simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + allClaimDeltaPrefix)

            var memToFree: Long = claimDelta.memStillNeeded
            val jobsClaimDeltasToKill: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]] = mutable.HashMap[Job, mutable.HashSet[ClaimDelta]]()
            (if(takeWhile)
              simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).reverseIterator.takeWhile(_ != claimDelta)
            else
              simulator.cellState.claimDeltasPerMachine(claimDelta.machineID).reverseIterator
              ).foreach( cd => {
              if (cd != claimDelta && memToFree > 0  && cd.taskType == TaskType.Elastic) {
                memToFree -= cd.currentMem
                jobsClaimDeltasToKill(cd.job.get) = jobsClaimDeltasToKill.getOrElse(cd.job.get, mutable.HashSet[ClaimDelta]()) + cd
              }
            })
            if(memToFree <= 0){
              jobsClaimDeltasToKill.foreach { case (job1, elastics1) =>
                killElasticClaimDeltas(job1, elastics1)
                claimDeltasKilledToSaveOOMRisk ++= elastics1
              }
              assert(!claimDeltasKilledToSaveOOMRisk.contains(claimDelta), {
                "I killed myself while I was trying to find resources to stay alive? It makes no sense human!"
              })
              val (slackCpu, slackMem): (Long, Long) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
              cpuVariation += slackCpu
              memVariation += slackMem
              assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
                "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it?"
              })
              simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + " ClaimDelta was in OOMRisk and we could free some resources for it")
            }else if (claimDelta.taskType == TaskType.Core)
              simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + " ClaimDelta was in OOMRisk and we could NOT free some resources for it. This job will soon crash!")
            else
              simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Fortunately it was an elastic component, so the job will not crash.")
          }
        }
      })
    }
    (cpuVariation, memVariation)
  }

  def preventCrashes_smarterApproach(machineIDsWithClaimDeltasOOMRisk: Array[Boolean]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    var machineID: Int  = 0
    while (machineID < simulator.cellState.numMachines){
      if(machineIDsWithClaimDeltasOOMRisk(machineID)) {
        // Create a data structure for faster access to a job and its running claimDelta on that machine
        val sortedJobs: mutable.TreeSet[Job] = mutable.TreeSet[Job]()(policy)
        val jobsClaimDeltasRunningOnMachine = mutable.HashMap[Job, (mutable.HashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])]()
        simulator.cellState.claimDeltasPerMachine(machineID).foreach(claimDelta => {
          val job = claimDelta.job.get
          sortedJobs.add(job)
          val (cores, elastics) = jobsClaimDeltasRunningOnMachine.getOrElse(job, (mutable.HashSet[ClaimDelta](), mutable.LinkedHashSet[ClaimDelta]()))
          if (claimDelta.taskType == TaskType.Core)
            cores.add(claimDelta)
          else if (claimDelta.taskType == TaskType.Elastic)
            elastics.add(claimDelta)
          jobsClaimDeltasRunningOnMachine(job) = (cores, elastics)
        })

        simulator.logger.debug(loggerPrefix + "[Machine: " + machineID + "] There are " +
          simulator.cellState.claimDeltasPerMachine(machineID).count(_.status == ClaimDeltaStatus.OOMRisk) + " claimDeltas in OOMRisk over " +
          simulator.cellState.claimDeltasPerMachine(machineID).size)

        var memoryFree: Long = simulator.cellState.memPerMachine
        var currentMemoryFree: Long = memoryFree

        val jobsClaimDeltasToKeep: ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])] =
          new ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])]()
        val tmpJobsClaimDeltasToKeep: ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])] =
          new ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])]()
        var stop: Boolean = false
        sortedJobs.iterator.takeWhile(_ => !stop).foreach(job => {
          // Create a copy
          tmpJobsClaimDeltasToKeep.clear()
          jobsClaimDeltasToKeep.foreach { case (job1, cores1, elastics1) =>
            tmpJobsClaimDeltasToKeep += ((job1, mutable.HashSet[ClaimDelta]() ++ cores1, mutable.HashSet[ClaimDelta]() ++ elastics1))
          }


          // START ALGORITHM PRESENTED IN THE PAPER
          // Remove all elastic components from the machine
          jobsClaimDeltasToKeep.foreach { case (job1, _, elastics1) =>
            elastics1.foreach( cd => {
              val (cpu, mem) = predictor.predictFutureResourceUtilization(job1, simulator.currentTime)
              val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
              currentMemoryFree += resizeMem
            })
            elastics1.clear()
          }
          assert(currentMemoryFree <= simulator.cellState.memPerMachine)

          var claimDelta_core = mutable.HashSet[ClaimDelta]()
          var tmpMemory = currentMemoryFree
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (cores, _) = jobsClaimDeltasRunningOnMachine(job)
          cores.foreach(cd => {
            val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
            tmpMemory -= resizeMem
          })
          // We check if we managed to put back all the core, otherwise we already know that the job will die
          //     therefore no point in wasting resources with elastic components
          // By checking if the free memory is >= 0, we also consider the case where no core components are allocated
          //     on that machine
          if (tmpMemory >= 0) {
            currentMemoryFree = tmpMemory
            claimDelta_core ++= cores
            jobsClaimDeltasToKeep += ((job, claimDelta_core, mutable.HashSet[ClaimDelta]()))
          }

          jobsClaimDeltasToKeep.iterator.takeWhile(_ => currentMemoryFree > 0).foreach { case (job1, _, elastics1) =>
            val (cpu1, mem1) = predictor.predictFutureResourceUtilization(job1, simulator.currentTime)
            val (_, elastics) = jobsClaimDeltasRunningOnMachine(job1)
            elastics.iterator.takeWhile(_ => currentMemoryFree > 0).foreach(cd => {
              val (_, resizeMem) = cd.calculateNextAllocations(cpu1, mem1, resizePolicy = resizePolicy)
              if (currentMemoryFree >= resizeMem) {
                currentMemoryFree -= resizeMem
                elastics1 += cd
              }
            })
          }
          if (currentMemoryFree > memoryFree) {
            stop = true
            jobsClaimDeltasToKeep.clear()
            jobsClaimDeltasToKeep ++= tmpJobsClaimDeltasToKeep
          }
          memoryFree = currentMemoryFree
          // FINISH ALGORITHM PRESENTED IN THE PAPER
        })
        simulator.logger.debug(loggerPrefix + "[Machine: " + machineID + "] After simulation we have " +
          jobsClaimDeltasToKeep.foldLeft(0)((b, a) => b + a._2.size + a._3.size) + " claimDeltas")

        // Time to kill the claimDelta that are no longer allocated for a job
        jobsClaimDeltasToKeep.foreach { case (job, cores, elastics) =>
          val (originalCores, originalElastics) = jobsClaimDeltasRunningOnMachine.remove(job).get
          // We check if the new allocation for the job has less core components than the original
          //     this means that a core component was not allocated, therefore we kill the application
          if (originalCores.size > cores.size) {
            killJob(job)
            originalElastics.clear()
          }
          if(originalElastics.nonEmpty)
            killElasticClaimDeltas(job, mutable.HashSet[ClaimDelta]() ++ (originalElastics -- elastics))
        }
        // Time to kill the jobs that are no longer allocated
        jobsClaimDeltasRunningOnMachine.foreach { case (job, (cores, elastics)) =>
          if (cores.nonEmpty) {
            killJob(job)
            elastics.clear()
          }
          if (elastics.nonEmpty)
            killElasticClaimDeltas(job, mutable.HashSet[ClaimDelta]() ++ elastics)
        }

        // Now we can resize the claimDeltas so that they exit the status of OOMRisk
        jobsClaimDeltasToKeep.foreach { case (job, cores, elastics) =>
          cores.foreach(cd => {
            val (slackCpu, slackMem) = resizeClaimDelta(job, cd)
            cpuVariation += slackCpu
            memVariation += slackMem
          })
          elastics.foreach(cd => {
            val (slackCpu, slackMem) = resizeClaimDelta(job, cd)
            cpuVariation += slackCpu
            memVariation += slackMem
          })
        }

        def resizeClaimDelta(job: Job, claimDelta: ClaimDelta): (Long, Long) = {
          val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
          val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
            "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it?"
          })
          (slackCpu, slackMem)
        }
      }
    machineID += 1
    }

    (cpuVariation, memVariation)
  }

}

class Oracle(window: Int, introduceError: Boolean = false) extends LazyLogging{
  val mu: Double = 0
  val sigma: Double = 0.062
  val errorDistribution: LogNormalDistribution = new LogNormalDistribution(
    new Well19937c(0), mu, sigma, LogNormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY)

  // We use these structure to cache the information so that we get the same one if we call the functions
  //     multiple times at the same event time
//  val errorsCache: mutable.HashMap[Long, (Double, Double)] = mutable.HashMap[Long, (Double, Double)]()
  val predictionCache: mutable.HashMap[Long, (Double, (Long, Long))] = mutable.HashMap[Long, (Double, (Long, Long))]()

  var errors: ArrayBuffer[Float] = ArrayBuffer[Float]()
  var predictions: Long = 0
  def averageError: Float = if(errors.nonEmpty) (errors.sum / predictions.toDouble).toFloat else 0F
  def maxError: Float = if(errors.nonEmpty) errors.max else 0F
  def minError: Float = if(errors.nonEmpty) errors.min else 0F

  def getError(jobID: Long, currentTime: Double): Double = {
    def generateError(factor: Double = 1): Double = {
      val ret = factor * errorDistribution.sample()
      ret
    }

    if(introduceError){
//      // We look for a cached value, otherwise we get a new one
//      var (time, _error) = errorsCache.getOrElse(jobID, (currentTime, -1.0))
//      if(time != currentTime || _error == -1.0)
//        _error = generateError()
//      errorsCache(jobID) = (currentTime, _error)
//      _error
      generateError()
    }else
      1.0
  }

  def predictFutureResourceUtilization(job: Job, currentTime: Double): (Long, Long) = {
    def maxRight(job: Job): ((Int, Long), (Int, Long)) = {
      var cpus: Long = 0
      var idxCpus: Int = 0
      var mem: Long = 0
      var idxMem: Int = 0

      val jobFinishTime = currentTime + job.remainingTime
      val windowTime = currentTime + window
      // We check if the oracle prediction is not going to be outside the job execution time
      val finalTime = if(jobFinishTime < windowTime) jobFinishTime else windowTime

      var time = currentTime
      while(time <= finalTime){
        var v = job.cpuUtilization(time)
        if(v > cpus){
          cpus = v
          idxCpus = (time - currentTime).toInt
        }

        v = job.memoryUtilization(time)
        if(v > mem){
          mem = v
          idxMem = (time - currentTime).toInt
        }
        time += 1
      }

      ((idxCpus, cpus), (idxMem, mem))
    }

    if(job.disableResize)
      return (job.cpusPerTask, job.memPerTask)

    val jobID = job.id

    // We look for a cached prediction value, otherwise we get a new one
    var (time, (cpus, mem)) = predictionCache.getOrElse(jobID, (currentTime, (-1L, -1L)))
    if(time != currentTime || cpus == -1L || mem == -1L){
      var ((_idxCpus, _cpus), (_idxMem, _mem)) = maxRight(job)
      val error: Double = getError(jobID, currentTime)
      if(error != 1.0){
        var factorCpus = 0.0
        var factorMem = 0.0
        if(error < 1){
          factorCpus = -(_idxCpus / 5000.0)
          factorMem = -(_idxMem / 5000.0)
        }else{
          factorCpus = _idxCpus / 5000.0
          factorMem = _idxMem / 5000.0
        }

        if(Math.abs(1 - (error + factorMem)) > ClaimDelta.ResizePolicy.SafeMargin)
          logger.warn("Oracle Error (" + Math.abs(1 - (error + factorMem)) + ") is above the safe ClaimDelta margin " + ClaimDelta.ResizePolicy.SafeMargin)
        errors += Math.abs(1 - (error + factorMem)).toFloat

        _cpus = (_cpus * (error + factorCpus)).toLong
        _mem = (_mem * (error + factorMem)).toLong
      }
      cpus = _cpus
      mem = _mem
      predictionCache(jobID) = (currentTime, (cpus, mem))
      predictions += 1
    }
    (cpus, mem)
  }
}

object Oracle {
  def apply(window: Int, introduceError: Boolean): Oracle = new Oracle(window, introduceError)
}

class RealPredictor(window: Int) extends LazyLogging{
  // We use these structure to cache the information so that we get the same one if we call the functions
  //     multiple times at the same event time
  //  val errorsCache: mutable.HashMap[Long, (Double, Double)] = mutable.HashMap[Long, (Double, Double)]()
  val predictionCache: mutable.HashMap[Long, (Double, (Long, Long))] = mutable.HashMap[Long, (Double, (Long, Long))]()

  var errors: ArrayBuffer[Float] = ArrayBuffer[Float]()
  var predictions: Long = 0
  def averageError: Float = if(errors.nonEmpty) (errors.sum / predictions.toDouble).toFloat else 0F
  def maxError: Float = if(errors.nonEmpty) errors.max else 0F
  def minError: Float = if(errors.nonEmpty) errors.min else 0F

  val predictorsCpu: mutable.HashMap[Long, TimeseriesPredictor] = mutable.HashMap[Long, TimeseriesPredictor]()
  val predictorsMem: mutable.HashMap[Long, TimeseriesPredictor] = mutable.HashMap[Long, TimeseriesPredictor]()
  def predictFutureResourceUtilization(job: Job, currentTime: Double): (Long, Long) = {
    def maxRight(job: Job): ((Int, Long), (Int, Long)) = {
      var cpus: Long = 0
      var idxCpus: Int = 0
      var mem: Long = 0
      var idxMem: Int = 0

      val jobFinishTime = currentTime + job.remainingTime
      val windowTime = currentTime + window
      // We check if the oracle prediction is not going to be outside the job execution time
      val finalTime = if(jobFinishTime < windowTime) jobFinishTime else windowTime

      var time = currentTime
      while(time <= finalTime){
        var v = job.cpuUtilization(time)
        if(v > cpus){
          cpus = v
          idxCpus = (time - currentTime).toInt
        }

        v = job.memoryUtilization(time)
        if(v > mem){
          mem = v
          idxMem = (time - currentTime).toInt
        }
        time += 1
      }

      ((idxCpus, cpus), (idxMem, mem))
    }

    def maxLeft(job: Job): (Long, Long) = {
      var cpus: Long = 0
      var mem: Long = 0

      val windowTime = currentTime - window + 1
      // We check if the oracle prediction is not going to be outside the job execution time
      var startTime = if(windowTime < job.jobStartedWorking) job.jobStartedWorking else windowTime

      while(startTime <= currentTime){
        var v = job.cpuUtilization(startTime)
        if(v > cpus){
          cpus = v
        }

        v = job.memoryUtilization(startTime)
        if(v > mem){
          mem = v
        }
        startTime += 1
      }

      (cpus, mem)
    }

    if(job.disableResize)
      return (job.cpusPerTask, job.memPerTask)
    val jobID = job.id

    // We look for a cached prediction value, otherwise we get a new one
    var (time, (cpus, mem)) = predictionCache.getOrElse(jobID, (currentTime, (-1L, -1L)))
    if(time != currentTime || cpus == -1L || mem == -1L){
      val ((_, _), (_, _futureMem)) = maxRight(job)

      val (_cpus, _mem) = maxLeft(job)

      val predictorMem = predictorsMem.getOrElse(jobID, new TimeseriesPredictor(new KernelExp, 20, 1, 100, 0))
      val predictorCpu = predictorsCpu.getOrElse(jobID, new TimeseriesPredictor(new KernelExp, 20, 1, 100, 0))

      try{
        predictorMem.addTimeseriesObservations(Array[Double](_mem))
        predictorCpu.addTimeseriesObservations(Array[Double](_cpus))

        val predictionMem:Array[Prediction] = predictorMem.predict()
        mem = predictionMem(0).getMean.toLong + (Math.sqrt(predictionMem(0).getVariance).toLong * 3)
        val predictionCpu:Array[Prediction] = predictorCpu.predict()
        cpus = predictionCpu(0).getMean.toLong


        val error = if(mem == 0){
          Math.abs(1 - (_futureMem / 1.0)).toFloat
        }
        else{
          Math.abs(1 - (_futureMem / mem.toDouble)).toFloat
        }

        //        if(error > ClaimDelta.ResizePolicy.SafeMargin)
        //          logger.warn("Oracle Error (" + error + ") is above the safe ClaimDelta margin " + ClaimDelta.ResizePolicy.SafeMargin)
        errors += error
      }catch {
        case e: NoDataException =>
          mem = job.memPerTask
          cpus = job.cpusPerTask
      }

      predictorsCpu(jobID) = predictorCpu
      predictorsMem(jobID) = predictorMem

      //      logger.info(job.loggerPrefix + " Time: " + currentTime + " Predicted Value: " + mem + " Future Real Value: " + _futureMem + " Current Value: " + _mem)

      predictionCache(jobID) = (currentTime, (cpus, mem))
      predictions += 1
    }
    (cpus, mem)
  }
}

object RealPredictor {
  def apply(window: Int): RealPredictor = new RealPredictor(window)
}
