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
import ClusterSchedulingSimulation.core.{TaskType, _}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import util.control.Breaks._

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

  override val enableCellStateSnapshot: Boolean = false

  val reclaimResourcesPeriod: Int = 1
  lazy val fclLoggerPrefix: String = "[FCL]"

  // This variable is used to checked if loops are running correctly at the given period
  // and to prevent running two events at the same time
  var previousCheckingResourceUtilizationTime: Double = -1

  val crashedAllowed = 1

  val resizePolicy: ResizePolicy = ResizePolicy.Instant

  var forceWakeUpScheduler:Boolean = false

  override def addJob(job: Job): Unit = {
    val maxMemNeeded: Long = job.memoryUtilization.max
    if(maxMemNeeded < job.memPerTask){
      simulator.logger.info(loggerPrefix + job.loggerPrefix + " This job will use a lower memory (" + maxMemNeeded + ") than requested (" + job.memPerTask + ")")
      job.memPerTask = maxMemNeeded
    }

    super.addJob(job)
  }

  override def addRunningJob(job: Job): Unit = {
    super.addRunningJob(job)
    if (reclaimResourcesPeriod > 0 && previousCheckingResourceUtilizationTime == -1 && !job.disableResize) {
      previousCheckingResourceUtilizationTime = simulator.currentTime
      simulator.afterDelay(reclaimResourcesPeriod) {
//        isCheckingResourceUtilizationActive = true
        checkResourceUtilization()
      }
    }
  }


//  override def scheduleJob(job: Job, cellState: CellState, taskType: TaskType.Value): ListBuffer[ClaimDelta] = {
//    assert(simulator != null)
//    assert(cellState != null)
//    assert(job.cpusPerTask <= cellState.cpusPerMachine, {
//      "Looking for machine with " + job.cpusPerTask + " CPUs, but machines only have " + cellState.cpusPerMachine + " CPUs."
//    })
//    assert(job.memPerTask <= cellState.memPerMachine, {
//      "Looking for machine with " + job.memPerTask + " mem, but machines only have " + cellState.memPerMachine + " mem."
//    })
//    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()
//
//    // Cache candidate pools in this scheduler for performance improvements.
////    val candidatePool = candidatePoolCache.getOrElseUpdate(cellState.numMachines, Array.range(0, cellState.numMachines))
//
//    var numRemainingTasks = taskType match {
//      case TaskType.Core => job.coreTasksUnscheduled
//      case TaskType.Elastic => job.elasticTasksUnscheduled
//      case TaskType.None => job.tasksUnscheduled
//    }
//
//    val remainingCandidates = math.max(0, cellState.numMachines - numMachinesToBlackList).toInt
//    var allocated: Boolean = true
//    while (numRemainingTasks > 0 && allocated) {
//      val candidatePool = if(job.cpusPerTask / cellState.cpusPerMachine.toDouble >= job.memPerTask / cellState.memPerMachine.toDouble) {
//        cellState.allocatedCpusPerMachine.zipWithIndex.sortWith(_._1 < _._1)
//      } else {
//        cellState.allocatedMemPerMachine.zipWithIndex.sortWith(_._1 < _._1)
//      }
//      // Pick a random machine out of the remaining pool, i.e., out of the set
//      // of machineIDs in the first remainingCandidate slots of the candidate
//      // pool.
//      var candidateIndex: Int = 0
//      allocated = false
//      while(candidateIndex < remainingCandidates && !allocated){
//        val currMachID = candidatePool(candidateIndex)._2.toInt
//        // If one of this job's tasks will fit on this machine, then assign
//        // to it by creating a claimDelta and leave it in the candidate pool.
//        if (cellState.availableCpusPerMachine(currMachID) >= job.cpusPerTask &&
//          cellState.availableMemPerMachine(currMachID) >= job.memPerTask) {
//          assert(currMachID >= 0 && currMachID < cellState.machineSeqNums.length)
//          val claimDelta = ClaimDelta(this,
//            currMachID,
//            cellState.machineSeqNums(currMachID),
//            job.taskDuration,
//            job.cpusPerTask,
//            job.memPerTask,
//            job = Option(job),
//            taskType = taskType)
//          claimDelta.apply(cellState = cellState)
//          claimDeltas += claimDelta
//          numRemainingTasks -= 1
//          allocated = true
//        } else {
//          failedFindVictimAttempts += 1
//          // Move the chosen candidate to the end of the range of
//          // remainingCandidates so that we won't choose it again after we
//          // decrement remainingCandidates. Do this by swapping it with the
//          // machineID currently at position (remainingCandidates - 1)
//          //        candidatePool(candidateIndex) = candidatePool(remainingCandidates - 1)
//          //        candidatePool(remainingCandidates - 1) = currMachID
////          remainingCandidates -= 1
//          simulator.logger.debug(name + " in scheduling algorithm, tried machine " + currMachID + ", but " + job.cpusPerTask + " CPUs and " + job.memPerTask +
//            " mem are required, and it only has " + cellState.availableCpusPerMachine(currMachID) + " cpus and " + cellState.availableMemPerMachine(currMachID) + " mem available.")
//        }
//        candidateIndex += 1
//      }
//    }
//    claimDeltas
//  }

  def predictFutureResourceUtilization(job: Job): (Long, Long) = {
    val futureCpus = job.cpuUtilization(simulator.currentTime + reclaimResourcesPeriod)
    val futureMem = job.memoryUtilization(simulator.currentTime + reclaimResourcesPeriod)
    (
      Math.max(job.cpuUtilization(simulator.currentTime), futureCpus),
      Math.max(job.memoryUtilization(simulator.currentTime), futureMem)
    )
  }

  def jobCrashed(job: Job): Unit = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " A core component crashed. We are killing the application and rescheduling.")
    job.numJobCrashes += 1
//    job.finalStatus = JobStatus.Crashed
    job.numTasksCrashed += job.claimDeltas.size

    if (job.numJobCrashes >= crashedAllowed){
      job.disableResize = true
      simulator.logger.warn(loggerPrefix + job.loggerPrefix + " This job crashed more than " + crashedAllowed + " times (" + job.numJobCrashes + "). Disabling resize.")
    }

    killJob(job)
  }

  def killJob(job: Job): Unit = {
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
    forceWakeUpScheduler = true
  }

  def elasticsClaimDeltasCrashed(job: Job, elastic: mutable.HashSet[ClaimDelta]): Unit = {
    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + elastic.size + " elastic components crashed: "
      + elastic.foldLeft("")((b, a) => b + " " + a.id))

    job.numTasksCrashed += elastic.size
    killElasticClaimDeltas(job, elastic)
  }

  def killElasticClaimDeltas(job: Job, elastic: mutable.HashSet[ClaimDelta]): Unit = {
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
    if (addToPendingQueue) {
      addPendingJob(job)
      forceWakeUpScheduler = true
    }
  }

  def checkResourceUtilization(): Unit = {
    simulator.logger.info(loggerPrefix + fclLoggerPrefix + " checkResourceUtilization called. The global cell state is ("
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
//    val machineIDsWithClaimDeltasOOMRisk: mutable.HashSet[Int] = mutable.HashSet[Int]()

    val claimDeltasOOMRiskPerMachine: Array[(mutable.LinkedHashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])] =
      Array.fill(simulator.cellState.numMachines)((mutable.LinkedHashSet[ClaimDelta](), mutable.LinkedHashSet[ClaimDelta]()))

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
          claimDelta.loggerPrefix + " This claimDelta is not inside the cluster. Then why are we processing?"
        })
        claimDelta.job match {
          case Some(job) =>
            if (!job.disableResize &&
              job.finalStatus != JobStatus.Crashed && job.finalStatus != JobStatus.Completed && job.finalStatus != JobStatus.Not_Scheduled) {

              if(claimDelta.checkResourcesLimits(job.cpuUtilization(simulator.currentTime), job.memoryUtilization(simulator.currentTime)) == ClaimDeltaStatus.OOMKilled){
                val (_, elastics) = jobsClaimDeltasOOMKilled.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
                if (claimDelta.taskType == TaskType.Elastic) {
                  jobsClaimDeltasOOMKilled(job) = (false, elastics + claimDelta)
                } else {
                  jobsClaimDeltasOOMKilled(job) = (true, elastics)
                }
              }else{
                val (cpu, mem) = predictFutureResourceUtilization(job)
                val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
                val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
                cpuVariation += slackCpu
                memVariation += slackMem

                if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
//                  machineIDsWithClaimDeltasOOMRisk.add(machineID)
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
//    val (slackCpu, slackMem) = preventCrashes_smarterApproach(machineIDsWithClaimDeltasOOMRisk)
    val (slackCpu, slackMem) = preventCrashes_baseApproach(claimDeltasOOMRiskPerMachine)
    cpuVariation += slackCpu
    memVariation += slackMem


    simulator.logger.info(loggerPrefix + fclLoggerPrefix + " Variation of " + cpuVariation + " CPUs and " + memVariation +
      " memory. The global cell state now is (" + simulator.cellState.availableCpus + " CPUs, " + simulator.cellState.availableMem + " mem)")

    // Consistency checks to stop if something is wrong in the simulator
    val memoryOccupied: Long = simulator.cellState.claimDeltas.foldLeft(0L)(_ + _.currentMem)
    assert(simulator.cellState.totalMem - memoryOccupied == simulator.cellState.availableMem, {
      "The available memory (" + (simulator.cellState.totalMem - memoryOccupied) + ") from all the allocated claimDeltas does not correspond to the one in the cell (" + simulator.cellState.availableMem + ")"
    })
    val cpuOccupied: Long = simulator.cellState.claimDeltas.foldLeft(0L)(_ + _.currentCpus)
    assert(simulator.cellState.totalCpus - cpuOccupied == simulator.cellState.availableCpus, {
      "The available cpus (" + (simulator.cellState.totalCpus - cpuOccupied) + ") from all the allocated claimDeltas does not correspond to the one in the cell (" + simulator.cellState.availableCpus + ")"
    })


    // This control checks if we claimed some resources (negative values)
    if (cpuVariation < 0 || memVariation < 0 || forceWakeUpScheduler) {
      simulator.logger.debug(loggerPrefix + fclLoggerPrefix + " Getting up to check if I can schedule some jobs with the reclaimed resources")
      wakeUp()
//      forceWakeUpScheduler = false
    }

    if (runningJobQueueSize > 0) {
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

          val (cpu, mem) = predictFutureResourceUtilization(job)
          val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
          cpuVariation += slackCpu
          memVariation += slackMem

          if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
//            lazy val allClaimDeltaPrefix = "[Machine: " + machineID + " | Size: " + simulator.cellState.claimDeltasPerMachine(machineID).length +
//              " | CLs:" + simulator.cellState.claimDeltasPerMachine(machineID).foldLeft("")((b, a) => b + " " + a.id) + "]"
//            simulator.logger.info(loggerPrefix + claimDelta.job.get.loggerPrefix + allClaimDeltaPrefix)
            simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix +
              " This claimDelta is is OOMRisk and needs more memory (" + claimDelta.memStillNeeded + "), checking if we can find some space for it.")

            val jobsClaimDeltasToKill: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()
            var memToFree: Long = claimDelta.memStillNeeded

            simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(_ => memToFree > 0).foreach(cd => {
              memToFree -= (if (cd != claimDelta //cd.job.get.id != job.id
                && PolicyModes.comparePolicyPriority(job, cd.job.get, policyMode, simulator.currentTime) >= 0
                && cd.taskType == TaskType.Elastic) {
                val (c, e) = jobsClaimDeltasToKill.getOrElse(cd.job.get, (false, mutable.HashSet[ClaimDelta]()))
                jobsClaimDeltasToKill(cd.job.get) = (c, e + cd)
                cd.currentMem
              }else 0)
            })

            simulator.cellState.claimDeltasPerMachine(machineID).reverseIterator.takeWhile(_ => memToFree > 0).foreach( cd => {
              memToFree -= (if (cd.job.get.id != job.id
                && PolicyModes.comparePolicyPriority(job, cd.job.get, policyMode, simulator.currentTime) >= 0
                && cd.taskType == TaskType.Core) {
                jobsClaimDeltasToKill(cd.job.get) = (true, mutable.HashSet[ClaimDelta]())
                cd.currentMem
              }else 0)
            })

            if(memToFree > 0){
              if (claimDelta.taskType == TaskType.Core){
                simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
                  " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Killing my entire job because I was a core component!")
                claimDeltasKilledToSaveOOMRisk ++= job.claimDeltas
                killJob(job)
              }else{
                simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
                  " ClaimDelta was in OOMRisk and we could NOT free some resources for it. Killing myself, elastic component.")
                claimDeltasKilledToSaveOOMRisk += claimDelta
                killElasticClaimDeltas(job, mutable.HashSet[ClaimDelta]() + claimDelta)
              }
            } else {
              jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
//                var memoryFreed: Long = 0
//                val originalMemoryAvailable: Long = simulator.cellState.availableMem
                if(coreDied){
//                  memoryFreed = job1.claimDeltas.foldLeft(0L)(_ + _.currentMem)
                  simulator.logger.info(loggerPrefix + fclLoggerPrefix + job1.loggerPrefix +
                    " Killing this job to make some space for others claimDeltas.")
                  claimDeltasKilledToSaveOOMRisk ++= job1.claimDeltas
                  killJob(job1)
                } else if (elastics1.nonEmpty){
//                  memoryFreed = elastics1.foldLeft(0L)(_ + _.currentMem)
                  simulator.logger.info(loggerPrefix + fclLoggerPrefix + job1.loggerPrefix +
                    " Killing elastic components of this job to make some space for others claimDeltas." +
                    elastics1.foldLeft("")((b, a) => b + " " + a.id))
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
              simulator.logger.info(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix +
                " ClaimDelta was in OOMRisk and we could free some resources for it.")
            }
          }
        }
      })
      machineID += 1
    }

    (cpuVariation, memVariation)
  }

  def preventCrashes_naiveApproach(jobsClaimDeltasOOMRisk: mutable.HashMap[Job, mutable.HashSet[ClaimDelta]], takeWhile: Boolean = false): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    val claimDeltasKilledToSaveOOMRisk: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
    jobsClaimDeltasOOMRisk.foreach{case (job, elastics) =>
      val (cpu, mem) = predictFutureResourceUtilization(job)
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
      val (cpu, mem) = predictFutureResourceUtilization(job)
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

  def preventCrashes_smarterApproach(machineIDsWithClaimDeltasOOMRisk: mutable.HashSet[Int]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    machineIDsWithClaimDeltasOOMRisk.foreach(machineID => {
      // Create a data structure for faster access to a job and its running claimDelta on that machine
      val jobs: mutable.HashSet[Job] = mutable.HashSet[Job]()
      val jobsClaimDeltasRunningOnMachine = mutable.HashMap[Job, (mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])]()
      simulator.cellState.claimDeltasPerMachine(machineID).foreach(claimDelta => {
        val job = claimDelta.job.get
        jobs.add(job)
        val (cores, elastics) = jobsClaimDeltasRunningOnMachine.getOrElse(job, (mutable.HashSet[ClaimDelta](), mutable.HashSet[ClaimDelta]()))
        if(claimDelta.taskType == TaskType.Core)
          cores.add(claimDelta)
        else if (claimDelta.taskType == TaskType.Elastic)
          elastics.add(claimDelta)
        jobsClaimDeltasRunningOnMachine(job) = (cores, elastics)
      })

      simulator.logger.debug(loggerPrefix  + "[Machine: " + machineID + "] There are " +
        simulator.cellState.claimDeltasPerMachine(machineID).count(_.status == ClaimDeltaStatus.OOMRisk) + " claimDeltas in OOMRisk over " +
        simulator.cellState.claimDeltasPerMachine(machineID).size)

      // Sorted List of the jobs running on a machine. The sort is done per policy
      val sortedJobsRunningOnMachine: ListBuffer[Job] = PolicyModes.getJobs(jobs, policyMode, simulator.currentTime)

      var memoryFree: Long = simulator.cellState.memPerMachine
      var currentMemoryFree: Long = memoryFree

      val jobsClaimDeltasToKeep: ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])] =
        new ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])]()
      val tmpJobsClaimDeltasToKeep: ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])] =
        new ListBuffer[(Job, mutable.HashSet[ClaimDelta], mutable.HashSet[ClaimDelta])]()
      var stop: Boolean = false
      sortedJobsRunningOnMachine.foreach( job => {
        var claimDelta_core =  mutable.HashSet[ClaimDelta]()
        val (cpu, mem) = predictFutureResourceUtilization(job)
        if (!stop) {
          // Create a copy
          tmpJobsClaimDeltasToKeep.clear()
          jobsClaimDeltasToKeep.foreach { case (job1, cores1, elastics1) =>
            tmpJobsClaimDeltasToKeep += ((job1, mutable.HashSet[ClaimDelta]() ++ cores1, mutable.HashSet[ClaimDelta]() ++ elastics1))
          }

          // START ALGORITHM PRESENTED IN THE PAPER
          // Remove all elastic components from the machine
          jobsClaimDeltasToKeep.foreach { case (job1, _, elastics1) =>
            val (cpu1, mem1) = predictFutureResourceUtilization(job1)
            elastics1.foreach(cd => {
              val (_, resizeMem) = cd.calculateNextAllocations(cpu1, mem1, resizePolicy = resizePolicy)
              currentMemoryFree += resizeMem
            })
            elastics1.clear()
          }
          assert(currentMemoryFree <= simulator.cellState.memPerMachine)

          val (cores, _) = jobsClaimDeltasRunningOnMachine(job)
          var tmpMemory = currentMemoryFree
          cores.foreach(cd => {
            val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
            tmpMemory -= resizeMem
          })
          if(tmpMemory >= 0){
            currentMemoryFree = tmpMemory
            claimDelta_core ++= cores
          }
          // We check if we managed to put back all the core, otherwise we already know that the job will die
          //     therefore no point in wasting resources with elastic components
          if(cores.size == claimDelta_core.size)
            jobsClaimDeltasToKeep += ((job, claimDelta_core, mutable.HashSet[ClaimDelta]()))

          jobsClaimDeltasToKeep.foreach { case (job1, _, elastics1) =>
            if(currentMemoryFree > 0){
              val (cpu1, mem1) = predictFutureResourceUtilization(job1)
              val (_, elastics) = jobsClaimDeltasRunningOnMachine(job1)
              elastics.foreach(cd => {
                val (_, resizeMem) = cd.calculateNextAllocations(cpu1, mem1, resizePolicy = resizePolicy)
                if (currentMemoryFree >= resizeMem){
                  currentMemoryFree -= resizeMem
                  elastics1 += cd
                }
              })
            }
          }
          if (currentMemoryFree > memoryFree) {
            stop = true
            jobsClaimDeltasToKeep.clear()
            jobsClaimDeltasToKeep ++= tmpJobsClaimDeltasToKeep
          }
          memoryFree = currentMemoryFree
          // FINISH ALGORITHM PRESENTED IN THE PAPER
        }
      })
      simulator.logger.debug(loggerPrefix  + "[Machine: " + machineID + "] After simulation we have " +
        jobsClaimDeltasToKeep.foldLeft(0)((b, a) => b + a._2.size + a._3.size) + " claimDeltas")

      // Time to kill the claimDelta that are not in the jobsClaimDeltasToKeep
      jobsClaimDeltasToKeep.foreach { case (job, cores, elastics) =>
        val (originalCores, originalElastics) = jobsClaimDeltasRunningOnMachine.remove(job).get
        if ((originalCores -- cores).nonEmpty){
          killJob(job)
          originalElastics.clear()
        }
        val elasticsToRemove = originalElastics -- elastics
        if (elasticsToRemove.nonEmpty){
          killElasticClaimDeltas(job, elasticsToRemove)
        }
      }
      jobsClaimDeltasRunningOnMachine.foreach { case (job, (cores, elastics)) =>
        if(cores.nonEmpty){
          killJob(job)
          elastics.clear()
        }
        if(elastics.nonEmpty)
          killElasticClaimDeltas(job, elastics)
      }

      // Now we can resize the claimDeltas so that they exist the status of OOMRisk
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
        val (cpu, mem) = predictFutureResourceUtilization(job)
        val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
        val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
        assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
          "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it?"
        })
        (slackCpu, slackMem)
      }

    })

    (cpuVariation, memVariation)
  }


}
