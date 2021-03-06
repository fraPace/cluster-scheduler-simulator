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
import com.typesafe.scalalogging.{LazyLogging, Logger}
import com.workday.insights.timeseries.arima.struct.{ArimaParams, ForecastResult}
import gp.kernels.KernelExp
import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.commons.math3.random.Well19937c

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


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
  
  val varianceMultiplier: Int = System.getProperty("nosfe.simulator.schedulers.zoedynamic.predictor.varianceMultiplier", "3").toInt
  val predictor: Predictor = if(System.getProperty("nosfe.simulator.schedulers.zoedynamic.predictor").toBoolean){
    RealPredictor(window = reclaimResourcesPeriod * 1, varianceMultiplier = varianceMultiplier)
  } else {
    Oracle(window = reclaimResourcesPeriod * 1, introduceError = false)
  }
  val distributedOrCentralized: String = System.getProperty("nosfe.simulator.schedulers.zoedynamic.distributedOrCentralized", "distributed")
  val pessimisticOrOptimistic: String = System.getProperty("nosfe.simulator.schedulers.zoedynamic.pessimisticOrOptimistic", "pessimistic")
  val isDistributedApproach: Boolean = distributedOrCentralized.toLowerCase.equals("distributed")
  val isPessimisticApproach: Boolean = pessimisticOrOptimistic.toLowerCase.equals("pessimistic")

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

  def jobCrashed(job: Job): (Long, Long) = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " A core component crashed. We are killing the application and rescheduling.")
    job.numJobCrashes += 1
    //    job.finalStatus = JobStatus.Crashed
    job.numTasksCrashed += job.claimDeltas.size

    if (job.numJobCrashes >= crashedAllowed) {
      job.disableResize = true
      simulator.logger.info(loggerPrefix + job.loggerPrefix + " This job crashed more than " + crashedAllowed + " times (" + job.numJobCrashes + "). Disabling resize.")
    }

    killJob(job)
  }

  def killJob(job: Job): (Long, Long) = {
    simulator.logger.info(loggerPrefix + job.loggerPrefix + " Killing the entire job.")

    val originalMemoryAvailable: Long = simulator.cellState.availableMem
    val originalCpusAvailable: Long = simulator.cellState.availableCpus
    var memFreed: Long = 0
    var cpuFreed: Long = 0
    job.claimDeltas.foreach(cl => {
      cl.unApply(simulator.cellState)
      simulator.recordWastedResources(cl)
      memFreed += cl.currentMem
      cpuFreed += cl.currentCpus
    })
    assert(originalMemoryAvailable + memFreed == simulator.cellState.availableMem, {
      "The memory freed (" + memFreed + ") is not consistent with the memory that we have now in the cell (" +
        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
    })
    assert(originalCpusAvailable + cpuFreed == simulator.cellState.availableCpus, {
      "The cpus freed (" + cpuFreed + ") is not consistent with the cpus that we have now in the cell (" +
        simulator.cellState.availableCpus + ") considering that originally was " + originalCpusAvailable
    })

    job.numTasksKilled += job.claimDeltas.size

    removeAllUpcomingEventsForJob(job)
    removePendingJob(job)
    removeRunningJob(job)
    job.reset()
    addPendingJob(job)

    (cpuFreed, memFreed)
  }

  def elasticsClaimDeltasCrashed(job: Job, elastic: mutable.HashSet[ClaimDelta]): (Long, Long) = {
    if (elastic.isEmpty)
      return (0L, 0L)

    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Some (" + elastic.size + ") elastic components crashed.")

    job.numTasksCrashed += elastic.size
    killElasticClaimDeltas(job, elastic)
  }

  def killElasticClaimDeltas(job: Job, elastic: mutable.HashSet[ClaimDelta]): (Long, Long) = {
    if (elastic.isEmpty)
      return (0L, 0L)

    simulator.logger.info(loggerPrefix + elasticPrefix + job.loggerPrefix + " Killing elastic components for this job: " +
      elastic.foldLeft("")((b, a) => b + " " + a.id))

    val addToPendingQueue: Boolean = job.elasticTasksUnscheduled == 0
    val originalMemoryAvailable: Long = simulator.cellState.availableMem
    val originalCpusAvailable: Long = simulator.cellState.availableCpus
    var memFreed: Long = 0
    var cpuFreed: Long = 0
    job.removeElasticClaimDeltas(elastic)
    elastic.foreach(cl => {
      cl.unApply(simulator.cellState)
      simulator.recordWastedResources(cl)
      memFreed += cl.currentMem
      cpuFreed += cl.currentCpus
    })
    assert(originalMemoryAvailable + memFreed == simulator.cellState.availableMem, {
      "The memory freed (" + memFreed + ") is not consistent with the memory that we have now in the cell (" +
        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
    })
    assert(originalCpusAvailable + cpuFreed == simulator.cellState.availableCpus, {
      "The cpus freed (" + cpuFreed + ") is not consistent with the cpus that we have now in the cell (" +
        simulator.cellState.availableCpus + ") considering that originally was " + originalCpusAvailable
    })

    job.numTasksKilled += elastic.size

    updateJobFinishingEvents(job, elasticsRemoved = elastic)

    // We have to reinsert the job in queue if this had all it's elastic services allocated
    if (addToPendingQueue)
      addPendingJob(job)

    (cpuFreed, memFreed)
  }

  def checkResourceUtilization(): Unit = {
    def maxLeft(job: Job): (Long, Long) = {
      var cpus: Long = 0
      var mem: Long = 0

      val windowTime = simulator.currentTime - reclaimResourcesPeriod + 1
      // We check if the oracle prediction is not going to be outside the job execution time
      var startTime = if (windowTime < job.jobStartedWorking) job.jobStartedWorking else windowTime

      while (startTime <= simulator.currentTime) {
        var v = job.cpuUtilization(startTime)
        if (v > cpus)
          cpus = v

        v = job.memoryUtilization(startTime)
        if (v > mem)
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

//    val claimDeltasOOMRiskPerMachine: Array[(mutable.LinkedHashSet[ClaimDelta], mutable.LinkedHashSet[ClaimDelta])] =
//      Array.fill(simulator.cellState.numMachines)((mutable.LinkedHashSet[ClaimDelta](), mutable.LinkedHashSet[ClaimDelta]()))

//    val machinesToCheckForOOMRisk: Array[Boolean] = Array.fill(simulator.cellState.numMachines)(false)

    var cpuVariation: Long = 0
    var memVariation: Long = 0
    val originalMemoryAvailable: Long = simulator.cellState.availableMem
    val originalCpusAvailable: Long = simulator.cellState.availableCpus

    // Check if some components/jobs crashed
    var machineID: Int = 0
    while (machineID < simulator.cellState.numMachines) {
      var j: Int = 0
      while (j < simulator.cellState.claimDeltasPerMachine(machineID).length) {
        val claimDelta: ClaimDelta = simulator.cellState.claimDeltasPerMachine(machineID)(j)
        assert(simulator.cellState.claimDeltas.contains(claimDelta), {
          claimDelta.loggerPrefix + " This claimDelta is not inside the cluster. Then why are we processing it?"
        })
        claimDelta.job match {
          case Some(job) =>
            if (!job.disableResize &&
              job.finalStatus != JobStatus.Crashed && job.finalStatus != JobStatus.Completed && job.finalStatus != JobStatus.Not_Scheduled) {

              val (currentCpus, currentMem) = maxLeft(job)
              if (claimDelta.checkResourcesLimits(currentCpus, currentMem) == ClaimDeltaStatus.OOMKilled) {
                val (core, elastics) = jobsClaimDeltasOOMKilled.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
                if (claimDelta.taskType == TaskType.Elastic) {
                  jobsClaimDeltasOOMKilled(job) = (core, elastics + claimDelta)
                } else {
                  jobsClaimDeltasOOMKilled(job) = (true, elastics)
                }
              }
//              else {
//                val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
//                val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
//                simulator.logger.debug(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix + " This claimDelta will use " +
//                  resizeMem + " memory, %.2f".format(resizeMem / claimDelta.requestedMem.toDouble) + "% than originally requested")
//                val (slackCpu, slackMem) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
//                cpuVariation += slackCpu
//                memVariation += slackMem
//
//                if (claimDelta.status == ClaimDeltaStatus.OOMRisk) {
//                  machinesToCheckForOOMRisk(machineID) = true
//                  val (cores, elastics) = claimDeltasOOMRiskPerMachine(machineID)
//                  if (claimDelta.taskType == TaskType.Core) {
//                    cores += claimDelta
//                  } else {
//                    elastics += claimDelta
//                  }
//                }
//              }
            }
          case None => None
        }
        j += 1
      }
      machineID += 1
    }

//    // Consistency checks to stop if something is wrong in the simulator
//    assert(originalMemoryAvailable - memVariation == simulator.cellState.availableMem, {
//      "The memory variation (" + memVariation + ") is not consistent with the memory that we have now in the cell (" +
//        simulator.cellState.availableMem + ") considering that originally was " + originalMemoryAvailable
//    })
//    assert(originalCpusAvailable - cpuVariation == simulator.cellState.availableCpus, {
//      "The cpus variation (" + cpuVariation + ") is not consistent with the cpus that we have now in the cell (" +
//        simulator.cellState.availableCpus + ") considering that originally was " + originalCpusAvailable
//    })

    //Let's remove the components that died
    jobsClaimDeltasOOMKilled.foreach { case (job, (coreDied, elastics)) =>
      val (_cpuVariation, _memVariation) = if (coreDied) {
        jobCrashed(job)
      } else {
        elasticsClaimDeltasCrashed(job, elastics)
      }
      cpuVariation += _cpuVariation
      memVariation += _memVariation
    }

    // Time to prevent resource contention
    if (isPessimisticApproach){
      val (_cpuVariation, _memVariation) = if(isDistributedApproach){
        preventCrashes_pessimisticDistributedApproach()
      } else {
        preventCrashes_pessimisticCentralizedApproach(_runningQueue)
      }
      cpuVariation += _cpuVariation
      memVariation += _memVariation
    }


    // Time to resize the components
    val claimDeltasOOMRisk: ArrayBuffer[ClaimDelta] = ArrayBuffer[ClaimDelta]()
    machineID = 0
    while (machineID < simulator.cellState.numMachines) {
      var j: Int = 0
      while (j < simulator.cellState.claimDeltasPerMachine(machineID).length) {
        val claimDelta: ClaimDelta = simulator.cellState.claimDeltasPerMachine(machineID)(j)
        assert(simulator.cellState.claimDeltas.contains(claimDelta), {
          claimDelta.loggerPrefix + " This claimDelta is not inside the cluster. Then why are we processing it?"
        })
        claimDelta.job match {
          case Some(job) =>
            val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
            val (resizeCpus, resizeMem) = claimDelta.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
            simulator.logger.debug(loggerPrefix + fclLoggerPrefix + claimDelta.job.get.loggerPrefix + claimDelta.loggerPrefix + " This claimDelta will use " +
              resizeMem + " memory, %.2f".format(resizeMem / claimDelta.requestedMem.toDouble) + "% than originally requested")
            val (_cpuVariation, _memVariation) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
            cpuVariation += _cpuVariation
            memVariation += _memVariation

            if(claimDelta.status == ClaimDeltaStatus.OOMRisk){
              claimDeltasOOMRisk += claimDelta
            }
          case None => None
        }
        j += 1
      }

      // It is possible that some components are in OOMRisk, because the resource they need are freed by the next component in the list
      // Thus we iterate over the OOMRisk component to give them the last resource they require.
      claimDeltasOOMRisk.foreach(claimDelta => {
        claimDelta.job match {
          case Some(_) =>
            val (resizeCpus, resizeMem) = (claimDelta.currentCpus + claimDelta.cpusStillNeeded, claimDelta.currentMem + claimDelta.memStillNeeded)
            val (_cpuVariation, _memVariation) = claimDelta.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
            cpuVariation += _cpuVariation
            memVariation += _memVariation

            if(isPessimisticApproach){
              // Now if the component is still in OOMRisk we raise an error, since it should not be
              assert(claimDelta.status != ClaimDeltaStatus.OOMRisk, {
                "How can the claimDelta be in OOMRisk(" + claimDelta.memStillNeeded + ") if we just freed up the resources for it? " +
                  "The current available Memory on machine " + machineID + " is " + simulator.cellState.availableCpusPerMachine(machineID)
              })
            }
          case None => None
        }
      })
      machineID += 1
    }



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
    if (pendingQueueSize > 0 && (originalCpusAvailable != simulator.cellState.availableCpus || originalMemoryAvailable != simulator.cellState.availableMem)) {
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

  def preventCrashes_pessimisticDistributedApproach(): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    var machineID: Int = 0
    while (machineID < simulator.cellState.numMachines) {
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

        var tmpMemFree: Long = memFree
        cores.foreach(cd => {
          val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          tmpMemFree -= resizeMem
        })
        if (tmpMemFree < 0) {
          jobsClaimDeltasToKill(job) = (true, mutable.HashSet[ClaimDelta]())
        } else {
          memFree = tmpMemFree
          elastics.toList.sortWith(_.creationTime < _.creationTime).foreach(cd => {
            val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
            tmpMemFree = memFree - resizeMem
            if (tmpMemFree < 0) {
              val (c, e) = jobsClaimDeltasToKill.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
              jobsClaimDeltasToKill(job) = (c, e + cd)
            } else {
              memFree = tmpMemFree
            }
          })
        }
      })

      jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
        val (_cpuVariation, _memVariation) = if (coreDied) {
          killJob(job1)
        } else {
          killElasticClaimDeltas(job1, elastics1)
        }
        cpuVariation += _cpuVariation
        memVariation += _memVariation
      }

//      simulator.cellState.claimDeltasPerMachine(machineID).iterator.foreach(cd => {
//        val job: Job = cd.job.get
//        val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
//        val (resizeCpus, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
//        val (slackCpu, slackMem): (Long, Long) = cd.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
//        cpuVariation += slackCpu
//        memVariation += slackMem
//        assert(cd.status != ClaimDeltaStatus.OOMRisk, {
//          "How can the claimDelta be in OOMRisk(" + cd.memStillNeeded + ") if we just freed up the resources for it?"
//        })
//      })

      machineID += 1
    }

    (cpuVariation, memVariation)
  }

  def preventCrashes_pessimisticCentralizedApproach(runningJobs: mutable.TreeSet[Job]): (Long, Long) = {
    var cpuVariation: Long = 0
    var memVariation: Long = 0

    val jobsClaimDeltasToKill: mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])] = mutable.HashMap[Job, (Boolean, mutable.HashSet[ClaimDelta])]()

    val sortedJobs: mutable.TreeSet[Job] = runningJobs
    var memPerMachine: Array[Long] = Array.fill(simulator.cellState.numMachines)(simulator.cellState.memPerMachine)
    sortedJobs.foreach(job => {
      val cores = job.coreClaimDeltas
      val elastics = job.elasticClaimDeltas.toList.sortWith(_.creationTime < _.creationTime)
      val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)

      var preemptJob: Boolean = false
      val tmpMemPerMachine: Array[Long] = memPerMachine.clone()
      cores.foreach(cd => {
        if (!preemptJob) {
          val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          tmpMemPerMachine(cd.machineID) -= resizeMem
          if (tmpMemPerMachine(cd.machineID) < 0) {
            preemptJob = true
          }
        }
      })
      if (preemptJob) {
        jobsClaimDeltasToKill(job) = (true, mutable.HashSet[ClaimDelta]())
      } else {
        memPerMachine = tmpMemPerMachine
        elastics.foreach(cd => {
          val (_, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
          val tmpMemFree = memPerMachine(cd.machineID) - resizeMem
          if (tmpMemFree < 0) {
            val (c, e) = jobsClaimDeltasToKill.getOrElse(job, (false, mutable.HashSet[ClaimDelta]()))
            jobsClaimDeltasToKill(job) = (c, e + cd)
          } else {
            memPerMachine(cd.machineID) = tmpMemFree
          }
        })
      }
    })

    jobsClaimDeltasToKill.foreach { case (job1, (coreDied, elastics1)) =>
      val (_cpuVariation, _memVariation) = if (coreDied) {
        killJob(job1)
      } else {
        killElasticClaimDeltas(job1, elastics1)
      }
      cpuVariation += _cpuVariation
      memVariation += _memVariation
    }

//    sortedJobs.foreach(job => {
//      (job.coreClaimDeltas ++ job.elasticClaimDeltas).foreach(cd => {
//        val (cpu, mem) = predictor.predictFutureResourceUtilization(job, simulator.currentTime)
//        val (resizeCpus, resizeMem) = cd.calculateNextAllocations(cpu, mem, resizePolicy = resizePolicy)
//        val (slackCpu, slackMem): (Long, Long) = cd.resize(simulator.cellState, resizeCpus, resizeMem, resizePolicy = resizePolicy)
//        cpuVariation += slackCpu
//        memVariation += slackMem
//        assert(cd.status != ClaimDeltaStatus.OOMRisk, {
//          "How can the claimDelta be in OOMRisk(" + cd.memStillNeeded + ") if we just freed up the resources for it?"
//        })
//      })
//    })

    (cpuVariation, memVariation)
  }

}

abstract class Predictor() extends LazyLogging {
  def averageError: Float
  def maxError: Float
  def minError: Float

  def predictFutureResourceUtilization(job: Job, currentTime: Double): (Long, Long)
}


object Oracle {
  def apply(window: Int, introduceError: Boolean): Oracle = new Oracle(window, introduceError)
}

class Oracle(window: Int, introduceError: Boolean = false) extends Predictor{
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

object RealPredictor{
  val gracePeriod: Int = 10

  def apply(window: Int, varianceMultiplier:Int = 0): RealPredictor = new RealPredictor(window, varianceMultiplier)
}

class RealPredictor(window: Int, varianceMultiplier: Int) extends Predictor{
  def printModuleInfo(module: String): Unit = {
    module match {
      case "GP" => GP.printInfo(logger)
      case "Arima" => Arima.printInfo(logger)
      case "Base" => Base.printInfo(logger)
      case _ => throw new RuntimeException("Module for Predictor not found!")
    }
  }
  def moduleToUse(module: String): AbstractPredictor = {
    module match {
      case "GP" => new GP
      case "Arima" => new Arima
      case "Base" => new Base
      case _ => throw new RuntimeException("Module for Predictor not found!")
    }
  }
  logger.warn("Loading a Real Predictor with the following parameters:" +
    " GracePeriod = " + RealPredictor.gracePeriod +
    ", varianceMultiplier = " + varianceMultiplier)
  val moduleToUse: String = System.getProperty("nosfe.simulator.schedulers.zoedynamic.predictor.module", "GP")
  printModuleInfo(moduleToUse)


  // We use these structure to cache the information so that we get the same one if we call the functions
  //     multiple times at the same event time
  //  val errorsCache: mutable.HashMap[Long, (Double, Double)] = mutable.HashMap[Long, (Double, Double)]()
  val predictionCache: mutable.HashMap[Long, (Double, (Long, Long))] = mutable.HashMap[Long, (Double, (Long, Long))]()

  var errors: ArrayBuffer[Float] = ArrayBuffer[Float]()
  var predictions: Long = 0
  def averageError: Float = if(errors.nonEmpty) (errors.sum / predictions.toDouble).toFloat else 0F
  def maxError: Float = if(errors.nonEmpty) errors.max else 0F
  def minError: Float = if(errors.nonEmpty) errors.min else 0F

  val predictorsCpu: mutable.HashMap[Long, AbstractPredictor] = mutable.HashMap[Long, AbstractPredictor]()
  val predictorsMem: mutable.HashMap[Long, AbstractPredictor] = mutable.HashMap[Long, AbstractPredictor]()
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

      val predictorMem: AbstractPredictor = predictorsMem.getOrElse(jobID, moduleToUse(moduleToUse))
      val predictorCpu: AbstractPredictor = predictorsCpu.getOrElse(jobID, moduleToUse(moduleToUse))

      predictorMem.addTimeSeriesObservations(Array[Double](_mem))
      predictorCpu.addTimeSeriesObservations(Array[Double](_cpus))

      if(predictorMem.getTimeSeriesSize > RealPredictor.gracePeriod){
        val predictionMem:Array[Prediction] = predictorMem.predict()
        val predictionCpu:Array[Prediction] = predictorCpu.predict()
        mem = predictionMem(0).getMean.toLong + Math.sqrt(predictionMem(0).getVariance).toLong * varianceMultiplier
        cpus = predictionCpu(0).getMean.toLong + Math.sqrt(predictionCpu(0).getVariance).toLong * varianceMultiplier
        if (mem < 0) mem = 0
        if (cpus < 0) cpus = 0

        val error = if(mem == 0){
          Math.abs(1 - (_futureMem / 1.0)).toFloat
        }
        else{
          Math.abs(1 - (_futureMem / mem.toDouble)).toFloat
        }

        //        if(error > ClaimDelta.ResizePolicy.SafeMargin)
        //          logger.warn("Oracle Error (" + error + ") is above the safe ClaimDelta margin " + ClaimDelta.ResizePolicy.SafeMargin)
        errors += error
      }else{
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

abstract class AbstractPredictor extends LazyLogging{

  var ts: ArrayBuffer[Double] = ArrayBuffer[Double]()

  def name: String
  def addTimeSeriesObservations(values: Array[Double]): Unit = { ts ++= values }
  def getTimeSeriesSize: Long = { ts.length }
  def predict(forecastSize: Int = 1): Array[Prediction]
  def reset(): Unit = { ts = ArrayBuffer[Double]() }
}

object Arima {
  // Set ARIMA model parameters.// Set ARIMA model parameters.
  // Non-Seasonality parameters
  val p = 0
  val d = 1
  val q = 1
  // Seasonality parameters
  val P = 1
  val D = 1
  val Q = 0
  val m = 0 // when this is set to 0 the model is assumed with non-seasonality
  val arimaPar: ArimaParams = new ArimaParams(p, d, q, P, D, Q, m)

  val historySize = 2000

  def printInfo(logger: Logger): Unit = {
    logger.warn("Loading Arima Module with the following parameters:"  +
      ", p = " + Arima.p +
      ", d = " + Arima.d +
      ", q = " + Arima.q +
      ", P = " + Arima.P +
      ", D = " + Arima.D +
      ", Q = " + Arima.Q +
      ", m = " + Arima.m +
      ", historySize = " + Arima.historySize)
  }

  def apply: Arima = new Arima()
}

class Arima extends AbstractPredictor{
    def predict(forecastSize: Int = 1): Array[Prediction] = {
    var ret: ArrayBuffer[Prediction] = ArrayBuffer[Prediction]()

    // Obtain forecast result. The structure contains forecast values and performance metric etc.
    val forecastResult: ForecastResult = com.workday.insights.timeseries.arima.Arima.
      forecast_arima(ts.takeRight(Arima.historySize).toArray, forecastSize, Arima.arimaPar)
    // Read forecast values
    val forecast = forecastResult.getForecast
    //    val forecast = forecastResult.getForecastUpperConf
    // It also provides the maximum normalized variance of the forecast values and their confidence interval.
    //    val maxNormalizedSigma = forecastResult.getMaxNormalizedVariance
    val maxNormalizedSigma = 0

    forecast.foreach(v => {
      ret += new Prediction(v, maxNormalizedSigma)
    })
    ret.toArray
  }

  override def name: String = "Arima"

}

object GP {
  val predictionWindowSize: Int = 1
  val modelHistorySize: Int = 50
  val numRestarts: Int = 0
  val optimizeModelEvery: Int = 10

  def printInfo(logger: Logger): Unit = {
    logger.warn("Loading GP Module with the following parameters:"  +
      ", PredictionWindowSize = " + GP.predictionWindowSize +
      ", ModelHistorySize = " + GP.modelHistorySize +
      ", NumRestarts = " + GP.numRestarts +
      ", OptimizeModelEvery = " + GP.optimizeModelEvery )
  }

  def apply: GP = new GP()
}

class GP extends AbstractPredictor {

  var predictor = new TimeseriesPredictor(new KernelExp, 20, GP.predictionWindowSize,
    GP.modelHistorySize, 2147483647, GP.numRestarts, GP.optimizeModelEvery)

  override def addTimeSeriesObservations(values: Array[Double]): Unit = {
    predictor.addTimeseriesObservations(values)
  }

  override def getTimeSeriesSize: Long = {
    predictor.getTimeseriesSize
  }

  def predict(forecastSize: Int = 1): Array[Prediction] = {
    var ret: ArrayBuffer[Prediction] = ArrayBuffer[Prediction]()
    predictor.predict().foreach(v => {
      ret += new Prediction(v.getMean, v.getVariance)
    })
    ret.toArray
  }

  override def name: String = "GP"

  override def reset(): Unit = {
    predictor = new TimeseriesPredictor(new KernelExp, 20, GP.predictionWindowSize,
      GP.modelHistorySize, 2147483647, GP.numRestarts, GP.optimizeModelEvery)
  }
}

object Base {
  def printInfo(logger: Logger): Unit = {
    logger.warn("Loading Base Module.")
  }

  def apply: GP = new GP()
}

class Base extends AbstractPredictor{
  override def name: String = "Base"

  override def predict(forecastSize: Int = 1): Array[Prediction] = {
    val prediction = if(ts.nonEmpty){
      new Prediction(ts.last, 0)
    } else {
      new Prediction(0, 0)
    }
    ArrayBuffer[Prediction](prediction).toArray
  }
}
