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

package ClusterSchedulingSimulation.core

import ClusterSchedulingSimulation.utils.UniqueIDGenerator
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object JobStatus extends Enumeration {
  type JobStatus = Value
  val TimedOut, Not_Scheduled, Partially_Scheduled, Fully_Scheduled, Completed, Crashed, Killed = Value

  def exists(jobStatus: JobStatus): Boolean = values.exists(_.equals(jobStatus))
  def valuesToString(): String = {values.toString()}
}

object TaskType extends Enumeration {
  type TaskType = Value
  val Core, Elastic, None = Value

  def exists(taskType: TaskType): Boolean = values.exists(_.equals(taskType))
  def valuesToString(): String = {values.toString()}
}

/**
  *
  * @param id           Job ID
  * @param submitted    The time the job was submitted in seconds
  * @param _numTasks    Number of tasks that compose this job
  * @param taskDuration Durations of each task in seconds
  * @param workloadName Type of job
  * @param cpusPerTask  Number of cpus required by this job
  * @param memPerTask   Amount of ram, in GB, required by this job
  * @param isRigid      Boolean value to check if the job is picky or not
  */
case class Job(id: Long,
               submitted: Double,
               _numTasks: Int,
               var taskDuration: Double,
               workloadName: String,
               cpusPerTask: Long,
               var memPerTask: Long,
               isRigid: Boolean = false,
               numCoreTasks: Option[Int] = None,
               priority: Int = 0,
               error: Double = 1) extends LazyLogging {
  assert(_numTasks != 0 || (numCoreTasks.isDefined && numCoreTasks.get != 0), {
    "numTasks for job " + id + " is " + _numTasks + " and numCoreTasks is " + numCoreTasks
  })
//  assert(!cpusPerTask.isNan), {
//    "cpusPerTask for job " + id + " is " + cpusPerTask
//  })
//  assert(!memPerTask.isNaN, {
//    "memPerTask for job " + id + " is " + memPerTask
//  })

  val sizeAdjustment: Double = {
    if (workloadName.equals("Interactive"))
      0.001
    else
      1
  }
  var requestedCores: Double = Int.MaxValue
  var finalStatus = JobStatus.Not_Scheduled
  // Timestamp last inserted into scheduler's queue, used to compute timeInQueue.
  var lastEnqueued: Double = 0.0
  var lastSchedulingStartTime: Double = 0.0
  // Number of scheduling attempts for this job (regardless of how many
  // tasks were successfully scheduled in each attempt).
  var numSchedulingAttempts: Long = 0
  // Number of "task scheduling attempts". I.e. each time a scheduling
  // attempt is made for this job, we increase this counter by the number
  // of tasks that still need to be be scheduled (at the beginning of the
  // scheduling attempt). This is useful for analyzing the impact of no-fit
  // events on the scheduler busytime metric.
  var numTaskSchedulingAttempts: Long = 0
  var usefulTimeScheduling: Double = 0.0
  var wastedTimeScheduling: Double = 0.0
  var jobFinishedWorking: Double = 0.0
  private[this] var _claimDeltas: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
  private[this] var _coreClaimDeltas: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
  private[this] var _elasticClaimDeltas: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()
//  private[this] var _unscheduledTasks: Int = _numTasks
  // Time, in seconds, this job spent waiting in its scheduler's queue
  private[this] var _firstScheduled: Boolean = true
  private[this] var _timeInQueueTillFirstScheduled: Double = 0.0
  private[this] var _timeInQueueTillFullyScheduled: Double = 0.0
  // Store information to calculate the execution time of the job
  private[this] var _jobStartedWorking: Double = 0.0
  // Zoe Applications variables
//  private[this] var _isZoeApp: Boolean = false
  private[this] var _coreTasks: Int = _numTasks
  private[this] var _elasticTasks: Int = 0
//  private[this] var _moldableTasksUnscheduled: Int = 0
//  private[this] var _elasticTasksUnscheduled: Int = 0

  private[this] val _jobDuration: Double = taskDuration
  private[this] var _progress: Double = 0
  // This variable is used to calculate the relative (non absolute) progression
  private[this] var _lastProgressTimeCalculation: Double = 0

  private[this] var _cpuUtilization: Array[Float] = Array[Float]()
  private[this] var _memoryUtilization: Array[Float] = Array[Float]()

  private[this] var _numJobCrashes: Long = 0

  var numTasksKilled: Long = 0
  var numTasksCrashed: Long = 0

  lazy val loggerPrefix: String = "[Job " + id + " (" + workloadName + ")]"

  var disableResize: Boolean = false

  def numJobCrashes: Long = _numJobCrashes
  def numJobCrashes_=(value: Long): Unit = {
    _numJobCrashes = value
  }

  def copyEverything(): Job = {
    val newJob: Job = this.copy()
    newJob.cpuUtilization = this.cpuUtilization.clone()
    newJob.memoryUtilization = this.memoryUtilization.clone()
    newJob
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: Job => that.id == this.id && that.workloadName.equals(this.workloadName)
      case _ => false
    }

  override def hashCode(): Int = this.id.hashCode()

  var cpuUtilizationIndexAdjustment: Float = 0
  def cpuUtilization: Array[Float] = _cpuUtilization
  def cpuUtilization_=(value: Array[Float]): Unit = {
    if(value.nonEmpty){
      value.foreach(v => assert(v <= cpusPerTask, {
        "CPU Utilization (" + v + ") is higher than allocated (" + cpusPerTask + ")."
      }))
      assert(value.length == jobDuration, {
        "The number of points must be equal to the job duration."
      })
      _cpuUtilization = value
      // We use this formula be able to divide the utilization in buckets
      // By also considering to have fewer points than the total size of the array
      // Using module is not a good option because the polling times (monitoringTime and others) might be different
      cpuUtilizationIndexAdjustment = Math.ceil(jobDurationCoreOnly / _cpuUtilization.length.toDouble).toFloat
    }
  }

  var memoryUtilizationIndexAdjustment: Float = 0
  def memoryUtilization: Array[Float] = _memoryUtilization
  def memoryUtilization_=(value: Array[Float]): Unit = {
    if(value.nonEmpty){
      value.foreach(v => assert(v <= memPerTask, {
        "Memory Utilization (" + v + ") is higher than allocated (" + memPerTask + ")."
      }))
      _memoryUtilization = value
      // We use this formula be able to divide the utilization in buckets
      // By also considering to have fewer points than the total size of the array
      // Using module is not a good option because the polling times (monitoringTime and others) might be different
      memoryUtilizationIndexAdjustment = (jobDurationCoreOnly / _memoryUtilization.length.toDouble).toFloat
    }
  }

  def cpuUtilization(currtime: Double): Long = {
    if(currtime < _jobStartedWorking || _cpuUtilization.isEmpty)
      return 0

    var index = ((currtime - _jobStartedWorking) % _memoryUtilization.length).toInt - 1
    if(index < 0)
      index = 0
    val r = (_cpuUtilization(index) * cpusPerTask).toLong
    if(r > cpusPerTask)
      cpusPerTask
    else
      r
  }

  def memoryUtilization(currtime: Double): Long = {
    if(currtime < _jobStartedWorking || _memoryUtilization.isEmpty)
      return 0

    var index = ((currtime - _jobStartedWorking) % _memoryUtilization.length).toInt - 1
    if(index < 0)
      index = 0
    val r = (_memoryUtilization(index) * memPerTask).toLong
    if(r > memPerTask)
      memPerTask
    else
      r
  }

  def coreClaimDeltas: mutable.HashSet[ClaimDelta] = _coreClaimDeltas
  def coreClaimDeltas_=(value:mutable.HashSet[ClaimDelta]): Unit = {_coreClaimDeltas = value}
  def addCoreClaimDeltas(value:Seq[ClaimDelta]): Unit = {
    _coreClaimDeltas ++= value
    assert(_coreClaimDeltas.size <= coreTasks, {
      "The number of deployed ClaimDelta (" + _coreClaimDeltas.size +
      ") cannot be higher than requested core tasks (" + coreTasks + ")."
    })
    _claimDeltas ++= value
  }
  def removeCoreClaimDeltas(value:Seq[ClaimDelta]): Unit = {
    _coreClaimDeltas --= value
    _claimDeltas --= value
  }

  def elasticClaimDeltas: mutable.HashSet[ClaimDelta] = _elasticClaimDeltas
  def elasticClaimDeltas_=(value:mutable.HashSet[ClaimDelta]): Unit = {_elasticClaimDeltas = value}
  def addElasticClaimDeltas(value:Seq[ClaimDelta]): Int = {
    if(_elasticClaimDeltas.size + coreTasks != _claimDeltas.size){
      logger.warn("Something is wrong! The size of elasticClaimDelta (" + _elasticClaimDeltas.size +
        ") should be the same as claimDelta(" + _claimDeltas.size + ") minus the number of coreTasks(" + coreTasks +
        "), but it is not.")
    }
    assert(_elasticClaimDeltas.size + coreTasks == _claimDeltas.size, {
      "Something is wrong! The size of elasticClaimDelta (" + _elasticClaimDeltas.size +
        ") should be the same as claimDelta(" + _claimDeltas.size + ") minus the number of coreTasks(" + coreTasks +
        "), but it is not."
    })
    val originalSize: Int = _elasticClaimDeltas.size

    _elasticClaimDeltas ++= value
    assert(_elasticClaimDeltas.size <= elasticTasks, {
      "The number of deployed ClaimDelta (" + _elasticClaimDeltas.size +
      ") cannot be higher than requested elastic tasks (" + elasticTasks + ")."
    })
    _claimDeltas ++= value

    _elasticClaimDeltas.size - originalSize
  }
  def removeElasticClaimDeltas(value:mutable.HashSet[ClaimDelta]): Unit = {
    value.foreach(removeElasticClaimDelta)
  }

  def removeElasticClaimDelta(value:ClaimDelta): Unit = {
    assert(_elasticClaimDeltas.size + coreTasks == _claimDeltas.size, {
      "Something is wrong! The size of elasticClaimDelta (" + _elasticClaimDeltas.size +
        ") should be the same as claimDelta(" + _claimDeltas.size + ") minus the number of coreTasks(" + coreTasks +
        "), but it is not."
    })
    _claimDeltas -= value
    assert(_elasticClaimDeltas.remove(value))
  }

  def claimDeltas: mutable.HashSet[ClaimDelta] = _claimDeltas
  def claimDeltas_=(value:mutable.HashSet[ClaimDelta]): Unit = {_claimDeltas = value}

  /*
    * We set the number of core tasks that compose the Zoe Applications
    * remember that at least one core service must exist
    * a Zoe Application cannot be composed by just elastic services
    */
  numCoreTasks match {
    case Some(param) =>
      _elasticTasks = _numTasks

      _coreTasks = param
      if (_coreTasks == 0)
        _coreTasks = 1

    case None => None
  }

//  def isZoeApp: Boolean = _isZoeApp

  def jobStartedWorking: Double = _jobStartedWorking
  def jobStartedWorking_=(value: Double): Unit = {
    _jobStartedWorking = value
//    _lastProgressTimeCalculation = _jobStartedWorking
  }

  def firstScheduled: Boolean = _firstScheduled
  def firstScheduled_=(value: Boolean): Unit = {
    _firstScheduled = value
  }

//  /**
//    * Calculate the job duration if only moldable (core components) tasks are running.
//    * Useful for Zoe applications
//    *
//    * @return Double
//    */
//  def jobDurationMoldableOnly: Double = (numTasks / moldableTasks) * jobDuration




  def numTasks: Int = _coreTasks + _elasticTasks

  /**
    * It will return the elastic component of this job.
    * The elastic component is the tasks set that are NOT required to be deployed before the job can start to
    * produce some work. They just contribute to the speedup of the application
    * Currently only a Zoe Application support this knowledge.
    *
    * @return The number of elastic tasks
    */
  def elasticTasks: Int = _elasticTasks
  def elasticTasksScheduled: Int = elasticClaimDeltas.size
  def elasticTasksUnscheduled: Int = elasticTasks - elasticTasksScheduled
  /**
    * It will return the core component of this job.
    * The core component is the tasks set that are required to be deployed before the job can start to
    * produce some work.
    * Currently only a Zoe Application support this knowledge.
    *
    * @return The number of core tasks
    */
  def coreTasks: Int = _coreTasks
  def coreTasksScheduled: Int = coreClaimDeltas.size
  def coreTasksUnscheduled: Int = coreTasks - coreTasksScheduled

  def tasksScheduled: Int = claimDeltas.size
  def tasksUnscheduled: Int = numTasks - tasksScheduled

  def jobDuration: Double = _jobDuration
  def jobDurationCoreOnly: Double = _jobDuration * (numTasks / coreTasks.toFloat)
  def remainingTime: Double = {
    assert(jobFinishedWorking == 0, {
      "Why are you calculating the progress if the job has finished already?"
    })
    val timeLeft: Double = (1 - _progress) * ((numTasks / tasksScheduled.toDouble) * jobDuration)
    assert(timeLeft >= 0)
    timeLeft
  }
  def remainingTimeCoresOnly: Double = {
    assert(jobFinishedWorking == 0, {
      "Why are you calculating the progress if the job has finished already?"
    })
    val timeLeft: Double = (1 - _progress) * ((numTasks / coreTasks.toDouble) * jobDuration)
    assert(timeLeft >= 0)
    timeLeft
  }

//  def updateProgress(currTime: Double = 0.0): Unit = {
//    var relativeProgress: Double = 0.0
//    // Optimizations:
//    //  1) if the currTime is equal to _jobStartedWorking it means that the progress is 0
//    //  2) if the currTime is equal to _lastProgressTimeCalculation it means that we already calculated the progress
//    // So let's avoid extra calculations to speed up the simulation
//    if(currTime != _jobStartedWorking && currTime != _lastProgressTimeCalculation){
//      // In case we do not calculate the progress as soon as the job starts
//      //     so we have a _lastProgressTimeCalculation that is aligned with the starting of the job
//      if (_lastProgressTimeCalculation < _jobStartedWorking)
//        _lastProgressTimeCalculation = _jobStartedWorking
//
//      relativeProgress = (currTime - _lastProgressTimeCalculation) /
//        ((numTasks / tasksScheduled.toDouble) * jobDuration)
//      assert(relativeProgress >= 0 && relativeProgress <= 1, {
//        "relativeProgress (" + relativeProgress + ") cannot be lower than 0 or higher than 1. "
//      })
//    }
//
//    _progress += relativeProgress
//    _lastProgressTimeCalculation = currTime
//    assert(_progress <= 1.0, {
//      "Progress cannot be higher than 1! (" + _progress + ")"
//    })
//  }

  def updateProgress(currTime: Double = 0.0, newTasksAllocated: Int = 0,
                        tasksRemoved: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()): Unit = {
    var relativeProgress: Double = 0.0
    var progressLost: Double = 0.0
    // Optimizations:
    //  1) if the currTime is equal to _jobStartedWorking it means that the progress is 0
    //  2) if the currTime is equal to _lastProgressTimeCalculation it means that we already calculated the progress
    // So let's avoid extra calculations to speed up the simulation
    if(currTime != _jobStartedWorking && currTime != _lastProgressTimeCalculation){
      // In case we do not calculate the progress as soon as the job starts
      //     so we have a _lastProgressTimeCalculation that is aligned with the starting of the job
      if (_lastProgressTimeCalculation < _jobStartedWorking)
        _lastProgressTimeCalculation = _jobStartedWorking

      relativeProgress = (currTime - _lastProgressTimeCalculation) /
        ((numTasks / (tasksScheduled - newTasksAllocated + tasksRemoved.size).toDouble) * jobDuration)
      assert(relativeProgress >= 0 && relativeProgress <= 1, {
        "relativeProgress (" + relativeProgress + ") cannot be lower than 0 or higher than 1. "
      })

      progressLost = tasksRemoved.foldLeft(0.0)((b, a) => b + (currTime - a.creationTime) / (numTasks * jobDuration))
      assert(progressLost >= 0 && progressLost <= 1, {
        "progressLost (" + progressLost + ") cannot be lower than 0 or higher than 1. "
      })
    }

    _progress += (relativeProgress - progressLost)
    _lastProgressTimeCalculation = currTime
    assert(_progress <= 1.0, {
      "Progress cannot be higher than 1! (" + _progress + ")"
    })
  }


//  def elasticTasksUnscheduled_=(value: Int): Unit = {
//    _elasticTasksUnscheduled = value
//    assert(_elasticTasksUnscheduled <= elasticTasks, {
//      "Elastic Services to schedule (" + _elasticTasksUnscheduled + ") are more than requested (" + elasticTasks + ")"
//    })
//  }

//  def unscheduledTasks_=(value: Int): Unit = {
//    if (_isZoeApp) _moldableTasksUnscheduled = value else _unscheduledTasks = value
//  }

  var numResets: Int = 0
  def reset(): Unit = {
    numResets += 1

    finalStatus = JobStatus.Not_Scheduled

    _claimDeltas = mutable.HashSet[ClaimDelta]()
    _coreClaimDeltas = mutable.HashSet[ClaimDelta]()
    _elasticClaimDeltas = mutable.HashSet[ClaimDelta]()

    _progress = 0
    _lastProgressTimeCalculation = 0.0

    _firstScheduled = true
    _timeInQueueTillFirstScheduled = 0.0
    _timeInQueueTillFullyScheduled = 0.0
    _jobStartedWorking = 0.0
    jobFinishedWorking = 0.0
  }

  def isScheduled: Boolean = !isNotScheduled && !isTimedOut

  def isTimedOut: Boolean = finalStatus == JobStatus.TimedOut

  def isNotScheduled: Boolean = finalStatus == JobStatus.Not_Scheduled

  def isFullyScheduled: Boolean = finalStatus == JobStatus.Fully_Scheduled || finalStatus == JobStatus.Completed

  def isCompleted: Boolean = finalStatus == JobStatus.Completed

  def isPartiallyScheduled: Boolean = finalStatus == JobStatus.Partially_Scheduled

  def isCrashed: Boolean = finalStatus == JobStatus.Crashed

  def responseRatio(currentTime: Double): Double = 1 + (currentTime - submitted) / jobDuration

  def coresLeft: Double = math.max(0, requestedCores - coresGranted)

  // For Spark
  def coresGranted: Double = cpusPerTask.toDouble * tasksScheduled

//  def cpusStillNeeded: Double = cpusPerTask * tasksUnscheduled

  def memStillNeeded: Double = memPerTask * tasksUnscheduled

  // Calculate the maximum number of this jobs tasks that can fit into
  // the specified resources
//  def numTasksToSchedule(cpusAvail: Double, memAvail: Double): Int = {
//    if (cpusAvail == 0.0 || memAvail == 0.0) {
//      0
//    } else {
//      val cpusChoppedToTaskSize = cpusAvail - (cpusAvail % cpusPerTask)
//      val memChoppedToTaskSize = memAvail - (memAvail % memPerTask)
//      val maxTasksThatWillFitByCpu = math.round(cpusChoppedToTaskSize / cpusPerTask)
//      val maxTasksThatWillFitByMem = math.round(memChoppedToTaskSize / memPerTask)
//      val maxTasksThatWillFit = math.min(maxTasksThatWillFitByCpu,
//        maxTasksThatWillFitByMem)
//      math.min(tasksUnscheduled, maxTasksThatWillFit.toInt)
//    }
//  }

  // We cannot use this function for the Zoe Scheduler due to the fact that
  // jobs are not removed from the queue and then re-queued, thus the value
  // in timeInQueueTillFullyScheduled and timeInQueueTillFirstScheduled will
  // be uncorrect.
  // Notherless, using numSchedulingAttempts to check if the first task has been
  // scheduled is wrong, since a schedulingAttempts might not schedule any tasks
  // at all
  def updateTimeInQueueStats(currentTime: Double): Unit = {
    // Every time part of this job is partially scheduled, add to
    // the counter tracking how long it spends in the queue till
    // its final task is scheduled.
    timeInQueueTillFullyScheduled += currentTime - lastEnqueued
    // If this is the first scheduling done for this job, then make a note
    // about how long the job waited in the queue for this first scheduling.
    if (numSchedulingAttempts == 0) {
      timeInQueueTillFirstScheduled += currentTime - lastEnqueued
    }
  }

  def timeInQueueTillFirstScheduled: Double = _timeInQueueTillFirstScheduled

  def timeInQueueTillFirstScheduled_=(value: Double): Unit = {
    _timeInQueueTillFirstScheduled = value
  }

  def timeInQueueTillFullyScheduled: Double = _timeInQueueTillFullyScheduled

  def timeInQueueTillFullyScheduled_=(value: Double): Unit = {
    _timeInQueueTillFullyScheduled = value
  }

  logger.trace("New Job. numTasks: " + numTasks + ", cpusPerTask: " + cpusPerTask + ", memPerTask: " + memPerTask)
}

/**
  * A class that holds a list of jobs, each of which is used to record
  * statistics during a run of the simulator.
  *
  * Keep track of avgJobInterarrivalTime for easy reference later when
  * ExperimentRun wants to record it in experiment result protos.
  */
class Workload(val name: String,
               private var jobs: ListBuffer[Job] = ListBuffer()) extends LazyLogging {
  def getJobs: Seq[Job] = jobs

  def addJobs(jobs: Seq[Job]): Unit = jobs.foreach(job => {
    addJob(job)
  })

  def addJob(job: Job): Unit = {
    assert(job.workloadName == name)
    jobs.append(job)
  }

  def removeJobs(jobs: Seq[Job]): Unit = jobs.foreach(job => {
    removeJob(job)
  })

  def removeJob(job: Job): Unit = {
    assert(job.workloadName == name)
    jobs -= job
  }

  def sortJobs(): Unit = {
    jobs = jobs.sortBy(_.submitted)
  }

  def numJobs: Int = jobs.length

  //  def cpus: Double = jobs.map(j => {j.numTasks * j.cpusPerTask}).sum
  //  def mem: Double = jobs.map(j => {j.numTasks * j.memPerTask}).sum
  // Generate a new workload that has a copy of the jobs that
  // this workload has.
  def copy: Workload = {
    val newWorkload = new Workload(name)
    jobs.foreach(job => {
      newWorkload.addJob(job.copyEverything())
    })
    newWorkload
  }

  def totalJobUsefulThinkTimes: Double = jobs.map(_.usefulTimeScheduling).sum

  def totalJobWastedThinkTimes: Double = jobs.map(_.wastedTimeScheduling).sum

  def avgJobInterarrivalTime: Double = {
    val submittedTimesArray = new Array[Double](jobs.length)
    jobs.map(_.submitted).copyToArray(submittedTimesArray)
    util.Sorting.quickSort(submittedTimesArray)
    // pass along (running avg, count)
    var sumInterarrivalTime = 0.0
    for (i <- 1 until submittedTimesArray.length) {
      sumInterarrivalTime += submittedTimesArray(i) - submittedTimesArray(i - 1)
    }
    sumInterarrivalTime / submittedTimesArray.length
  }

  def jobUsefulThinkTimesPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduledJobs = jobs.filter(_.isScheduled).toList
    // println("Setting up thinkTimesArray of length " +
    //         scheduledJobs.length)
    if (scheduledJobs.nonEmpty) {
      val thinkTimesArray = new Array[Double](scheduledJobs.length)
      scheduledJobs.map(job => {
        job.usefulTimeScheduling
      }).copyToArray(thinkTimesArray)
      util.Sorting.quickSort(thinkTimesArray)
      //println(thinkTimesArray.deep.toSeq.mkString("-*-"))
      // println("Looking up think time percentile value at position " +
      //         ((thinkTimesArray.length-1) * percentile).toInt)
      thinkTimesArray(((thinkTimesArray.length - 1) * percentile).toInt)
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFirstScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFirstScheduled.")
    val scheduledJobs = jobs.filter(_.isScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFirstScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def avgJobQueueTimeTillFullyScheduled: Double = {
    // println("Computing avgJobQueueTimeTillFullyScheduled.")
    val scheduledJobs = jobs.filter(_.isFullyScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(_.timeInQueueTillFullyScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def avgJobRampUpTime: Double = {
    // println("Computing avgJobRampUpTime.")
    val scheduledJobs = jobs.filter(_.isFullyScheduled)
    if (scheduledJobs.nonEmpty) {
      val queueTimes = scheduledJobs.map(job => job.timeInQueueTillFullyScheduled - job.timeInQueueTillFirstScheduled).sum
      queueTimes / scheduledJobs.length
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFirstScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.isScheduled)
    if (scheduled.nonEmpty) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFirstScheduled)
        .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result =
        queueTimesArray(((queueTimesArray.length - 1) * percentile).toInt)
      logger.debug("Looking up job queue time till first scheduled percentile value at position " + (queueTimesArray.length * percentile).toInt + " of " + queueTimesArray.length + ": " + result + ".")
      result
    } else {
      -1.0
    }
  }

  def jobQueueTimeTillFullyScheduledPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    val scheduled = jobs.filter(_.isFullyScheduled)
    if (scheduled.nonEmpty) {
      val queueTimesArray = new Array[Double](scheduled.length)
      scheduled.map(_.timeInQueueTillFullyScheduled)
        .copyToArray(queueTimesArray)
      util.Sorting.quickSort(queueTimesArray)
      val result = queueTimesArray(((queueTimesArray.length - 1) * 0.9).toInt)
      logger.debug("Looking up job queue time till fully scheduled percentile value at position " + (queueTimesArray.length * percentile).toInt + " of " + queueTimesArray.length + ": " + result + ".")
      result
    } else {
      -1.0
    }
  }

  def numSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    logger.debug("Largest 200 job scheduling attempt counts: " + jobs.map(_.numSchedulingAttempts).sorted.takeRight(200).mkString(","))
    val scheduled = jobs.filter(_.numSchedulingAttempts > 0)
    if (scheduled.nonEmpty) {
      val schedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numSchedulingAttempts).copyToArray(schedulingAttemptsArray)
      util.Sorting.quickSort(schedulingAttemptsArray)
      val result = schedulingAttemptsArray(((schedulingAttemptsArray.length - 1) * 0.9).toInt)
      logger.debug("Looking up num job scheduling attempts percentile value at position " + (schedulingAttemptsArray.length * percentile).toInt + " of " + schedulingAttemptsArray.length + ": " + result + ".")
      result
    } else {
      -1.0
    }
  }

  def numTaskSchedulingAttemptsPercentile(percentile: Double): Double = {
    assert(percentile <= 1.0 && percentile >= 0)
    logger.debug("Largest 200 task scheduling attempt counts: " + jobs.map(_.numTaskSchedulingAttempts).sorted.takeRight(200).mkString(","))
    val scheduled = jobs.filter(_.numTaskSchedulingAttempts > 0)
    if (scheduled.nonEmpty) {
      val taskSchedulingAttemptsArray = new Array[Long](scheduled.length)
      scheduled.map(_.numTaskSchedulingAttempts).copyToArray(taskSchedulingAttemptsArray)
      util.Sorting.quickSort(taskSchedulingAttemptsArray)
      val result = taskSchedulingAttemptsArray(((taskSchedulingAttemptsArray.length - 1) * 0.9).toInt)
      logger.debug("Looking up num task scheduling attempts percentile value at position " + (taskSchedulingAttemptsArray.length * percentile).toInt + " of " + taskSchedulingAttemptsArray.length + ": " + result + ".")
      result
    } else {
      -1.0
    }
  }
}

case class WorkloadDesc(
                         cell: String,
                         assignmentPolicy: String,
                         // getJob(0) is called
                         workloadGenerators: List[WorkloadGenerator],
                         cellStateDesc: CellStateDesc,
                         prefillWorkloadGenerators: List[WorkloadGenerator] =
                         List[WorkloadGenerator]()) {
  assert(!cell.contains(" "), {
    "Cell names cannot have spaces in them."
  })
  assert(!assignmentPolicy.contains(" "), {
    "Assignment policies cannot have spaces in them."
  })
  assert(prefillWorkloadGenerators.length ==
    prefillWorkloadGenerators.map(_.workloadName).toSet.size)

  val workloads: ListBuffer[Workload] = ListBuffer[Workload]()

  def generateWorkloads(timeWindow: Double): Unit = {
    workloadGenerators.foreach(workloadGenerator => {
      workloads += workloadGenerator.newWorkload(timeWindow)
    })
  }

  def cloneWorkloads(): ListBuffer[Workload] = {
    val newWorkloads: ListBuffer[Workload] = ListBuffer[Workload]()
    workloads.foreach(workload => {
      newWorkloads += workload.copy
    })
    newWorkloads
  }
}


/**
  * A threadsafe Workload factory.
  */
trait WorkloadGenerator extends LazyLogging {
  val workloadName: String

  /**
    * Generate a workload using this workloadGenerator's parameters
    * The timestamps of the jobs in this workload should fall in the range
    * [0, timeWindow].
    *
    * @param maxCpus                       The maximum number of cpus that the workload returned
    *                                      can contain.
    * @param maxMem                        The maximum amount of mem that the workload returned
    *                                      can contain.
    * @param updatedAvgJobInterarrivalTime if non-None, then the interarrival
    *                                      times of jobs in the workload returned
    *                                      will be approximately this value.
    */
  def newWorkload(timeWindow: Double,
                  maxCpus: Option[Long] = None,
                  maxMem: Option[Long] = None,
                  updatedAvgJobInterarrivalTime: Option[Double] = None)
  : Workload
}

/**
  * An object for building and caching empirical distributions
  * from traces. Caches them because they are expensive to build
  * (require reading from disk) and are re-used between different
  * experiments.
  */
object DistCache extends LazyLogging {
  val distributions: mutable.Map[(String, String), Array[Double]] =
    collection.mutable.Map[(String, String), Array[Double]]()

  /**
    * @param value [0, 1] representing distribution quantile to return.
    */
  def getQuantile(empDistribution: Array[Double],
                  value: Double): Double = {
    // Look up the two closest quantiles and interpolate.
    assert(value >= 0 || value <= 1, {
      "value must be >= 0 and <= 1."
    })
    val rawIndex = value * (empDistribution.length - 1)
    val interpAmount = rawIndex % 1
    if (interpAmount == 0) {
      empDistribution(rawIndex.toInt)
    } else {
      val below = empDistribution(math.floor(rawIndex).toInt)
      val above = empDistribution(math.ceil(rawIndex).toInt)
      below + interpAmount * (below + above)
    }
  }

  def getDistribution(workloadName: String,
                      traceFileName: String): Array[Double] = {
    distributions.getOrElseUpdate((workloadName, traceFileName),
      buildDist(workloadName, traceFileName))
  }

  def buildDist(workloadName: String, traceFileName: String): Array[Double] = {
    assert(workloadName.equals("Batch") || workloadName.equals("Service") || workloadName.equals("Interactive") || workloadName.equals("Batch-MPI"))
    val dataPoints = new collection.mutable.ListBuffer[Double]()
    val refDistribution = new Array[Double](1001)

    var realDataSum: Double = 0.0
    // Custom distribution for interactive jobs
    if (traceFileName.equals("interactive-runtime-dist")) {
      for (_ <- 0 until 50) {
        val dataPoint: Double = Random.nextInt(30)
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
      for (_ <- 0 until 900) {
        val dataPoint: Double = Random.nextInt(50400) + 30
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
      for (_ <- 0 until 50) {
        val dataPoint: Double = Random.nextInt(30) + 50430
        dataPoints.append(dataPoint)
        realDataSum += dataPoint
      }
    } else {
      logger.info("Reading tracefile " + traceFileName + " and building distribution of job data points based on it.")
      val traceSrc = io.Source.fromFile(traceFileName)
      val lines = traceSrc.getLines()

      lines.foreach(line => {
        logger.trace("Original : " + line)
        val parsedLine = line.split(" ")
        // The following parsing code is based on the space-delimited schema
        // used in textfile. See the README for a description.
        // val cell: Double = parsedLine(1).toDouble
        // val allocationPolicy: String = parsedLine(2)
        val isServiceJob: Boolean = parsedLine(2).equals("1")
        val dataPoint: Double = parsedLine(3).toDouble
        // Add the job to this workload if its job type is the same as
        // this generator's, which we determine based on workloadName.
        if ((isServiceJob && workloadName.equals("Service")) ||
          (!isServiceJob && workloadName.equals("Batch")) ||
          (!isServiceJob && workloadName.equals("Batch-MPI")) ||
          (isServiceJob && workloadName.equals("Interactive"))) {
          var occurrences: Int = 1
          // Check if there is just a 5th field, if there is it holds the occurrences value
          // It is a ugly hack to discard other traces files that have more than 5 fields
          if (parsedLine.length > 4) {
            occurrences = parsedLine(4).toInt
            logger.trace("Transformed : - - - " + dataPoint + " " + occurrences)
          }

          dataPoints ++= List.fill(occurrences)(dataPoint)
          realDataSum += dataPoint * occurrences
        }

      })
    }

    assert(dataPoints.nonEmpty, {
      "Trace file must contain at least one data point."
    })
    logger.info("Done reading tracefile of " + dataPoints.length + " jobs, average of real data points was " + realDataSum / dataPoints.length + ". Now constructing distribution.")
    val dataPointsArray = dataPoints.toArray
    util.Sorting.quickSort(dataPointsArray)
    for (i <- 0 to 1000) {
      // Store summary quantiles.
      // 99.9 %tile = length * .999
      val index = ((dataPointsArray.length - 1) * i / 1000.0).toInt
      val currPercentile =
        dataPointsArray(index)
      refDistribution(i) = currPercentile
      // println("refDistribution(%d) = dataPointsArray(%d) = %f"
      //         .format(i, index, currPercentile))
    }
    refDistribution
  }
}

object PrefillJobListsCache extends LazyLogging {
  // Map from (workloadname, traceFileName) -> list of jobs.
  val jobLists: mutable.Map[(String, String), Iterable[Job]] =
    collection.mutable.Map[(String, String), Iterable[Job]]()
  val cpusPerTaskDistributions: mutable.Map[String, Array[Double]] =
    collection.mutable.Map[String, Array[Double]]()
  val memPerTaskDistributions: mutable.Map[String, Array[Double]] =
    collection.mutable.Map[String, Array[Double]]()

  /**
    * When we load jobs from a trace file, we fill in the duration for all jobs
    * that don't have an end event as -1, then we fill in the duration for all
    * such jobs.
    */
  def getJobs(workloadName: String,
              traceFileName: String,
              timeWindow: Double): Iterable[Job] = {
    val jobs = getOrLoadJobs(workloadName, traceFileName)
    // Update duration of jobs with duration set to -1.
    jobs.foreach(job => {
      if (job.taskDuration == -1)
        job.taskDuration = timeWindow
    })
    jobs
  }

  def getCpusPerTaskDistribution(workloadName: String,
                                 traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    logger.trace("getCpusPerTaskDistribution called.")
    cpusPerTaskDistributions.getOrElseUpdate(
      traceFileName, buildDist(jobs.map(_.cpusPerTask.toDouble).toArray))
  }

  def getMemPerTaskDistribution(workloadName: String,
                                traceFileName: String): Array[Double] = {
    //TODO(andyk): Fix this ugly hack of prepending "Prefill".
    val jobs = getOrLoadJobs("Prefill" + workloadName, traceFileName)
    logger.trace("getMemPerTaskDistribution called.")
    memPerTaskDistributions.getOrElseUpdate(
      traceFileName, buildDist(jobs.map(_.memPerTask.toDouble).toArray))
  }

  def getOrLoadJobs(workloadName: String,
                    traceFileName: String): Iterable[Job] = {
    val cachedJobs = jobLists.getOrElseUpdate((workloadName, traceFileName), {
      val newJobs = collection.mutable.Map[String, Job]()
      val traceSrc = io.Source.fromFile(traceFileName)
      val lines: Iterator[String] = traceSrc.getLines()
      lines.foreach(line => {
        val parsedLine = line.split(" ")
        // The following parsing code is based on the space-delimited schema
        // used in textfile. See the README Andy made for a description of it.

        // Parse the fields that are common between both (6 & 8 column)
        // row formats.
        val timestamp: Double = parsedLine(1).toDouble
        val jobID: String = parsedLine(2)
        val isHighPriority: Boolean = parsedLine(3).equals("1")
        val schedulingClass: Int = parsedLine(4).toInt
        // Label the job according to PBB workload split. SchedulingClass 0 & 1
        // are batch, 2 & 3 are service.
        // (isServiceJob == false) => this is a batch job.
        val isServiceJob = isHighPriority && schedulingClass != 0 && schedulingClass != 1
        // Add the job to this workload if its job type is the same as
        // this generator's, which we determine based on workloadName
        // and if we haven't reached the resource size limits of the
        // requested workload.
        if ((isServiceJob && workloadName.equals("PrefillService")) ||
          (!isServiceJob && workloadName.equals("PrefillBatch")) ||
          (!isServiceJob && workloadName.equals("PrefillBatch-MPI")) ||
          (isServiceJob && workloadName.equals("PrefillInteractive")) ||
          workloadName.equals("PrefillBatchService")) {
          if (parsedLine(0).equals("11")) {
            assert(parsedLine.length == 8, {
              "Found " + parsedLine.length + " fields, expecting 8"
            })

            val numTasks: Int = parsedLine(5).toInt
            assert(numTasks != 0, {
              "Number of tasks for job " + jobID + " is " + numTasks
            })
            // The tracefile has cpus in cores. Our simulator in millicores.
            val cpusPerJob: Long = (parsedLine(6).toDouble * 1000).toLong
            // The tracefile has memory in bytes, our simulator is in bytes as well.
            val memPerJob: Long = parsedLine(7).toLong
            val newJob = Job(UniqueIDGenerator.generateUniqueID, 0.0, numTasks, -1, workloadName, cpusPerJob / numTasks, memPerJob / numTasks)
            newJobs(jobID) = newJob
            // Update the job/task duration for jobs that we have end times for.
          } else if (parsedLine(0).equals("12")) {
            assert(parsedLine.length == 6, {
              "Found " + parsedLine.length + " fields, expecting 6"
            })
            assert(newJobs.contains(jobID), {
              "Expect to find job " + jobID + " in newJobs."
            })
            newJobs(jobID).taskDuration = timestamp
          } else {
            logger.error("Invalid trace event type code " + parsedLine(0) + " in tracefile " + traceFileName)
          }
        }
      })
      traceSrc.close()
      // Add the newly parsed list of jobs to the cache.
      logger.debug("loaded " + newJobs.size + " newJobs.")
      newJobs.values
    })
    // Return a copy of the cached jobs since the job durations will
    // be updated according to the experiment time window.
    logger.debug("returning " + cachedJobs.size + " jobs from cache")
    cachedJobs.map(_.copyEverything())
  }

  def buildDist(dataPoints: Array[Double]): Array[Double] = {
    val refDistribution = new Array[Double](1001)
    assert(dataPoints.length > 0, {
      "dataPoints must contain at least one data point."
    })
    util.Sorting.quickSort(dataPoints)
    for (i <- 0 to 1000) {
      // Store summary quantiles. 99.9 %tile = length * .999
      val index = ((dataPoints.length - 1) * i / 1000.0).toInt
      val currPercentile =
        dataPoints(index)
      refDistribution(i) = currPercentile
    }
    refDistribution
  }
}

