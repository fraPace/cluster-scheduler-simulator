package ClusterSchedulingSimulation.core

import ClusterSchedulingSimulation.core.ClaimDelta.ResizePolicy.ResizePolicy
import ClusterSchedulingSimulation.utils.{Constant, Seed, StatisticsArray}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object ClaimDeltaStatus extends Enumeration {
  type ClaimDeltaStatus = Value
  val Starting, Running, Dead, OOMRisk, OOMKilled, CPUKilled, CPURisk = Value

  def exists(claimDeltaStatus: ClaimDeltaStatus): Boolean = values.exists(_.equals(claimDeltaStatus))
  def valuesToString(): String = {values.toString()}
}

object ClaimDelta {
  object ResizePolicy extends Enumeration {
    // Value close to 1 will remove all the non-used resources
    @inline final val DecreaseRatio: Double = 1
    // Value greater than 0 will add more resources than used above the safe margin
    @inline final val IncreaseRatio: Double = 0
    @inline final val SafeMargin: Double = System.getProperty("nosfe.simulator.core.scheduler.safeMargin", "0").toDouble

    type ResizePolicy = Value
    val Instant, Average, Maximum, MovingAverage, MovingMaximum, None = Value

    def exists(resizePolicy: ResizePolicy): Boolean = values.exists(_.equals(resizePolicy))
    def valuesToString(): String = {values.toString()}

    def calculate(values: StatisticsArray, value: Long, resizePolicy: ResizePolicy): Long = {
      resizePolicy match {
        case ClaimDelta.ResizePolicy.Instant =>
          value

        case ClaimDelta.ResizePolicy.Average =>
          Math.ceil(values.getMean(value)).toLong

        case ClaimDelta.ResizePolicy.Maximum =>
          values.getMax(value)

        case ClaimDelta.ResizePolicy.MovingAverage =>
          Math.ceil(values.getMeanWindow(value)).toLong

        case ClaimDelta.ResizePolicy.MovingMaximum =>
          values.getMaxWindow(value)
      }
    }

    val safeGuardCache: mutable.HashMap[Long, (Double, Double)] = mutable.HashMap[Long, (Double, Double)]()
    def getSafeGuardValue(jobID: Long, currentTime: Double): Double = {
      def generateSafeGuard(): Double = {
        SafeMargin
      }

//      // We look for a cached value, otherwise we get a new one
//      var (time, _safeGuard) = safeGuardCache.getOrElse(jobID, (currentTime, -1.0))
//      if(time != currentTime || _safeGuard == -1.0)
//        _safeGuard = Math.abs(generateSafeGuard())
//      assert(_safeGuard >= 0 && _safeGuard <= 1.0)
//      safeGuardCache(jobID) = (currentTime, _safeGuard)
//      _safeGuard

      generateSafeGuard()
    }
  }

  var uniqueId: Long = 0

  def apply(
             scheduler: Scheduler,
             machineID: Int,
             machineSeqNum: Long,
             duration: Double,
             requestedCpus: Long,
             requestedMem: Long,
             onlyLocked: Boolean = false,
             job: Option[Job] = None,
             taskType: TaskType.Value = TaskType.None
           ): ClaimDelta = {
    uniqueId += 1
    new ClaimDelta(uniqueId, scheduler, machineID, machineSeqNum, duration, requestedCpus, requestedMem, onlyLocked, job, taskType, scheduler.simulator.currentTime)
  }

}

class ClaimDelta(val id: Long,
                 val scheduler: Scheduler,
                 val machineID: Int,
                 val machineSeqNum: Long,
                 val duration: Double,
                 val requestedCpus: Long,
                 val requestedMem: Long,
                 val onlyLocked: Boolean = false,
                 val job: Option[Job] = None,
                 val taskType: TaskType.Value = TaskType.None,
                 val creationTime: Double = 0) extends LazyLogging {
  assert(creationTime >= 0, {
    "The creation time for the claimDelta cannot be negative. " + creationTime + "s."
  })

  lazy val loggerPrefix: String = "[" + id + " (" + taskType + ")]"

  var currentCpus: Long = requestedCpus
  var currentMem: Long = requestedMem
  var status: ClaimDeltaStatus.Value = ClaimDeltaStatus.Starting
  var cpusStillNeeded: Long = 0L
  var memStillNeeded: Long = 0L

  var cpusUtilization: StatisticsArray = new StatisticsArray()
  var memUtilization: StatisticsArray = new StatisticsArray()

  /**
    * Claim `@code cpus` and `@code mem` from `@code cellState`.
    * Increments the sequenceNum of the machine with ID referenced
    * by machineID.
    */
  def apply(cellState: CellState, locked: Boolean = false): Unit = {
    assert(cellState.claimDeltas.add(this), {
       "We already added this claimDelta! (" + this.id + ")"
    })

    cellState.assignResources(scheduler, machineID, currentCpus, currentMem, locked)
    // Mark that the machine has changed, used for testing for conflicts
    // when using optimistic concurrency.
    cellState.machineSeqNums(machineID) += 1
    status = ClaimDeltaStatus.Running
    cellState.claimDeltasPerMachine(machineID) = cellState.claimDeltasPerMachine(machineID) :+ this
  }

  def unApply(cellState: CellState, locked: Boolean = false): Unit = {
    assert(cellState.claimDeltas.remove(this), {
      "We are removing a claimDelta that was not in the system! (" + this.id + ")"
    })

    cellState.freeResources(scheduler, machineID, currentCpus, currentMem, onlyLocked || locked)
    status = ClaimDeltaStatus.Dead
    cellState.claimDeltasPerMachine(machineID) = cellState.claimDeltasPerMachine(machineID).filter(_ != this)
  }

  def checkResourcesLimits(currentTime: Double): ClaimDeltaStatus.Value = {
    checkResourcesLimits(job.get.cpuUtilization(currentTime), job.get.memoryUtilization(currentTime))
  }

  def checkResourcesLimits(cpus: Long, mem: Long): ClaimDeltaStatus.Value = {
    if (currentCpus < cpus) {
//      scheduler.simulator.logger.info(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
//        " This claimDelta on machine " + machineID + " is using more CPUs than allocated. Originally Requested: " +
//        requestedCpus + " Allocated: " + currentCpus + " Current: " + cpus)
      status = ClaimDeltaStatus.CPUKilled
    }

    if (currentMem < mem) {
//      scheduler.simulator.logger.warn(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
//        " This claimDelta on machine " + machineID + " is using more memory than allocated. Originally Requested: " +
//        requestedMem + " Allocated: " + currentMem + " Current: " + mem + " (" + (Math.abs(1 - (currentMem / mem.toDouble)) * 100) + " %)")
      status = ClaimDeltaStatus.OOMKilled
    }
    status
  }

  def calculateNextAllocations(cpus: Long, mem: Long, resizePolicy: ResizePolicy): (Long, Long) = {
    val job: Job = this.job.get
    val safeGuard = ClaimDelta.ResizePolicy.getSafeGuardValue(job.id, scheduler.simulator.currentTime)

    var deltaCpus: Long =
      if(resizePolicy != ClaimDelta.ResizePolicy.None)
//        (ClaimDelta.ResizePolicy.calculate(cpusUtilization, cpus, resizePolicy) * (1 + safeGuard)).toLong - currentCpus
        (ClaimDelta.ResizePolicy.calculate(cpusUtilization, cpus, resizePolicy) + (requestedCpus * safeGuard)).toLong - currentCpus
      else 0
    if (deltaCpus > 0) {
      deltaCpus += (deltaCpus * ClaimDelta.ResizePolicy.IncreaseRatio).toLong
      // Enforce limitations imposed by the user allocation request
      if(deltaCpus + currentCpus >= requestedCpus)
        deltaCpus = requestedCpus - currentCpus
    } else if (deltaCpus < 0){
      // Negative value
      deltaCpus -= (deltaCpus * (1 - ClaimDelta.ResizePolicy.DecreaseRatio)).toLong
    }

    var deltaMem: Long =
      if(resizePolicy != ClaimDelta.ResizePolicy.None)
//        (ClaimDelta.ResizePolicy.calculate(memUtilization, mem, resizePolicy) * (1 + safeGuard)).toLong - currentMem
        (ClaimDelta.ResizePolicy.calculate(memUtilization, mem, resizePolicy) + (requestedMem * safeGuard)).toLong - currentMem
      else 0
    if (deltaMem > 0) {
      deltaMem += (deltaMem * ClaimDelta.ResizePolicy.IncreaseRatio).toLong
      // Enforce limitations imposed by the user allocation request
      if(deltaMem + currentMem > requestedMem)
        deltaMem = requestedMem - currentMem
    } else if(deltaMem < 0){
      // Negative value
      deltaMem -= (deltaMem * (1 - ClaimDelta.ResizePolicy.DecreaseRatio)).toLong
    }
    (currentCpus + deltaCpus, currentMem + deltaMem)
  }

  def resize(cellState: CellState, cpus: Long, mem: Long, locked: Boolean = false, resizePolicy: ResizePolicy): (Long, Long) = {
    scheduler.simulator.logger.debug(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
      " Resizing this claimDelta from " + currentCpus + " CPUs and %.2fGiB".format(currentMem / Constant.GiB.toDouble) +
      " memory to " + cpus + " CPUs and %.2fGiB".format(mem / Constant.GiB.toDouble) + " Memory. The status is " +
      status + " and the resize policy is " + resizePolicy)

    status = ClaimDeltaStatus.Running
    cpusStillNeeded = 0L
    memStillNeeded = 0L

    var deltaCpus: Long =
      if(resizePolicy != ClaimDelta.ResizePolicy.None)
        cpus - currentCpus
      else 0
    if (deltaCpus > 0) {
      if (cellState.availableCpusPerMachine(machineID) >= deltaCpus) {
        cellState.assignResources(scheduler, machineID, deltaCpus, 0L, locked)
      }
      else {
        scheduler.simulator.logger.debug(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
          " This claimDelta on machine " + machineID + " may crash because there are not enough CPUs to allocate. Originally Requested: " +
          requestedCpus + " Allocated: "+ currentCpus + " Current Requested: " + cpus + " Slack: " + deltaCpus)
        status = ClaimDeltaStatus.CPURisk
        cpusStillNeeded = deltaCpus
        deltaCpus = 0
      }
    } else if (deltaCpus < 0){
      cellState.freeResources(scheduler, machineID, -deltaCpus, 0L, locked)
    }
    currentCpus += deltaCpus

    var deltaMem: Long =
      if(resizePolicy != ClaimDelta.ResizePolicy.None)
        mem - currentMem
      else 0
    if (deltaMem > 0) {
      if (cellState.availableMemPerMachine(machineID) >= deltaMem) {
        cellState.assignResources(scheduler, machineID, 0, deltaMem, locked)
      }
      else {
        scheduler.simulator.logger.debug(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
          " This claimDelta on machine " + machineID + " may crash because there are not enough memory to allocate. Originally Requested: " +
          requestedMem + " Allocated: "+ currentMem + " Current Requested: " + mem + " Slack: " + deltaMem)
        status = ClaimDeltaStatus.OOMRisk
        memStillNeeded = deltaMem
        deltaMem = 0
      }
    } else if(deltaMem < 0){
      cellState.freeResources(scheduler, machineID, 0, -deltaMem, locked)
    }
    currentMem += deltaMem

    scheduler.simulator.logger.debug(scheduler.loggerPrefix + job.get.loggerPrefix + loggerPrefix +
      " This claimDelta now has " + currentCpus + " CPUs and %.2fGiB".format(currentMem / Constant.GiB.toDouble) +
      " memory. Resources slack was: " + deltaCpus + " CPUs " + deltaMem + " Memory. The status is " + status)

    (deltaCpus, deltaMem)
  }

}

class SchedulerDesc(val name: String,
                    val constantThinkTimes: Map[String, Double],
                    val perTaskThinkTimes: Map[String, Double])

/**
  * A Scheduler maintains `@code Job`s submitted to it in a queue, and
  * attempts to match those jobs with resources by making "job scheduling
  * decisions", which take a certain amount of "scheduling time".
  * A simulator is responsible for interacting with a Scheduler, e.g.,
  * by deciding which workload types should be assigned to which scheduler.
  * A Scheduler must accept Jobs associated with any workload type (workloads
  * are identified by name), though it can do whatever it wants with those
  * jobs, include, optionally, dropping them on the floor, or handling jobs
  * from different workloads differently, which concretely, means taking
  * a different amount of "scheduling time" to schedule jobs from different
  * workloads.
  *
  * @param name                   Unique name that this scheduler is known by for the purposes
  *                               of jobs being assigned to.
  * @param constantThinkTimes     Map from workloadNames to constant times,
  *                               in seconds, this scheduler uses to schedule each job.
  * @param perTaskThinkTimes      Map from workloadNames to times, in seconds,
  *                               this scheduler uses to schedule each task that is assigned to
  *                               a scheduler of that name.
  * @param numMachinesToBlackList a positive number representing how many
  *                               machines (chosen randomly) this scheduler should ignore when
  *                               making scheduling decisions.
  */
abstract class Scheduler(val name: String,
                         constantThinkTimes: Map[String, Double],
                         perTaskThinkTimes: Map[String, Double],
                         numMachinesToBlackList: Double,
                         allocationMode: AllocationMode.Value) {
  assert(constantThinkTimes.size == perTaskThinkTimes.size)
  assert(numMachinesToBlackList >= 0)

  assert(AllocationMode.exists(allocationMode), {
    "allocationMode must be one of: {" + AllocationMode.valuesToString() + "}, but it was " + allocationMode + "."
  })

  // Keep a cache of candidate pools around, indexed by their length
  // to avoid the overhead of the Array.range call in our inner scheduling
  // loop.
  val candidatePoolCache: mutable.HashMap[Int, mutable.IndexedSeq[Int]] = mutable.HashMap[Int, mutable.IndexedSeq[Int]]()
  val randomNumberGenerator = new util.Random(Seed())
  protected var pendingQueue: collection.mutable.Queue[Job] = collection.mutable.Queue[Job]()
  protected var runningQueue: collection.mutable.Queue[Job] = collection.mutable.Queue[Job]()
  // This gets set when this scheduler is added to a Simulator.
  // TODO(andyk): eliminate this pointer and make the scheduler
  //              more functional.
  // TODO(andyk): Clean up these <subclass>Simulator classes
  //              by templatizing the Scheduler class and having only
  //              one simulator of the correct type, instead of one
  //              simulator for each of the parent and child classes.
  var simulator: ClusterSimulator = _
  var scheduling: Boolean = false
  // Job transaction stat counters.
  var numSuccessfulTransactions: Int = 0
  var numFailedTransactions: Int = 0
  var numRetriedTransactions: Int = 0
  var dailySuccessTransactions: mutable.HashMap[Int, Int] = mutable.HashMap[Int, Int]()
  var dailyFailedTransactions: mutable.HashMap[Int, Int] = mutable.HashMap[Int, Int]()
  var numJobsTimedOutScheduling: Int = 0
  // Task transaction stat counters.
  var numSuccessfulTaskTransactions: Int = 0
  var numFailedTaskTransactions: Int = 0
  var numNoResourcesFoundSchedulingAttempts: Int = 0
  // When trying to place a task, count the number of machines we look at
  // that the task doesn't fit on. This is a sort of wasted work that
  // causes the simulation to go slow.
  var failedFindVictimAttempts: Int = 0
  var totalUsefulTimeScheduling = 0.0 // in seconds
  var totalWastedTimeScheduling = 0.0 // in seconds
  var firstAttemptUsefulTimeScheduling = 0.0 // in seconds
  var firstAttemptWastedTimeScheduling = 0.0 // in seconds
  var dailyUsefulTimeScheduling: mutable.HashMap[Int, Double] = mutable.HashMap[Int, Double]()
  var dailyWastedTimeScheduling: mutable.HashMap[Int, Double] = mutable.HashMap[Int, Double]()
  // Also track the time, in seconds, spent scheduling broken out by
  // workload type. Note that all Schedulers (even, e.g., SinglePath
  // schedulers) can handle jobs from multiple workload generators.
  var perWorkloadUsefulTimeScheduling: mutable.HashMap[String, Double] = mutable.HashMap[String, Double]()
  var perWorkloadWastedTimeScheduling: mutable.HashMap[String, Double] = mutable.HashMap[String, Double]()

  var cpuUtilizationConflicts: Long = 0
  var memUtilizationConflicts: Long = 0

  override def toString: String = name

  def loggerPrefix: String = "[%.2f".format(simulator.currentTime) + "][" + name + "]"

  def printExtraStats(prefix: String): Unit = {}

  def wakeUp(): Unit = {
    //FIXME: Fix this hack thing to force the user to override this method
    throw new Exception("Please override this method.")
  }

  // Add a job to this scheduler's job queue.
  var positionInQueue: Long = 0
  def addJob(job: Job): Unit = {
    checkRegistered()

    assert(job.tasksUnscheduled > 0, {
      "A job must have at least one unscheduled task."
    })
    // Make sure the perWorkloadTimeScheduling Map has a key for this job's
    // workload type, so that we still print something for statistics about
    // that workload type for this scheduler, even if this scheduler never
    // actually gets a chance to schedule a job of that type.
    perWorkloadUsefulTimeScheduling(job.workloadName) =
      perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName, 0.0)
    perWorkloadWastedTimeScheduling(job.workloadName) =
      perWorkloadWastedTimeScheduling.getOrElse(job.workloadName, 0.0)
    job.lastEnqueued = simulator.currentTime
    job.initialQueuePosition = positionInQueue

    positionInQueue += 1
  }

  def checkRegistered(): Unit = {
    assert(simulator != null, {
      "You must assign a simulator to a Scheduler before you can use it."
    })
  }

  /**
    * Creates and applies ClaimDeltas for all available resources in the
    * provided `@code cellState`. This is intended to leave no resources
    * free in cellState, thus it doesn't use minCpu or minMem because that
    * could lead to leaving fragmentation. I haven't thought through
    * very carefully if floating point math could cause a problem here.
    */
  def scheduleAllAvailable(cellState: CellState,
                           locked: Boolean): Seq[ClaimDelta] = {
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    for (mID <- 0 until cellState.numMachines) {
      val cpusAvail = cellState.availableCpusPerMachine(mID)
      val memAvail = cellState.availableMemPerMachine(mID)
      if (cpusAvail > 0.0 || memAvail > 0.0) {
        // Create and apply a claim delta.
        assert(mID >= 0 && mID < cellState.machineSeqNums.length)
        //TODO(andyk): Clean up semantics around taskDuration in ClaimDelta
        //             since we want to represent offered resources, not
        //             tasks with these deltas.
        val claimDelta = ClaimDelta(this,
          mID,
          cellState.machineSeqNums(mID),
          -1.0,
          cpusAvail,
          memAvail)
        claimDelta.apply(cellState, locked)
        claimDeltas += claimDelta
      }
    }
    claimDeltas
  }

  /**
    * Given a job and a cellstate, find machines that the tasks of the
    * job will fit into, and allocate the resources on that machine to
    * those tasks, accounting those resources to this scheduler, modifying
    * the provided cellstate (by calling apply() on the created deltas).
    *
    * Implements the following randomized first fit scheduling algorithm:
    * while(more machines in candidate pool and more tasks to schedule):
    * candidateMachine = random machine in pool
    * if(candidate machine can hold at least one of this jobs tasks):
    * create a delta assigning the task to that machine
    * else:
    * remove from candidate pool
    *
    * @return List of deltas, one per task, so that the transactions can
    *         be played on some other cellstate if desired.
    */
  def scheduleJob(job: Job,
                  cellState: CellState,
                  taskType: TaskType.Value = TaskType.None): ListBuffer[ClaimDelta] = {
    assert(simulator != null)
    assert(cellState != null)
    assert(job.cpusPerTask <= cellState.cpusPerMachine, {
      "Looking for machine with " + job.cpusPerTask + " CPUs, but machines only have " + cellState.cpusPerMachine + " CPUs."
    })
    assert(job.memPerTask <= cellState.memPerMachine, {
      "Looking for machine with " + job.memPerTask + " mem, but machines only have " + cellState.memPerMachine + " mem."
    })
    val claimDeltas = collection.mutable.ListBuffer[ClaimDelta]()

    // Cache candidate pools in this scheduler for performance improvements.
    val candidatePool = candidatePoolCache.getOrElseUpdate(cellState.numMachines, Array.range(0, cellState.numMachines))

    var numRemainingTasks = taskType match {
      case TaskType.Core => job.coreTasksUnscheduled
      case TaskType.Elastic => job.elasticTasksUnscheduled
      case TaskType.None => job.tasksUnscheduled
    }
    var remainingCandidates = math.max(0, cellState.numMachines - numMachinesToBlackList).toInt
    while (numRemainingTasks > 0 && remainingCandidates > 0) {
      // Pick a random machine out of the remaining pool, i.e., out of the set
      // of machineIDs in the first remainingCandidate slots of the candidate
      // pool.
      val candidateIndex = randomNumberGenerator.nextInt(remainingCandidates)
      val currMachID = candidatePool(candidateIndex)

      // If one of this job's tasks will fit on this machine, then assign
      // to it by creating a claimDelta and leave it in the candidate pool.
      if (cellState.availableCpusPerMachine(currMachID) >= job.cpusPerTask &&
        cellState.availableMemPerMachine(currMachID) >= job.memPerTask) {
        assert(currMachID >= 0 && currMachID < cellState.machineSeqNums.length)
        val claimDelta = ClaimDelta(this,
          currMachID,
          cellState.machineSeqNums(currMachID),
          job.taskDuration,
          job.cpusPerTask,
          job.memPerTask,
          job = Option(job),
          taskType = taskType)
        claimDelta.apply(cellState = cellState)
        claimDeltas += claimDelta
        numRemainingTasks -= 1
      } else {
        failedFindVictimAttempts += 1
        // Move the chosen candidate to the end of the range of
        // remainingCandidates so that we won't choose it again after we
        // decrement remainingCandidates. Do this by swapping it with the
        // machineID currently at position (remainingCandidates - 1)
        candidatePool(candidateIndex) = candidatePool(remainingCandidates - 1)
        candidatePool(remainingCandidates - 1) = currMachID
        remainingCandidates -= 1
        simulator.logger.debug(name + " in scheduling algorithm, tried machine " + currMachID + ", but " + job.cpusPerTask + " CPUs and " + job.memPerTask +
          " mem are required, and it only has " + cellState.availableCpusPerMachine(currMachID) + " cpus and " + cellState.availableMemPerMachine(currMachID) + " mem available.")
      }
    }
    claimDeltas
  }

  // Give up on a job if (a) it hasn't scheduled a single task in
  // 100 tries or (b) it hasn't finished scheduling after 1000 tries.
  def giveUpSchedulingJob(job: Job): Boolean = {
    allocationMode match {
      case AllocationMode.Incremental =>
        (job.numSchedulingAttempts > 100 && job.tasksUnscheduled == job.coreTasks) || job.numSchedulingAttempts > 1000
      case AllocationMode.All => job.numSchedulingAttempts > 1000
      case _ => false
    }
  }

  def pendingQueueSize: Int = pendingQueue.size

  def runningQueueSize: Int = runningQueue.size

  def isMultiPath: Boolean =
    constantThinkTimes.values.toSet.size > 1 ||
      perTaskThinkTimes.values.toSet.size > 1

  def recordUsefulTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, {
      "This scheduler has not been added to a simulator yet."
    })
    // Scheduler level stats.
    totalUsefulTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyUsefulTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptUsefulTimeScheduling += timeScheduling
    }
    simulator.logger.debug("Recorded " + timeScheduling + " seconds of " + name + " useful think time, total now: " + totalUsefulTimeScheduling + ".")

    // Job/workload level stats.
    job.usefulTimeScheduling += timeScheduling
    simulator.logger.debug("Recorded " + timeScheduling + " seconds of job " + job.id + " useful think time, total now: " + simulator.workloads.filter(_.name == job.workloadName).head.totalJobUsefulThinkTimes + ".")

    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadUsefulTimeScheduling(job.workloadName) =
      perWorkloadUsefulTimeScheduling.getOrElse(job.workloadName, 0.0) +
        timeScheduling
  }

  def recordWastedTimeScheduling(job: Job,
                                 timeScheduling: Double,
                                 isFirstSchedAttempt: Boolean): Unit = {
    assert(simulator != null, {
      "This scheduler has not been added to a simulator yet."
    })
    // Scheduler level stats.
    totalWastedTimeScheduling += timeScheduling
    addDailyTimeScheduling(dailyWastedTimeScheduling, timeScheduling)
    if (isFirstSchedAttempt) {
      firstAttemptWastedTimeScheduling += timeScheduling
    }
    simulator.logger.debug("Recorded " + timeScheduling + " seconds of " + name + " wasted think time, total now: " + totalWastedTimeScheduling + ".")

    // Job/workload level stats.
    job.wastedTimeScheduling += timeScheduling
    simulator.logger.debug("Recorded " + timeScheduling + " seconds of job " + job.id + " wasted think time, total now: " + simulator.workloads.filter(_.name == job.workloadName).head.totalJobWastedThinkTimes + ".")

    // Also track per-path (i.e., per workload) scheduling times
    perWorkloadWastedTimeScheduling(job.workloadName) =
      perWorkloadWastedTimeScheduling.getOrElse(job.workloadName, 0.0) +
        timeScheduling
  }

  def addDailyTimeScheduling(counter: mutable.HashMap[Int, Double],
                             timeScheduling: Double): Unit = {
    val index: Int = math.floor(simulator.currentTime / 86400).toInt
    val currAmt: Double = counter.getOrElse(index, 0.0)
    counter(index) = currAmt + timeScheduling
  }

  /**
    * Computes the time, in seconds, this scheduler requires to make
    * a scheduling decision for `@code job`.
    *
    * @param job the job to determine this schedulers think time for
    */
  def getThinkTime(job: Job): Double = {
    assert(constantThinkTimes.contains(job.workloadName))
    assert(perTaskThinkTimes.contains(job.workloadName))
    constantThinkTimes(job.workloadName) +
      perTaskThinkTimes(job.workloadName) * job.tasksUnscheduled.toFloat
  }

  /**
    * Computes the time, in seconds, this scheduler requires to make
    * a scheduling decision for `@code job`.
    *
    * @param job             the job to determine this schedulers think time for
    * @param unscheduledTask the number of tasks left to be scheduled
    */
  def getThinkTime(job: Job, unscheduledTask: Int): Double = {
    assert(constantThinkTimes.contains(job.workloadName))
    assert(perTaskThinkTimes.contains(job.workloadName))
    constantThinkTimes(job.workloadName) +
      perTaskThinkTimes(job.workloadName) * unscheduledTask.toFloat
  }
}

class PrefillScheduler(cellState: CellState)
  extends Scheduler(name = "prefillScheduler",
    constantThinkTimes = Map[String, Double](),
    perTaskThinkTimes = Map[String, Double](),
    numMachinesToBlackList = 0,
    allocationMode = AllocationMode.Incremental) {


  def scheduleWorkloads(workloads: Seq[Workload]): Unit = {
    // Prefill jobs that exist at the beginning of the simulation.
    // Setting these up is similar to loading jobs that are part
    // of the simulation run; they need to be scheduled onto machines
    simulator.logger.info("Prefilling cell-state with " + workloads.length + " workloads.")
    workloads.foreach(workload => {
      simulator.logger.info("Prefilling cell-state with " + workload.numJobs + " jobs from workload " + workload.name + ".")
      //var i = 0
      workload.getJobs.foreach(job => {
        //i += 1
        // println("Prefilling %d %s job id - %d."
        //         .format(i, workload.name, job.id))
        if (job.cpusPerTask > cellState.cpusPerMachine ||
          job.memPerTask > cellState.memPerMachine) {
          simulator.logger.warn("IGNORING A JOB REQUIRING " + job.cpusPerTask + " CPU & " + job.memPerTask + " MEM PER TASK " +
            "BECAUSE machines only have " + cellState.cpusPerMachine + " cpu / " + cellState.memPerMachine + " mem.")
        } else {
          val claimDeltas = scheduleJob(job, cellState)
          // assert(job.numTasks == claimDeltas.length,
          //        "Prefill job failed to schedule.")
          cellState.scheduleEndEvents(claimDeltas)

          simulator.logger.info("After prefill, common cell state now has " + cellState.totalOccupiedCpus / cellState.totalCpus * 100.0 + "%% (" + cellState.totalOccupiedCpus + ") " +
            "cpus and " + cellState.totalOccupiedMem / cellState.totalMem * 100.0 + "%% (" + cellState.totalOccupiedMem + ") mem occupied.")
        }
      })
    })
  }
}
