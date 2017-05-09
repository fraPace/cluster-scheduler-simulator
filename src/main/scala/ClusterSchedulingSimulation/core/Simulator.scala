package ClusterSchedulingSimulation.core

import ch.qos.logback.classic.Level
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


object EventType extends Enumeration {
  type EventTypes = Value
  val Remove, Trigger = Value

  def exists(eventType: EventTypes): Boolean = values.exists(_.equals(eventType))
  def valuesToString(): String = {values.toString()}
}

/**
  * A simple, generic, discrete event simulator. A modified version of the
  * basic discrete event simulator code from Programming In Scala, pg 398,
  * http://www.amazon.com/Programming-Scala-Comprehensive-Step-step/dp/0981531601
  */
abstract class Simulator(logging: Boolean = false) extends LazyLogging {

  type Action = () => Unit

  override lazy val logger: Logger = {
    val rootLogger: ch.qos.logback.classic.Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[ch.qos.logback.classic.Logger]
    if(!logging)
      rootLogger.setLevel(Level.WARN)
    Logger(rootLogger)
  }

  var eventId: Long = 0

  implicit object WorkItemOrdering extends Ordering[WorkItem] {
    /** Returns an integer whose sign communicates how x compares to y.
      *
      * The result sign has the following meaning:
      *
      *  - negative if x < y
      *  - positive if x > y
      *  - zero otherwise (if x == y)
      */
    def compare(a: WorkItem, b: WorkItem): Int = {
      //      if (a.time < b.time) -1
      //      else if (a.time > b.time) 1
      //      else 0
      a.time.compare(b.time)
    }
  }

  protected var curtime: Double = 0.0 // simulation time, in seconds
  private[this] var _agenda = Vector[WorkItem]()
  private[this] var _totalEventsProcessed: Long = 0

  def removeIf(p: WorkItem => Boolean): Unit = {
    _agenda = _agenda.filterNot(p)
  }

  /**
    * Run the simulation for `@code runTime` virtual (i.e., simulated)
    * seconds or until `@code wallClockTimeout` seconds of execution
    * time elapses.
    *
    * @return Pair (boolean, double)
    *         1. true if simulation ran till runTime or completion, and false
    *         if simulation timed out.
    *         2. the simulation total time
    */
  def run(runTime: Option[Double] = None,
          wallClockTimeout: Option[Double] = None): (Boolean, Double, Long) = {

    afterDelay(-1) {
      logger.info("*** Simulation started, time = " + currentTime + ". ***")
    }
    // Record wall clock time at beginning of simulation run.
    val startWallTime = System.currentTimeMillis()

    def timedOut: Boolean = {
      val elapsedTime = (System.currentTimeMillis() - startWallTime) / 1000.0
      if (wallClockTimeout.exists(elapsedTime > _)) {
        logger.info("Execution timed out after " + elapsedTime + " seconds, ending simulation now.")
        true
      } else {
        false
      }
    }

    def run_(time: Double): Double = {
      if (_agenda.isEmpty || runTime.exists(_ <= time) || timedOut) {
        logger.trace("run_() called. agenda.isEmpty = " + _agenda.isEmpty + " , runTime.exists(_ > time) = " + runTime.exists(_ > time) + " , timedOut = " + timedOut)
        time
      } else
        run_(next())
    }

    val totalTime = run_(currentTime)
    logger.info("*** Simulation finished running, time = " + totalTime + ". ***")
    (!timedOut, totalTime, _totalEventsProcessed)
  }

  def afterDelay(delay: Double, eventType: EventType.Value = null, itemId: Long = 0)(block: => Unit) {
    logger.trace("afterDelay() called. delay is " + delay)
    val item = WorkItem(currentTime + delay, () => block, eventType, itemId)
    _agenda = (_agenda :+ item).sorted
    logger.trace("Inserted new WorkItem into agenda to run at time " + (currentTime + delay))
  }

  def currentTime: Double = curtime

  private def next(): Double = {
    eventId += 1
    _totalEventsProcessed += 1
    val item = deqeue()
    curtime = item.time
    if (curtime < 0)
      curtime = 0
    logger.trace("Processing event number " + eventId + " (" + item + "). Total events left are: " + agenda.length)
    item.action()
    curtime
  }

  protected def agenda: Vector[WorkItem] = _agenda

  def deqeue(): WorkItem = {
    val item = _agenda.head
    _agenda = _agenda.filter(_ != item)
    item
  }

  case class WorkItem(time: Double, action: Action, eventType: EventType.Value, itemId: Long)

}

abstract class ClusterSimulatorDesc(val runTime: Double) {
  def newSimulator(constantThinkTime: Double,
                   perTaskThinkTime: Double,
                   blackListPercent: Double,
                   schedulerWorkloadsToSweepOver: Map[String, Seq[String]],
                   workloadToSchedulerMap: Map[String, Seq[String]],
                   cellStateDesc: CellStateDesc,
                   workloads: Seq[Workload],
                   prefillWorkloads: Seq[Workload],
                   logging: Boolean = false): ClusterSimulator
}

/**
  * A simulator to compare different cluster scheduling architectures
  * (single agent, dynamically partitioned,and replicated state), based
  * on a set of input parameters that define the schedulers being used
  * and the workload being played.
  *
  * @param schedulers             A Map from schedulerName to Scheduler, should
  *                               exactly one entry for each scheduler that is registered with
  *                               this simulator.
  * @param workloadToSchedulerMap A map from workloadName to Seq[SchedulerName].
  *                               Used to determine which scheduler each job is assigned.
  */
class ClusterSimulator(val cellState: CellState,
                       val schedulers: Map[String, Scheduler],
                       val workloadToSchedulerMap: Map[String, Seq[String]],
                       val workloads: Seq[Workload],
                       prefillWorkloads: Seq[Workload],
                       logging: Boolean = false,
                       monitorQueues: Boolean = true,
                       monitorAllocation: Boolean = true,
                       monitorUtilization: Boolean = true,
                       monitoringPeriod: Double = 1.0,
                       var prefillScheduler: PrefillScheduler = null)
  extends Simulator(logging) {
  assert(schedulers.nonEmpty, {
    "At least one scheduler must be provided to scheduler constructor."
  })
  assert(workloadToSchedulerMap.nonEmpty, {
    "No workload->scheduler map setup."
  })

  workloadToSchedulerMap.values.flatten.foreach(schedulerName => assert(schedulers.contains(schedulerName), {
    "Workload-Scheduler map points to a scheduler, " + schedulerName + ", that is not registered"
  })
  )

  // Set up a pointer to this simulator in the cellstate.
  cellState.simulator = this
  // Set up a pointer to this simulator in each scheduler.
  schedulers.values.foreach(_.simulator = this)


  if (prefillScheduler == null)
    prefillScheduler = new PrefillScheduler(cellState)
  prefillScheduler.simulator = this
  prefillScheduler.scheduleWorkloads(prefillWorkloads)

  // Set up workloads
  workloads.foreach(workload => {
    var jobsToRemove: Array[Job] = Array[Job]()
    var numSkipped, numLoaded = 0
    workload.getJobs.foreach(job => {
      val scheduler = getSchedulerForWorkloadName(job.workloadName)
      if (scheduler.isEmpty) {
        logger.warn("Skipping a job fom a workload type (" + job.workloadName + ") that has not been mapped to any registered schedulers. Please update a mapping for this scheduler via the workloadSchedulerMap param.")
        jobsToRemove = jobsToRemove :+ job
        numSkipped += 1
      } else {
        // Schedule the task to get submitted to its scheduler at its
        // submission time.

        // assert(job.cpusPerTask * job.numTasks <= cellState.totalCpus + 0.000001 &&
        //        job.memPerTask * job.numTasks <= cellState.totalMem + 0.000001,
        //        ("The cell (%f cpus, %f mem) is not big enough to hold job %d " +
        //        "all at once which requires %f cpus and %f mem in total.")
        //        .format(cellState.totalCpus,
        //                cellState.totalMem,
        //                job.id,
        //                job.cpusPerTask * job.numTasks,
        //                job.memPerTask * job.numTasks))
        //        if (job.cpusPerTask * job.numTasks > cellState.totalCpus ||
        //            job.memPerTask * job.numTasks > cellState.totalMem){
        val taskCanFitPerCpus = Math.floor(cellState.cpusPerMachine.toDouble / job.cpusPerTask.toDouble) * cellState.numMachines
        val taskCanFitPerMem = Math.floor(cellState.memPerMachine.toDouble / job.memPerTask) * cellState.numMachines
        if (taskCanFitPerCpus < job.numTasks || taskCanFitPerMem < job.numTasks) {
          //        if(taskCanFitPerCpus < job.moldableTasks || taskCanFitPerMem < job.moldableTasks) {
          logger.warn("The cell (" + cellState.totalCpus + " cpus, " + cellState.totalMem + " mem) is not big enough " +
            "to hold job id " + job.id + " all at once which requires " + job.numTasks + " tasks for " + job.cpusPerTask * job.numTasks + " cpus " +
            "and " + job.memPerTask * job.numTasks + " mem in total.")
          numSkipped += 1
          jobsToRemove = jobsToRemove :+ job
        } else {
          afterDelay(job.submitted - currentTime) {
            scheduler.foreach(_.addJob(job))
          }
          numLoaded += 1
        }

      }
    })
    if (numSkipped > 0) {
      logger.warn("Loaded " + numLoaded + " jobs from workload " + workload.name + ", and skipped " + numSkipped + ".")
      workload.removeJobs(jobsToRemove)
    }

  })

  val pendingQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Long]] = scala.collection.mutable.Map[String, ListBuffer[Long]]()
  val runningQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Long]] = scala.collection.mutable.Map[String, ListBuffer[Long]]()

  schedulers.values.foreach(scheduler => {
    pendingQueueStatus.put(scheduler.name, ListBuffer[Long]())
    runningQueueStatus.put(scheduler.name, ListBuffer[Long]())
  })
  val cpuAllocation: ListBuffer[Double] = ListBuffer()
  val memAllocation: ListBuffer[Double] = ListBuffer()
  val cpuUtilization: ListBuffer[Double] = ListBuffer()
  val memUtilization: ListBuffer[Double] = ListBuffer()
  var roundRobinCounter = 0
  var sumCpuAllocation: Double = 0.0
  var sumMemAllocation: Double = 0.0
  var sumCpuLocked: Double = 0.0
  var sumMemLocked: Double = 0.0
  var numAllocationMonitoringMeasurements: Long = 0
  var sumCpuUtilization: Double = 0.0
  var sumMemUtilization: Double = 0.0
  var numUtilizationMonitoringMeasurements: Long = 0

  def measureQueues(): Unit = {
    schedulers.values.foreach(scheduler => {
      pendingQueueStatus(scheduler.name) += scheduler.jobQueueSize
      runningQueueStatus(scheduler.name) += scheduler.runningJobQueueSize
    })
  }

  // If more than one scheduler is assigned a workload, round robin across them.
  def getSchedulerForWorkloadName(workloadName: String): Option[Scheduler] = {
    workloadToSchedulerMap.get(workloadName).map(schedulerNames => {
      // println("schedulerNames is %s".format(schedulerNames.mkString(" ")))
      roundRobinCounter += 1
      val name = schedulerNames(roundRobinCounter % schedulerNames.length)
      // println("Assigning job from workload %s to scheduler %s"
      //         .format(workloadName, name))
      schedulers(name)
    })
  }

  // Track Allocation due to resources actually being accepted by a
  // framework/scheduler. This does not include the time resources spend
  // tied up while they are pessimistically locked (e.g. while they are
  // offered as part of a Mesos resource-offer). That type of Allocation
  // is tracked separately below.
  def avgCpuAllocation: Double = sumCpuAllocation / numAllocationMonitoringMeasurements.toDouble

  def avgMemAllocation: Double = sumMemAllocation / numAllocationMonitoringMeasurements.toDouble

  // Track "Allocation" of resources due to their being pessimistically locked
  // (i.e. while they are offered as part of a Mesos resource-offer).
  def avgCpuLocked: Double = sumCpuLocked / numAllocationMonitoringMeasurements.toDouble

  def avgMemLocked: Double = sumMemLocked / numAllocationMonitoringMeasurements.toDouble

  def measureAllocation(): Unit = {
    numAllocationMonitoringMeasurements += 1

    val _cpuAllocation: Double = cellState.totalOccupiedCpus / cellState.totalCpus.toDouble
    cpuAllocation += _cpuAllocation
    sumCpuAllocation += _cpuAllocation
    val _memoryAllocation: Double = cellState.totalOccupiedMem / cellState.totalMem.toDouble
    memAllocation += _memoryAllocation
    sumMemAllocation += _memoryAllocation

    sumCpuLocked += cellState.totalLockedCpus.toDouble
    sumMemLocked += cellState.totalLockedMem.toDouble

    logger.debug("Adding Allocation measurement " + numAllocationMonitoringMeasurements + ". Avg cpu: " + avgCpuAllocation + ". Avg mem: " + avgMemAllocation)
  }

  def avgCpuUtilization: Double = sumCpuUtilization / numUtilizationMonitoringMeasurements.toDouble

  def avgMemUtilization: Double = sumMemUtilization / numUtilizationMonitoringMeasurements.toDouble

  def measureUtilization(): Unit = {
    numUtilizationMonitoringMeasurements += 1

    val _cpuUtilization: Double = cellState.totalCpuUtilization() / cellState.totalCpus.toDouble
    cpuUtilization += _cpuUtilization
    sumCpuUtilization += _cpuUtilization

    val _memUtilization: Double = cellState.totalMemoryUtilization() / cellState.totalMem.toDouble
    memUtilization += _memUtilization
    sumMemUtilization += _memUtilization

    logger.debug("Adding Utilization measurement " + numUtilizationMonitoringMeasurements + ". Avg cpu: " + avgCpuUtilization + ". Avg mem: " + avgMemUtilization)
  }

  def monitoring(): Unit = {
    if (monitorAllocation)
      measureAllocation()
    if (monitorQueues)
      measureQueues()
    if (monitorUtilization)
      measureUtilization()


    // Only schedule a monitoring event if the simulator has
    // more (non-monitoring) events to play. Else this will cause
    // the simulator to run forever just to keep monitoring.
    if (agenda.nonEmpty && (monitorAllocation | monitorQueues | monitorUtilization)) {
      afterDelay(monitoringPeriod) {
        monitoring()
      }
    }
  }

  override
  def run(runTime: Option[Double] = None,
          wallClockTimeout: Option[Double] = None): (Boolean, Double, Long) = {
    assert(currentTime == 0.0, {
      "currentTime must be 0 at simulator run time."
    })
    schedulers.values.foreach(scheduler => {
      assert(scheduler.jobQueueSize == 0, {
        "Schedulers are not allowed to have jobs in their queues when we run the simulator."
      })
    })
    // Start the Allocation monitoring loop.
    afterDelay(-1) {
      monitoring()
    }
    super.run(runTime, wallClockTimeout)
  }
}
