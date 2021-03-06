package ClusterSchedulingSimulation.core

import ch.qos.logback.classic.Level
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


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

  protected var curtime: Double = 0.0 // simulation time, in seconds
//  private[this] var _agenda = Vector[WorkItem]()
  private[this] var _agenda = mutable.PriorityQueue.empty[WorkItem]
  private[this] var _totalEventsProcessed: Long = 0


  def removeEventsIf(p: WorkItem => Boolean): Unit = {
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

//    afterDelay(0) {
//      logger.info("*** Simulation started, time = " + currentTime + ". ***")
//    }
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
//    _agenda = (_agenda :+ item).sorted
    _agenda += item
    logger.trace("Inserted new WorkItem into agenda to run at time " + (currentTime + delay))
  }

  def currentTime: Double = curtime

  private def next(): Double = {
    eventId += 1
    _totalEventsProcessed += 1
//    val item = dequeue()
    val item = _agenda.dequeue()
    assert(item.time >= curtime, {
      "The time (" + item.time + ") for the next event is lower than the current time (" + curtime + ")"
    })
    curtime = if(item.time < 0) 0 else item.time
    logger.trace("Processing event number " + eventId + " (" + item + "). Total events left are: " + agenda.length)
    item.action()
    curtime
  }

//  protected def agenda: Vector[WorkItem] = _agenda
  protected def agenda: mutable.PriorityQueue[WorkItem] = _agenda

//  def dequeue(): WorkItem = {
//    val item = _agenda.head
//    _agenda = _agenda.filter(_ != item)
//    item
//  }



  implicit object WorkItem extends Ordering[WorkItem] {
    var _agendaItemCounter: Long = 0

    def apply(time: Double, action: Action, eventType: EventType.Value, eventID: Long): WorkItem = {
      _agendaItemCounter += 1
      new WorkItem(_agendaItemCounter, time, action, eventType, eventID)
    }

    /**
      * This function compare two WorkItem.
      * First it will compare the time, then, if the two times are equal, it compared the id
      * The results will be a PriorityQueue on the WorkItem.time field in Ascending with a FIFO order for the WorkItems
      * with the same time.
      *
      * @param a WorkItem to compare
      * @param b WorkItem to compare with
      * @return Int
      */
    def compare(a: WorkItem, b: WorkItem): Int = {
      var ret = b.time.compare(a.time)
      if(ret == 0)
        ret = b.id.compare(a.id)
      ret
    }
  }

  case class WorkItem(id: Long, time: Double, action: Action, eventType: EventType.Value, eventID: Long)

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
                       val prefillWorkloads: Seq[Workload],
                       val logging: Boolean = false,
                       val monitorQueues: Boolean = true,
                       val monitorAllocation: Boolean = true,
                       val monitorUtilization: Boolean = true,
                       val monitoringPeriod: Double = 1.0,
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

  val pendingQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Int]] = scala.collection.mutable.Map[String, ListBuffer[Int]]()
  val runningQueueStatus: scala.collection.mutable.Map[String, ListBuffer[Int]] = scala.collection.mutable.Map[String, ListBuffer[Int]]()

  schedulers.values.foreach(scheduler => {
    pendingQueueStatus.put(scheduler.name, ListBuffer[Int]())
    runningQueueStatus.put(scheduler.name, ListBuffer[Int]())
  })

  val cpuAllocation: ListBuffer[Float] = ListBuffer()
  val memAllocation: ListBuffer[Float] = ListBuffer()
  val cpuAllocationWasted: ArrayBuffer[Float] = ArrayBuffer()
  val memAllocationWasted: ArrayBuffer[Float] = ArrayBuffer()
  val cpuUtilization: ListBuffer[Float] = ListBuffer()
  val memUtilization: ListBuffer[Float] = ListBuffer()
  val cpuUtilizationWasted: ArrayBuffer[Float] = ArrayBuffer()
  val memUtilizationWasted: ArrayBuffer[Float] = ArrayBuffer()
  var roundRobinCounter = 0
  var sumCpuLocked: Double = 0.0
  var sumMemLocked: Double = 0.0
  var numAllocationMonitoringMeasurements: Long = 0

  def avgPendingQueueSize(scheduler: String): Double = pendingQueueStatus(scheduler).foldLeft(0L)((b, a) => b + a) / pendingQueueStatus(scheduler).size.toDouble
  def avgRunningQueueSize(scheduler: String): Double = runningQueueStatus(scheduler).foldLeft(0L)((b, a) => b + a) / runningQueueStatus(scheduler).size.toDouble

  def measureQueues(): Unit = {
    schedulers.values.foreach(scheduler => {
      pendingQueueStatus(scheduler.name) += scheduler.pendingQueueSize
      runningQueueStatus(scheduler.name) += scheduler.runningQueueSize
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
  def avgCpuAllocation: Double = cpuAllocation.sum / cpuAllocation.size.toDouble

  def avgMemAllocation: Double = memAllocation.sum / memAllocation.size.toDouble

  // Track "Allocation" of resources due to their being pessimistically locked
  // (i.e. while they are offered as part of a Mesos resource-offer).
  def avgCpuLocked: Double = sumCpuLocked / numAllocationMonitoringMeasurements.toDouble

  def avgMemLocked: Double = sumMemLocked / numAllocationMonitoringMeasurements.toDouble

  def measureAllocation(): Unit = {
    cpuAllocation += cellState.totalOccupiedCpus / cellState.totalCpus.toFloat
    memAllocation += cellState.totalOccupiedMem / cellState.totalMem.toFloat

    cpuAllocationWasted += 0
    memAllocationWasted += 0

    numAllocationMonitoringMeasurements += 1
    sumCpuLocked += cellState.totalLockedCpus.toFloat
    sumMemLocked += cellState.totalLockedMem.toFloat
  }


  def avgCpuUtilization: Double = cpuUtilization.sum / cpuUtilization.size.toDouble
  def avgMemUtilization: Double = memUtilization.sum / memUtilization.size.toDouble

  def measureUtilization(): Unit = {
    cpuUtilization += cellState.cpuAvgUtilization()
    memUtilization += cellState.memoryAvgUtilization()
    cpuUtilizationWasted += 0
    memUtilizationWasted += 0
  }

  val extraMonitoringActions: ListBuffer[Action] = ListBuffer[Action]()

  def monitoring(): Unit = {
    if (monitorAllocation)
      measureAllocation()
    if (monitorQueues)
      measureQueues()
    if (monitorUtilization)
      measureUtilization()

    extraMonitoringActions.foreach(_())

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
      assert(scheduler.pendingQueueSize == 0, {
        "Schedulers are not allowed to have jobs in their queues when we run the simulator."
      })
    })
    // Start the Allocation monitoring loop.
    afterDelay(0) {
      monitoring()
    }
    val (timedOut, totalTime, totalEventsProcessed) = super.run(runTime, wallClockTimeout)

    if(cellState.availableCpus != cellState.totalCpus || cellState.availableMem != cellState.totalMem)
      logger.warn("There are still some resources allocated allocated! CPU: " +  cellState.totalOccupiedCpus + " Mem: "
        + cellState.totalOccupiedMem + " ClaimDeltas: " + cellState.claimDeltas.size)

    (timedOut, totalTime, totalEventsProcessed)
  }

  def avgCpuUtilizationWasted: Double = cpuUtilizationWasted.sum / cpuUtilizationWasted.size.toDouble

  def avgMemUtilizationWasted: Double = memUtilizationWasted.sum / memUtilizationWasted.size.toDouble

  def avgCpuAllocationWasted: Double = cpuAllocationWasted.sum / cpuAllocationWasted.size.toDouble

  def avgMemAllocationWasted: Double = memAllocationWasted.sum / memAllocationWasted.size.toDouble

  def recordWastedResources(cl: ClaimDelta): Unit = {
    cl.job match {
      case Some(job) =>
        var time: Double = cl.creationTime
        var index: Int = (time / monitoringPeriod).toInt
        while(time < currentTime){
//          cpuAllocationWasted(index) += ((job.cpusPerTask /  cellState.totalCpus.toDouble) * percentageConversion).toShort
//          assert(cpuAllocationWasted(index) <= 100, {
//            "Cpu Allocation Wasted cannot be higher than 100% (" + cpuAllocationWasted(index) + "%)"
//          })
//          memAllocationWasted(index) += ((job.memPerTask /  cellState.totalMem.toDouble) * percentageConversion).toShort
//          assert(memAllocationWasted(index) <= 100, {
//            "Memory Allocation Wasted cannot be higher than 100% (" + memAllocationWasted(index) + "%)"
//          })

          cpuUtilizationWasted(index) += (job.cpuUtilization(time) /  cellState.totalCpus.toDouble).toFloat
          assert(cpuUtilizationWasted(index) <= 100, {
            "Cpu Utilization Wasted cannot be higher than 100% (" + cpuUtilizationWasted(index) + "%)"
          })
          memUtilizationWasted(index) += (job.memoryUtilization(time) /  cellState.totalMem.toDouble).toFloat
          assert(memUtilizationWasted(index) <= 100, {
            "Memory Utilization Wasted cannot be higher than 100% (" + memUtilizationWasted(index) + "%)"
          })
          index += 1
          time += monitoringPeriod
        }
      case None => None
    }
  }
}
