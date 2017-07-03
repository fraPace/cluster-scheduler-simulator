package ClusterSchedulingSimulation.core

import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AllocationMode extends Enumeration {
  type AllocationMode = Value
  val Incremental, All, AllCore = Value

  def exists(allocationMode: AllocationMode): Boolean = values.exists(_.equals(allocationMode))
  def valuesToString(): String = {values.toString()}
}

class CellStateDesc(val numMachines: Int,
                    val cpusPerMachine: Long,
                    val memPerMachine: Long)

class CellState(val numMachines: Int,
                val cpusPerMachine: Long,
                val memPerMachine: Long,
                val conflictMode: String = "None",
                val transactionMode: String) extends LazyLogging {
  assert(conflictMode.equals("resource-fit") || conflictMode.equals("sequence-numbers") || conflictMode.equals("None"), {
    "conflictMode must be one of: {'resource-fit', 'sequence-numbers'}, but it was " + conflictMode
  })
  assert(transactionMode.equals("all-or-nothing") || transactionMode.equals("incremental"), {
    "transactionMode must be one of: {'all-or-nothing', 'incremental'}, but it was " + transactionMode
  })

  // Data Structure used to consistency check in the code flow
  val claimDeltas: mutable.HashSet[ClaimDelta] = mutable.HashSet[ClaimDelta]()

  // An array where value at position k is the total CPUs that have been
  // allocated for machine k.
  val allocatedCpusPerMachine: Array[Long] = Array.fill(numMachines)(0L)
  val allocatedMemPerMachine: Array[Long] = Array.fill(numMachines)(0L)
  val machineSeqNums = new Array[Long](numMachines)
  // Map from scheduler name to number of CPUs assigned to that scheduler.
  val occupiedCpus: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
  val occupiedMem: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
  // Map from scheduler name to number of CPUs locked while that scheduler
  // makes scheduling decisions about them, i.e., while resource offers made
  // to that scheduler containing those amount of resources is pending.
  val lockedCpus: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
  val lockedMem: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()
  var simulator: ClusterSimulator = _
  // These used to be functions that sum the values of occupiedCpus,
  // occupiedMem, lockedCpus, and lockedMem, but its much faster to use
  // scalars that we update in assignResources and freeResources.
  var totalOccupiedCpus:Long = 0L
  var totalOccupiedMem:Long = 0
  var totalLockedCpus:Long = 0L
  var totalLockedMem:Long = 0

  val totalCpus: Long = numMachines * cpusPerMachine
  def availableCpus: Long = totalCpus - (totalOccupiedCpus + totalLockedCpus)

  val totalMem: Long = numMachines * memPerMachine
  def availableMem: Long = totalMem - (totalOccupiedMem + totalLockedMem)

  val claimDeltasPerMachine: Array[Vector[ClaimDelta]] = Array.fill(numMachines)(Vector[ClaimDelta]())

  def cpuAvgUtilization(): Float = {
    var _sum: Long = 0
    var i: Int = 0
    while (i < numMachines){
      var j: Int = 0
      while(j < claimDeltasPerMachine(i).length){
        val claimDelta: ClaimDelta = claimDeltasPerMachine(i)(j)
        claimDelta.job match {
          case Some(param) =>
            _sum += param.cpuUtilization(simulator.currentTime)
          case None => None
        }
        j += 1
      }
      i += 1
    }
    _sum / totalCpus.toFloat
  }

  def memoryAvgUtilization(): Float= {
    var _sum: Long = 0
    var i: Int = 0
    while (i < numMachines){
      var j: Int = 0
      while(j < claimDeltasPerMachine(i).length){
        val claimDelta: ClaimDelta = claimDeltasPerMachine(i)(j)
        claimDelta.job match {
          case Some(param) =>
            _sum += param.memoryUtilization(simulator.currentTime)
          case None => None
        }
        j += 1
      }
      i += 1
    }
    _sum / totalMem.toFloat
  }

//  def isFull: Boolean = availableCpus.floor <= 0 || availableMem.floor <= 0

  /**
    * Allocate resources on a machine to a scheduler.
    *
    * @param locked Mark these resources as being pessimistically locked
    *               (i.e. while they are offered as part of a Mesos
    *               resource-offer).
    */
  def assignResources(scheduler: Scheduler,
                      machineID: Int,
                      cpus: Long,
                      mem: Long,
                      locked: Boolean): Unit = {
    // Track the per machine resources available.
    assert(availableCpusPerMachine(machineID) >= cpus, {
      "Scheduler " + scheduler.name + "  tried to claim " + cpus + " CPUs on machine " + machineID + " in cell state " +
        hashCode() + ", but it only has " + availableCpusPerMachine(machineID) + " unallocated CPUs right now."
    })
    assert(availableMemPerMachine(machineID) >= mem, {
      "Scheduler " + scheduler.name + " tried to claim " + mem + " mem on machine " + machineID + " in cell state " +
        hashCode() + ", but it only has " + availableMemPerMachine(machineID) + " mem unallocated right now."
    })
    allocatedCpusPerMachine(machineID) += cpus
    allocatedMemPerMachine(machineID) += mem

    // Track the resources used by this scheduler.
    if (locked) {
      lockedCpus(scheduler.name) = lockedCpus.getOrElse(scheduler.name, 0L) + cpus
      lockedMem(scheduler.name) = lockedMem.getOrElse(scheduler.name, 0L) + mem
      assert(lockedCpus(scheduler.name) <= totalCpus)
      assert(lockedMem(scheduler.name) <= totalMem)
      totalLockedCpus += cpus
      totalLockedMem += mem
    } else {
      occupiedCpus(scheduler.name) = occupiedCpus.getOrElse(scheduler.name, 0L) + cpus
      occupiedMem(scheduler.name) = occupiedMem.getOrElse(scheduler.name, 0L) + mem
      assert(occupiedCpus(scheduler.name) <= totalCpus)
      assert(occupiedMem(scheduler.name) <= totalMem)
      totalOccupiedCpus += cpus
      totalOccupiedMem += mem
    }
  }

  // Convenience methods to see how many CPUs/mem are available on a machine.
  def availableCpusPerMachine(machineID: Int): Long = {
    assert(machineID <= allocatedCpusPerMachine.length - 1, {
      "There is no machine with ID " + machineID
    })
    val cpus = allocatedCpusPerMachine(machineID)
    assert(cpus <= cpusPerMachine, {
      "Allocated more CPUs (" + cpus + ") then possible (" + cpusPerMachine + ")"
    })
    cpusPerMachine - cpus
  }

  def availableMemPerMachine(machineID: Int): Long = {
    assert(machineID <= allocatedMemPerMachine.length - 1, {
      "There is no machine with ID " + machineID
    })
    val mem = allocatedMemPerMachine(machineID)
    assert(mem <= memPerMachine, {
      "Allocated more Memory (" + mem + ") then possible (" + memPerMachine + ")"
    })
    memPerMachine - mem
  }

  // Release the specified number of resources used by this scheduler.
  def freeResources(scheduler: Scheduler,
                    machineID: Int,
                    cpus: Long,
                    mem: Long,
                    locked: Boolean): Unit = {
    // Track the per machine resources available.
    assert(availableCpusPerMachine(machineID) + cpus <= cpusPerMachine, {
      "CPUs are " + (availableCpusPerMachine(machineID) + cpus) + " but they should be maximum " + cpusPerMachine
    })
    assert(availableMemPerMachine(machineID) + mem <= memPerMachine, {
      "Memory is " + (availableMemPerMachine(machineID) + mem) + " but they should be maximum " + memPerMachine
    })
    allocatedCpusPerMachine(machineID) -= cpus
    allocatedMemPerMachine(machineID) -= mem

    // Track the resources used by this scheduler.
    if (locked) {
      assert(lockedCpus.contains(scheduler.name))
      assert(lockedCpus(scheduler.name) >= cpus, {
        scheduler.name + " tried to free " + cpus + " CPUs, but was only occupying " + lockedCpus(scheduler.name)
      })
      assert(lockedMem.contains(scheduler.name))
      assert(lockedMem(scheduler.name) >= mem, {
        scheduler.name + " tried to free " + mem + " mem, but was only occupying " + lockedMem(scheduler.name)
      })
      lockedCpus(scheduler.name) = lockedCpus(scheduler.name) - cpus
      lockedMem(scheduler.name) = lockedMem(scheduler.name) - mem
      totalLockedCpus -= cpus
      totalLockedMem -= mem
    } else {
      assert(occupiedCpus.contains(scheduler.name))
      assert(occupiedCpus(scheduler.name) >= cpus, {
        scheduler.name + " tried to free " + cpus + " CPUs, but was only occupying " + occupiedCpus(scheduler.name)
      })
      assert(occupiedMem.contains(scheduler.name))
      assert(occupiedMem(scheduler.name) >= mem, {
        scheduler.name + " tried to free " + mem + " mem, but was only occupying " + occupiedMem(scheduler.name)
      })
      occupiedCpus(scheduler.name) = occupiedCpus(scheduler.name) - cpus
      occupiedMem(scheduler.name) = occupiedMem(scheduler.name) - mem
      totalOccupiedCpus -= cpus
      totalOccupiedMem -= mem
    }
  }

  /**
    * Return a copy of this cell state in its current state.
    */
  def copy: CellState = {
    val newCellState = new CellState(numMachines,
      cpusPerMachine,
      memPerMachine,
      conflictMode,
      transactionMode)
    Array.copy(src = allocatedCpusPerMachine,
      srcPos = 0,
      dest = newCellState.allocatedCpusPerMachine,
      destPos = 0,
      length = numMachines)
    Array.copy(src = allocatedMemPerMachine,
      srcPos = 0,
      dest = newCellState.allocatedMemPerMachine,
      destPos = 0,
      length = numMachines)
    Array.copy(src = machineSeqNums,
      srcPos = 0,
      dest = newCellState.machineSeqNums,
      destPos = 0,
      length = numMachines)
    newCellState.occupiedCpus ++= occupiedCpus
    newCellState.occupiedMem ++= occupiedMem
    newCellState.lockedCpus ++= lockedCpus
    newCellState.lockedMem ++= lockedMem
    newCellState.totalOccupiedCpus = totalOccupiedCpus
    newCellState.totalOccupiedMem = totalOccupiedMem
    newCellState.totalLockedCpus = totalLockedCpus
    newCellState.totalLockedMem = totalLockedMem

    newCellState.claimDeltas ++= claimDeltas

    newCellState.simulator  = simulator
    newCellState
  }

  /**
    * Attempt to play the list of deltas, return any that conflicted.
    */
  def commit(deltas: Seq[ClaimDelta],
             scheduleEndEvent: Boolean = false): CommitResult = {
    var rollback = false // Track if we need to rollback changes.
    var appliedDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    var conflictDeltas = collection.mutable.ListBuffer[ClaimDelta]()
    var conflictMachines = collection.mutable.ListBuffer[Int]()

    def commitNonConflictingDeltas(): Unit = {
      deltas.foreach(d => {
        // We check two things here
        // 1) if the deltas can cause conflict
        // 2) if the machine that is contained in the delta had previous conflict
        // This is to improve performances and to avoid a bug when using "incremental" and "sequence-number" that
        // affect the sequence number of a machine.
        // The bug appears when job A assign 2 tasks on the same machine and thus will have a difference in the
        // seq-number of 1. But at the same time job B may have already incremented the seq-number to the same value by
        // placing one task with a lot of resources requirements. Therefore when the seq-number of job A task 2 and job
        // B task 1 have the same values, but the resources left on the machine are different from what they think
        if (causesConflict(d) || conflictMachines.contains(d.machineID)) {
          simulator.logger.info("delta (" + d.scheduler + " mach-" + d.machineID + " seqNum-" + d.machineSeqNum + ") caused a conflict.")
          conflictDeltas += d
          conflictMachines += d.machineID
          if (transactionMode == "all-or-nothing") {
            rollback = true
            return
          } else if (transactionMode == "incremental") {

          } else {
            simulator.logger.error("Invalid transactionMode.")
          }
        } else {
          d.apply(cellState = this)
          appliedDeltas += d
        }
      })
    }

    commitNonConflictingDeltas()
    // Rollback if necessary.
    if (rollback) {
      simulator.logger.info("Rolling back " + appliedDeltas.length + " deltas.")
      appliedDeltas.foreach(d => {
        d.unApply(cellState = this)
        conflictDeltas += d
        appliedDeltas -= d
      })
    }

    if (scheduleEndEvent) {
      scheduleEndEvents(appliedDeltas)
    }
    CommitResult(appliedDeltas, conflictDeltas)
  }

  /**
    * Create an end event for each delta provided. The end event will
    * free the resources used by the task represented by the ClaimDelta.
    * We also calculate the highest delay after which the resources are
    * free and we return that value
    *
    * @param claimDeltas The resources that were busy
    * @param delay       Set the delay at which the end events should occur.
    *                    if value is -1 use the duration of the task
    * @return The higher delay after which the resources are free
    */
  def scheduleEndEvents(claimDeltas: Seq[ClaimDelta], delay: Double = -1): Unit = {
    assert(simulator != null, {
      "Simulator must be non-null in CellState."
    })
    var maxDelay = 0.0
    val jobs: ListBuffer[Job] = ListBuffer[Job]()
    claimDeltas.foreach(appliedDelta => {
      val job = appliedDelta.job.get
      lazy val jobPrefix = "[Job " + job.id + " (" + job.workloadName + ")]"
      if (!jobs.contains(job))
        jobs += job
      val realDelay = if (delay == -1) appliedDelta.duration else delay
      if (realDelay > maxDelay) maxDelay = realDelay
      simulator.afterDelay(realDelay, eventType = EventType.Remove, itemId = job.id) {
        simulator.logger.info(appliedDelta.scheduler.loggerPrefix + jobPrefix + " A task finished after " + (simulator.currentTime - appliedDelta.creationTime) + "s. Freeing " +
          appliedDelta.currentCpus + " CPUs, " + appliedDelta.currentMem + " mem. Available: " + availableCpus + " CPUs, " + availableMem + " mem.")
        appliedDelta.unApply(simulator.cellState)
      }
    })
    jobs.foreach(job => {
      simulator.schedulers.foreach { case (_, scheduler) =>
        simulator.afterDelay(maxDelay, eventType = EventType.Trigger, itemId = job.id) {
          scheduler.wakeUp()
        }
      }
    })

  }

  /**
    * Tests if this delta causes a transaction conflict.
    * Different test scheme is used depending on conflictMode.
    */
  def causesConflict(delta: ClaimDelta): Boolean = {
    conflictMode match {
      case "sequence-numbers" =>
        simulator.logger.debug("Checking if seqNum for machine " + delta.machineID + " are different. (" + delta.machineSeqNum + " - " + machineSeqNums(delta.machineID) + ")")
        // Use machine sequence numbers to test for conflicts.
        if (delta.machineSeqNum != machineSeqNums(delta.machineID)) {
          simulator.logger.info("Sequence-number conflict occurred (sched-" + delta.scheduler + ", mach-" + delta.machineID + ", seq-num-" + delta.machineSeqNum + ", seq-num-" + machineSeqNums(delta.machineID) + ").")
          true
        } else {
          false
        }
      case "resource-fit" =>
        // Check if the machine is currently short of resources,
        // regardless of whether sequence nums have changed.
        if (availableCpusPerMachine(delta.machineID) < delta.currentCpus ||
          availableMemPerMachine(delta.machineID) < delta.currentMem) {
          simulator.logger.info("Resource-aware conflict occurred (sched-" + delta.scheduler + ", mach-" + delta.machineID + ", CPUs-" + delta.currentCpus + ", mem-" + delta.currentMem + ").")
          true
        } else {
          false
        }
      case "None" => false
      case _ =>
        simulator.logger.error("Unrecognized conflictMode " + conflictMode)
        true // Should never be reached.
    }
  }

  case class CommitResult(committedDeltas: Seq[ClaimDelta],
                          conflictedDeltas: Seq[ClaimDelta])

}
