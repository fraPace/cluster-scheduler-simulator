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

package ClusterSchedulingSimulation

import java.io.File

import ClusterSchedulingSimulation.core.{CellStateDesc, WorkloadDesc}
import ClusterSchedulingSimulation.utils.Constant
import ClusterSchedulingSimulation.workloads.{FakeZoeDynamicWorkloadGenerator, FakeZoeWorkloadGenerator, TraceAllZoeWLGenerator}

/**
  * Set up workloads based on measurements from a real cluster.
  * In the Eurosys paper, we used measurements from Google clusters here.
  */
object Workloads {

  val tracesFolder: String = "traces/"
  val jobDistributionTracesFolder: String = tracesFolder + "job-distribution-traces/"

  val scaleFactor = 1
  val introduceError = false

  val workloadSize = 500

  val globalNumMachines: Int = 5 * scaleFactor
  val globalCpusPerMachine: Long = 32 * 1000L // value must be in millicores
  val globalMemPerMachine: Long = 128 * Constant.GiB //value must be in bytes

  val globalMaxCoresPerJob = 20.0 // only used in NewSpark
  //  val globalMaxCpusPerTask = 2
  //  val globalMaxMemPerTask = 8

  val globalMaxCpusPerTask: Long = globalCpusPerMachine / 15
  val globalMaxMemPerTask: Long = globalMemPerMachine / 5

  //  val maxTasksPerJob = ((globalNumMachines * globalCpusPerMachine * 1.5) / globalMaxCpusPerTask).toInt

  /**
    * Set up CellStateDescs that will go into WorkloadDescs. Fabricated
    * numbers are provided as an example. Enter numbers based on your
    * own clusters instead.
    */
  val eurecomCellStateDesc = new CellStateDesc(globalNumMachines,
    globalCpusPerMachine,
    globalMemPerMachine)

  //  val tenEurecomCellStateDesc = new CellStateDesc(globalNumMachines * 10,
  //    globalCpusPerMachine,
  //    globalMemPerMachine)
  //
  //  val fiveEurecomCellStateDesc = new CellStateDesc(globalNumMachines * 5,
  //    globalCpusPerMachine,
  //    globalMemPerMachine)

  /**
    * Prefill the cell
    */
  val prefillTraceFileName: String = tracesFolder + "init-cluster-state.log"
  assert(new File(prefillTraceFileName).exists(), {
    "File " + prefillTraceFileName + " does not exist."
  })

  //  val batchServicePrefillTraceWLGenerator =
  //    new PrefillPbbTraceWorkloadGenerator("PrefillBatchService", prefillTraceFileName)


  // Set up example workload with jobs that have interarrival times
  // from trace-based interarrival times.
  val interarrivalTraceFileName: String = jobDistributionTracesFolder + "interarrival_cmb.log"
  val numTasksTraceFileName: String = jobDistributionTracesFolder + "csizes_cmb.log"
  val jobDurationTraceFileName: String = jobDistributionTracesFolder + "runtimes_cmb.log"
  assert(new File(interarrivalTraceFileName).exists(), {
    "File " + interarrivalTraceFileName + " does not exist."
  })
  assert(new File(numTasksTraceFileName).exists(), {
    "File " + numTasksTraceFileName + " does not exist."
  })
  assert(new File(jobDurationTraceFileName).exists(), {
    "File " + jobDurationTraceFileName + " does not exist."
  })

  val cpuSlackTraceFileName: String = jobDistributionTracesFolder + "cpu_slack.log"
  val memorySlackTraceFileName: String = jobDistributionTracesFolder + "memory_slack.log"
  assert(new File(cpuSlackTraceFileName).exists(), {
    "File " + cpuSlackTraceFileName + " does not exist."
  })
  assert(new File(memorySlackTraceFileName).exists(), {
    "File " + memorySlackTraceFileName + " does not exist."
  })

  // A workload based on traces of interarrival times, tasks-per-job,
  // and job duration. Task shapes now based on pre-fill traces.
  val workloadGeneratorTraceAllBatch =
    new TraceAllZoeWLGenerator("Batch".intern(),
      interarrivalTraceFileName, numTasksTraceFileName, jobDurationTraceFileName,
      prefillTraceFileName, cpuSlackTraceFileName, memorySlackTraceFileName,
      maxCpusPerTask = globalMaxCpusPerTask, maxMemPerTask = globalMaxMemPerTask,
      jobsPerWorkload = Math.round(workloadSize * 0.8).toInt, scaleFactor = scaleFactor,
      allCore = false, introduceError = introduceError)

  val workloadGeneratorTraceAllService =
    new TraceAllZoeWLGenerator("Batch-MPI".intern(),
      interarrivalTraceFileName, numTasksTraceFileName, jobDurationTraceFileName,
      prefillTraceFileName,  cpuSlackTraceFileName, memorySlackTraceFileName,
      maxCpusPerTask = globalMaxCpusPerTask, maxMemPerTask = globalMaxMemPerTask,
      jobsPerWorkload = Math.round(workloadSize * 0.2).toInt, scaleFactor = scaleFactor,
      allCore = true, introduceError = introduceError)
  //
  //  val workloadGeneratorTraceAllInteractive =
  //    new TraceAllZoeWLGenerator(
  //      "Interactive".intern(),
  //      interarrivalTraceFileName,
  //      numTasksTraceFileName,
  //      jobDurationTraceFileName,
  //      prefillTraceFileName,
  //      cpuSlackTraceFileName,
  //      memorySlackTraceFileName,
  //      maxCpusPerTask = globalMaxCpusPerTask,
  //      maxMemPerTask = globalMaxMemPerTask,
  //      jobsPerWorkload = (workloadSize * 0.2).toInt,
  //      scaleFactor = scaleFactor,
  //      allMoldable = false,
  //      introduceError = introduceError)

  //  val workloadGeneratorTraceAllBatch =
  //    new UniformZoeWorkloadGenerator(
  //      "Batch".intern(),
  //      initJobInterarrivalTime = 1,
  //      tasksPerJob = 50,
  //      jobDuration = 200,
  //      cpusPerTask = 2,
  //      memPerTask = 4,
  //      numMoldableTasks = 4,
  //      jobsPerWorkload = 0)
  //
  //  val workloadGeneratorTraceAllService =
  //    new UniformZoeWorkloadGenerator(
  //      "Service".intern(),
  //      initJobInterarrivalTime = 1,
  //      tasksPerJob = 10,
  //      jobDuration = 2000,
  //      cpusPerTask = 2,
  //      memPerTask = 4,
  //      jobsPerWorkload = 4)

//  val fakeWorkloadGenerator = new FakeZoeWorkloadGenerator("Batch")
  //  val fakeWorkloadGenerator = new FakePreemptiveZoeWorkloadGenerator("Batch")
//  val fakeWorkloadGenerator = new FakeZoeDynamicWorkloadGenerator("Batch")
//
//  val eurecomCellTraceAllWorkloadPrefillDesc =
//    WorkloadDesc(
//      cell = "Eurecom",
//      assignmentPolicy = "CMB_PBB",
//      workloadGenerators =
//        fakeWorkloadGenerator ::
//          Nil,
//      cellStateDesc = new CellStateDesc(1,
//        globalCpusPerMachine,
//        128 * Constant.GiB)
//    )

  val eurecomCellTraceAllWorkloadPrefillDesc =
    WorkloadDesc(
      cell = "Eurecom",
      assignmentPolicy = "CMB_PBB",
      workloadGenerators =
        workloadGeneratorTraceAllBatch ::
          workloadGeneratorTraceAllService ::
          //          workloadGeneratorTraceAllInteractive ::
          Nil,
      cellStateDesc = eurecomCellStateDesc
    )

  //  val eurecomCellTraceAllWorkloadPrefillDesc =
  //    WorkloadDesc(
  //      cell = "Eurecom",
  //      assignmentPolicy = "CMB_PBB",
  //      workloadGenerators =
  //        workloadGeneratorTraceAllBatch ::
  //          workloadGeneratorTraceAllService ::
  //          Nil,
  //      cellStateDesc = eurecomCellStateDesc,
  //        prefillWorkloadGenerators =
  //          List(batchServicePrefillTraceWLGenerator)
  //    )

  //  val tenEurecomCellTraceAllWorkloadPrefillDesc =
  //    WorkloadDesc(
  //      cell = "10xEurecom",
  //      assignmentPolicy = "CMB_PBB",
  //      workloadGenerators =
  //        workloadGeneratorTraceAllBatch ::
  //          workloadGeneratorTraceAllService ::
  //          Nil,
  //      cellStateDesc = tenEurecomCellStateDesc,
  //      prefillWorkloadGenerators =
  //        List(batchServicePrefillTraceWLGenerator))
  //
  //  val fiveEurecomCellTraceAllWorkloadPrefillDesc =
  //    WorkloadDesc(
  //      cell = "5xEurecom",
  //      assignmentPolicy = "CMB_PBB",
  //      workloadGenerators =
  //        workloadGeneratorTraceAllBatch ::
  //          workloadGeneratorTraceAllService ::
  //          Nil,
  //      cellStateDesc = fiveEurecomCellStateDesc,
  //      prefillWorkloadGenerators =
  //        List(batchServicePrefillTraceWLGenerator))
}
