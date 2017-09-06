/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * Each Stage also has a jobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * The callSite provides a location in user code which relates to the stage. For a shuffle map
 * stage, the callSite gives the user code that created the RDD being shuffled. For a result
 * stage, the callSite gives the user code that executes the associated action (e.g. count()).
 *
 * A single stage can consist of multiple attempts. In that case, the latestInfo field will
 * be updated for each attempt.
 *
 */
// 一个阶段是一组独立的任务，他们都在计算同一个函数，需要运行作为Spark作业的一部分，其中所有的任务都具有相同的随机依赖关系。
// 调度程序运行的每一个DAG都被分成不同的阶段，在发生shuffle的地方，然后DAGScheduler以拓扑顺序运行这些阶段。

// 每个阶段可以是一个shuffle map stage，在这种情况下，其任务的结果将作为另一个stage或result stage的输入，在这种情况下，
// 它的任务直接计算触发job的action操作（例如count（），save（） ，等等）。对于shuffle map stages，还会跟踪每个输出partition所在的节点。

// 每个stage还有一个jobId，用于识别第一次提交stage的job。当使用FIFO调度时，这允许首先计算较早jobs的stage，或者在故障时恢复的更快。

// callSite提供与stage相关的用户代码中的位置。对于shuffle map stage，callSite给出创建RDD正在被shuffle的用户代码。
// 对于result stage，callSite给出执行相关action操作的用户代码（例如count（））。

// A single stage可以由多次尝试组成。在这种情况下，将为每次尝试更新latestInfo字段。
private[spark] class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val shuffleDep: Option[ShuffleDependency[_, _, _]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val jobId: Int,
    val callSite: CallSite)
  extends Logging {

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions = rdd.partitions.size
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  /** For stages that are the final (consists of only ResultTasks), link to the ActiveJob. */
  var resultOfJob: Option[ActiveJob] = None
  var pendingTasks = new HashSet[Task[_]]

  private var nextAttemptId = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  /** Pointer to the latest [StageInfo] object, set by DAGScheduler. */
  var latestInfo: StageInfo = StageInfo.fromStage(this)

  def isAvailable: Boolean = {
    if (!isShuffleMap) {
      true
    } else {
      numAvailableOutputs == numPartitions
    }
  }

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }

  /** Return a new attempt id, starting with 0. */
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def attemptId: Int = nextAttemptId

  override def toString = "Stage " + id

  override def hashCode(): Int = id

  override def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
}
