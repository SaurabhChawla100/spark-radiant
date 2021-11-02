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

package com.spark.radiant.core

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import scala.collection.mutable.HashMap

/**
 *  SparkJobMetricsCollector is used for collecting the important metrics to
 *  Spark Driver Memory, Stage metrics, Task Metrics. This is enabled by
 *  using the configuration
 *  --conf spark.extraListeners=com.spark.radiant.core.SparkJobMetricsCollector
 */

class SparkJobMetricsCollector()
  extends SparkListener with Logging {

  private val jobInfoMap: HashMap[Long, JobInfo] = HashMap.empty
  private val stageInfoMap: HashMap[Long, StageInfo] = HashMap.empty
  private val taskInfoMap: HashMap[Long, Seq[TaskInfo]] = HashMap.empty
  private var startTime: Long = 0

  // TODO work on this feature

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobInfo = jobInfoMap.getOrElseUpdate(jobStart.jobId,
      JobInfo(jobStart.jobId, jobStart.time))
    jobInfoMap.put(jobStart.jobId, jobInfo)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobInfo = jobInfoMap.getOrElseUpdate(jobEnd.jobId,
      JobInfo(jobEnd.jobId, 0, jobEnd.time))
    jobInfo.jobEnd = jobEnd.time
    jobInfoMap.put(jobEnd.jobId, jobInfo)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
   val stageInfo = stageSubmitted.stageInfo
    stageInfoMap.put(stageInfo.stageId,
      StageInfo(stageInfo.stageId, stageInfo.submissionTime.get))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageInfoValue = stageInfoMap.getOrElseUpdate(stageInfo.stageId,
      StageInfo(stageInfo.stageId, 0))
    stageInfoValue.StageEnd = stageInfo.completionTime.get
    val taskInfoValue: Seq[TaskInfo] = taskInfoMap(stageInfo.stageId)
    val executorGroupByTask =
      taskInfoValue.groupBy(_.executorId).mapValues { x =>
        (x.map(_.recordsRead).sum, x.map(_.shuffleWriteRecord).sum,
          x.map(_.shuffleReadRecord).sum, x.map(_.recordsWrite).sum)
      }
    val meanTaskProcessByExec =
      Math.floor(executorGroupByTask.values.map(_._1).sum / executorGroupByTask.size)
    val taskReadInfo = taskInfoValue.map(x => x.recordsRead)
    val meanTaskRecordsProcess = Math.floor(taskReadInfo.sum / taskReadInfo.size)
    val skewTaskInfo = taskInfoValue.filter {
      taskInfo =>
        (taskInfo.recordsRead/meanTaskRecordsProcess) >= (20 * meanTaskRecordsProcess)/100
    }
    val skewTaskInfoExec = executorGroupByTask.values.filter {
      taskInfo =>
        (taskInfo._1/meanTaskProcessByExec) >= (20 * meanTaskProcessByExec)/100
    }
    // scalastyle:off println
    println(s"Stage Info Metrics stageId :: ${stageInfo.stageId}")
    println(s"processing task info on executor:: ${executorGroupByTask.toString()}")
    println(s"skewTaskInfo based on the task processed:${skewTaskInfo}")
    println(s"skewTaskInfoExec based on the task processed on each executor :" +
      s" ${skewTaskInfoExec.toString()}")
    stageInfoMap.put(stageInfo.stageId, stageInfoValue)
    taskInfoMap.drop(stageInfo.stageId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val stageId = taskEnd.stageId
    val taskInfo = taskEnd.taskInfo
    val taskInputMetrics = taskEnd.taskMetrics.inputMetrics
    val taskOutputMetrics = taskEnd.taskMetrics.outputMetrics
    val taskShuffleReadRecords = taskEnd.taskMetrics.shuffleReadMetrics
    val taskShuffleWriteRecords = taskEnd.taskMetrics.shuffleWriteMetrics
    var taskInfoSeq = taskInfoMap.getOrElseUpdate(stageId, Seq.empty)
    // Create a taskInfo
    val info = TaskInfo(taskInfo.taskId,
      taskInputMetrics.recordsRead + taskShuffleWriteRecords.recordsWritten,
      taskShuffleWriteRecords.recordsWritten, taskInfo.executorId,
      taskShuffleReadRecords.recordsRead, taskOutputMetrics.recordsWritten,
      taskInfo.launchTime - taskInfo.finishTime)

    if (taskInfoSeq.nonEmpty) {
      taskInfoSeq = taskInfoSeq :+ info
    } else {
      taskInfoSeq = Seq(info)
    }
    taskInfoMap.put(taskEnd.stageId, taskInfoSeq)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    startTime = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val completionTime = (applicationEnd.time - startTime)/1000
    // scalastyle:off println
    println("Spark-Radiant Metrics Collector")
    println(s"Total Time taken by Application:: $completionTime sec")
    showDriverMetrics(completionTime)
  }

  private def addSkewTaskInfo(): Unit = {
  }

  private def getSkewTaskInfo(): Unit = {
  }

  private def showDriverMetrics(completionTime: Long): Unit = {
    val jobTime = (jobInfoMap.map(info => info._2.jobEnd - info._2.jobStart).sum)/1000
    val timeSpendInDriver = (completionTime - jobTime)
    // scalastyle:off println
    println("Driver Metrics:")
    println(s"Time spend in the Driver: $timeSpendInDriver sec")
    val percentDriverTime = (timeSpendInDriver*100)/completionTime
    // if the time spend in driver is greater than the 25% of total time
    // and total time spend in driver greater than 5 min than recommend
    // for adding more parallelism in the spark Application
    if (percentDriverTime > 25 && timeSpendInDriver > 300) {
      println(s"Percentage of time spend in the Driver: $percentDriverTime." +
        s" Try adding more parallelism to the Spark job for Optimal Performance")
    }
  }
}

case class JobInfo(jobId: Long = -1L, var jobStart: Long = 0L, var jobEnd: Long = 0L)

case class StageInfo (
   stageId: Long = -1L,
   var stageStart: Long = 0L,
   var StageEnd: Long = 0L,
   var taskInfo: Option[Seq[TaskInfo]] = None)

case class TaskInfo (
   taskId: Long = -1L,
   recordsRead: Long = -1L,
   shuffleWriteRecord: Long = -1L,
   executorId: String = "0",
   shuffleReadRecord: Long = -1L,
   recordsWrite: Long = -1L,
   taskCompletionTime: Long = 0L)
