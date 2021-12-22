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

import com.spark.radiant.core.config.CoreConf
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap

/**
 *  SparkJobMetricsCollector is used for collecting the important metrics to
 *  Spark Driver Memory, Stage metrics, Task Metrics. This is enabled by
 *  using the configuration
 *  --conf spark.extraListeners=com.spark.radiant.core.SparkJobMetricsCollector
 */

class SparkJobMetricsCollector()
  extends SparkListener with Logging {

  private val jobInfoMap: HashMap[Long, JobInfo] = HashMap.empty
  private val stageInfoMap: LinkedHashMap[Long, StageInfo] = LinkedHashMap.empty
  private val taskInfoMap: HashMap[Long, Seq[TaskInfo]] = HashMap.empty
  private var startTime: Long = 0
  private val sparkConf = SparkSession.getActiveSession match {
    case Some(spark) => spark.sparkContext.getConf
    case _ => new SparkConf()
  }
  private val maxStageInfo: Int = CoreConf.getMaxStageInfo(sparkConf)

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
    // synchronized block needed to handle parallel stage submitted at same time
    synchronized {
      if (stageInfoMap.size == maxStageInfo) {
        showStageLevelMetrics()
        // clean up n number of stage info given as per conf spark.core.clean.stage.info
        val stageCleanUp = CoreConf.getCleanUpStageInfo(sparkConf)
        val itr = stageInfoMap.iterator
        for (_ <- 1 to stageCleanUp) {
          stageInfoMap.remove(itr.next()._1)
        }
      }
    }
    stageInfoMap.put(stageInfo.stageId,
      StageInfo(stageInfo.stageId, stageInfo.numTasks, stageInfo.submissionTime.get))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageInfoValue = stageInfoMap.getOrElseUpdate(stageInfo.stageId,
      StageInfo(stageInfo.stageId, 0))
    stageInfoValue.stageEndTime = stageInfo.completionTime.get
    val taskInfoValue: Seq[TaskInfo] = taskInfoMap(stageInfo.stageId)
    val executorGroupByTask =
      taskInfoValue.groupBy(_.executorId).mapValues { x =>
        (x.map(_.recordsRead).sum, x.map(_.shuffleWriteRecord).sum,
          x.map(_.shuffleReadRecord).sum, x.map(_.recordsWrite).sum)
      }
    val meanTaskProcessByExec =
      Math.floor(executorGroupByTask.values.map(_._1).sum / executorGroupByTask.size)
    val taskInfo = taskInfoValue.map(x => (x.recordsRead, x.taskCompletionTime))
    val meanTaskRecordsProcess =
      Math.floor(taskInfo.map(_._1).sum / taskInfo.size)
    val meanTaskCompletionTime = Math.floor(taskInfo.map(_._2).sum / taskInfo.size)
    val skewTaskInfo = taskInfoValue.filter {
      taskInfo =>
        ((taskInfo.recordsRead/meanTaskRecordsProcess) >=
          (CoreConf.getPercentMeanRecordProcessed(sparkConf) * meanTaskRecordsProcess)/100
          || ((taskInfo.taskCompletionTime >=
          CoreConf.getTimeForTaskCompletion(sparkConf))
          && (taskInfo.taskCompletionTime/meanTaskCompletionTime) >=
          (CoreConf.getPercentMeanTimeRecordProcessed(sparkConf) * meanTaskCompletionTime)/100))
    }
    val skewTaskInfoExec = executorGroupByTask.values.filter {
      taskInfo =>
        (taskInfo._1/meanTaskProcessByExec) >= (20 * meanTaskProcessByExec)/100
    }
    logInfo(s"Stage Info Metrics stageId :: ${stageInfo.stageId}")
    logInfo(s"Average Completion time taken by task :: ${meanTaskCompletionTime}")
    logInfo(s"processing task info on executor:: ${executorGroupByTask.toString()}")
    logInfo(s"skewTaskInfo based on the task processed:${skewTaskInfo}")
    logInfo(s"skewTaskInfoExec based on the task processed on each executor :" +
      s" ${skewTaskInfoExec.toString()}")
    stageInfoValue.meanTaskCompletionTime = meanTaskCompletionTime.toLong
    stageInfoValue.skewTaskInfo =
      if (skewTaskInfo.nonEmpty && taskInfoValue.length > 1
        && skewTaskInfo.length != taskInfoValue.length) {
      Some(skewTaskInfo)
    } else {
      None
    }
    stageInfoValue.numberOfExecutor = executorGroupByTask.size
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
      taskInfo.finishTime - taskInfo.launchTime)

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
    println()
    showDriverMetrics(completionTime)
    showStageLevelMetrics()
  }

  private def addSkewTaskInfo(): Unit = {
  }

  private def getSkewTaskInfo(): Unit = {
  }

  private def showDriverMetrics(completionTime: Long): Unit = {
    val jobTime = (jobInfoMap.map(info => info._2.jobEnd - info._2.jobStart).sum)/1000
    val timeSpendInDriver = (completionTime - jobTime)
    // scalastyle:off println
    println("*****Driver Metrics*****")
    println(s"Time spend in the Driver: $timeSpendInDriver sec")
    val percentDriverTime = (timeSpendInDriver*100)/completionTime
    // if the time spend in driver is greater than the 30% of total time
    // and total time spend in driver greater than 5 min than recommend
    // for adding more parallelism in the spark Application
    if (percentDriverTime > CoreConf.getDriverPercentSpentThreshold(sparkConf)
      && timeSpendInDriver > CoreConf.getDriverSpentTimeThreshold(sparkConf)) {
      println(s"Percentage of time spend in the Driver: $percentDriverTime %." +
        s" Try adding more parallelism to the Spark job for Optimal Performance")
    }
    println()
  }

  private def showStageLevelMetrics(): Unit = {
    // scalastyle:off println
    println("*****Stage Info Metrics*****")
    val itr = stageInfoMap.iterator
    while(itr.hasNext) {
      val stageInfo = itr.next
      println(s"Stage Info Metrics Stage Id:${stageInfo._1}")
      println(stageInfo._2)
    }
    println()
  }
}

case class JobInfo(jobId: Long = -1L, var jobStart: Long = 0L, var jobEnd: Long = 0L)

case class StageInfo (
   stageId: Long = -1L,
   var totalTask: Long = 0L,
   var stageStart: Long = 0L,
   var stageEndTime: Long = 0L,
   var meanTaskCompletionTime: Long = 0L,
   var numberOfExecutor: Long = 0L,
   var skewTaskInfo: Option[Seq[TaskInfo]] = None) {

  override def toString(): String = {
    val stageCompletionTime = stageEndTime - stageStart
    val skewTaskInfoValue = if (skewTaskInfo.isDefined) {
      skewTaskInfo.get.toString()
    } else {
      "Skew task in not present in this stage"
    }
    s"""{
       | "Stage Id": ${stageId},
       | "Number of Task": ${totalTask},
       | "Total Executors ran to complete all Task": ${numberOfExecutor},
       | "Stage Completion Time": ${stageCompletionTime} ms,
       | "Average Task Completion Time": ${meanTaskCompletionTime} ms
       | "Stage Skew info": ${skewTaskInfoValue}
    }""".stripMargin
  }
}

case class TaskInfo (
   taskId: Long = -1L,
   recordsRead: Long = -1L,
   shuffleWriteRecord: Long = -1L,
   executorId: String = "0",
   shuffleReadRecord: Long = -1L,
   recordsWrite: Long = -1L,
   taskCompletionTime: Long = 0L) {

  override def toString(): String = {
    s"""{
       | "Task Id": ${taskId},
       | "Executor Id": ${executorId},
       | "Number of records read in task": ${recordsRead},
       | "Number of shuffle read Record in task": ${shuffleReadRecord},
       | "Number of records write in task": ${recordsWrite},
       | "Number of shuffle write Record in task": ${shuffleWriteRecord},
       | "Task Completion Time": ${taskCompletionTime} ms
    }""".stripMargin
  }
}
