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
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    startTime = applicationStart.time
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val endTime = applicationEnd.time - startTime
    logInfo(s"Total Time taken by Application:: $endTime ms")
    val jobTime = jobInfoMap.map(info => info._2.jobEnd - info._2.jobStart).sum
    val timeSpendInDriver = endTime - jobTime
    logInfo(s"Time spend in the Driver:  $timeSpendInDriver ms")
    val percentDriverTime = (timeSpendInDriver*100)/endTime
    if (percentDriverTime > 25) {
      logInfo(s"Percentage of time spend in the Driver: $percentDriverTime." +
        s" Try adding more parallelism to the Spark job for Optimal Performance")
    }
  }

  private def addSkewTaskInfo(): Unit = {
  }

  private def getSkewTaskInfo(): Unit = {
  }
}

case class JobInfo(jobId: Long = -1L, var jobStart: Long = 0L, var jobEnd: Long = 0L)
