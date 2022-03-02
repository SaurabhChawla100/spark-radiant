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

package com.spark.radiant.core.sparkCoreInfo

case class JobInfo(jobId: Long = -1L, var jobStart: Long = 0L, var jobEnd: Long = 0L)

case class StageInfo (
   stageId: Long = -1L,
   var totalTask: Long = 0L,
   var stageStart: Long = 0L,
   var stageStatus: String = "",
   var stageEndTime: Long = 0L,
   var meanTaskCompletionTime: Long = 0L,
   var numberOfExecutor: Long = 0L,
   var failedTaskCount: Long = 0L,
   var recommendedCompute: Option[String] = Some("No Benefit"),
   var skewTaskInfo: Option[Seq[TaskInfo]] = None,
   var failedTaskInfo: Option[Seq[TaskInfo]] = None) {

  override def toString(): String = {
    val stageCompletionTime = stageEndTime - stageStart
    val skewTaskInfoValue = if (skewTaskInfo.isDefined) {
      skewTaskInfo.get.toString()
    } else {
      "Skew task in not present in this stage"
    }
    val failedTaskInfoValue = if (failedTaskInfo.isDefined) {
      failedTaskInfo.get.toString()
    } else {
      "Failed task in not present in this stage"
    }
    s"""{
       | "Stage Id": $stageId,
       | "Final Stage Status": $stageStatus,
       | "Number of Task": $totalTask,
       | "Total Executors ran to complete all Task": $numberOfExecutor,
       | "Stage Completion Time": $stageCompletionTime ms,
       | "Stage Completion Time Recommendation": ${recommendedCompute.get},
       | "Average Task Completion Time": $meanTaskCompletionTime ms,
       | "Number of Task Failed in this Stage": $failedTaskCount,
       | "Few Skew task info in Stage": $skewTaskInfoValue,
       | "Few Failed task info in Stage": $failedTaskInfoValue
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
   taskCompletionTime: Long = 0L,
   taskStatus: String,
   failedTaskEndReason: String) {

  override def toString(): String = {
    s"""{
       | "Task Id": $taskId,
       | "Executor Id": $executorId,
       | "Number of records read in task": $recordsRead,
       | "Number of shuffle read Record in task": $shuffleReadRecord,
       | "Number of records write in task": $recordsWrite,
       | "Number of shuffle write Record in task": $shuffleWriteRecord,
       | "Task Completion Time": $taskCompletionTime ms,
       | "Final Status of task": $taskStatus,
       | "Failure Reason for task": $failedTaskEndReason
    }""".stripMargin
  }
}
