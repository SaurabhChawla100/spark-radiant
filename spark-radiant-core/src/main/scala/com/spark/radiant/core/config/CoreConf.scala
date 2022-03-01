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

package com.spark.radiant.core.config

import org.apache.spark.SparkConf

object CoreConf {

  private [core] def getDriverPercentSpentThreshold(sparkConf: SparkConf): Int = {
    // default value is 30 percent
     sparkConf.getInt("spark.core.percent.driver.threshold", 30)
  }

  private [core] def getDriverSpentTimeThreshold(sparkConf: SparkConf): Int = {
    // default value is 300 seconds
    sparkConf.getInt("spark.core.timeSpent.driver.threshold", 300)
  }

  private [core] def getPercentMeanRecordProcessed(sparkConf: SparkConf): Int = {
    // default value is 20 percent
    sparkConf.getInt("spark.core.percent.mean.record.processed", 20)
  }

  private [core] def getPercentMeanTimeRecordProcessed(sparkConf: SparkConf): Int = {
    // default value is 30 percent
    sparkConf.getInt("spark.core.percent.mean.time.record.processed", 30)
  }

  private [core] def getTimeForTaskCompletion(sparkConf: SparkConf): Long = {
    // default value is 5 sec
    sparkConf.getLong("spark.core.time.task.completion", 5000L)
  }

  private [core] def getCleanUpStageInfo(sparkConf: SparkConf): Int = {
    // default value is 10
    sparkConf.getInt("spark.core.clean.stage.info", 20)
  }

  private [core] def getMaxStageInfo(sparkConf: SparkConf): Int = {
    // default value is 30
    sparkConf.getInt("spark.core.max.stage.info", 30)
  }

  private [core] def getTopEntryTaskInfo(sparkConf: SparkConf): Int = {
    // default value is 10
    sparkConf.getInt("spark.core.display.limit.task.info", 10)
  }

  private [core] def getAllEntryFromTaskInfo(sparkConf: SparkConf): Boolean = {
    // default value is false
    sparkConf.getBoolean("spark.core.display.all.task.info", false)
  }

  private [core] def getMeanTaskCompletionFactor(sparkConf: SparkConf): Int = {
    // default value is 2
    sparkConf.getInt("spark.core.mean.task.completion.factor", 2)
  }

}
