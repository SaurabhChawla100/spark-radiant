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
    sparkConf.getInt("spark.core.clean.stage.info", 10)
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

}
