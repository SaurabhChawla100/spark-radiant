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

}
