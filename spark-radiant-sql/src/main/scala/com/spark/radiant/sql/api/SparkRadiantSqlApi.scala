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

package com.spark.radiant.sql.api

import com.spark.radiant.sql.catalyst.optimizer.SparkSqlDFOptimizerRule
import com.spark.radiant.sql.utils.SparkSqlUtils

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{Column, DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.sparkRadiantUtil.SparkSqlUtil

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * SparkRadiantSqlApi having list of methods that are exposed to users
 */

class SparkRadiantSqlApi extends Logging with Serializable {

  private[api] val seqRule: Seq[Rule[LogicalPlan]] =
    Seq(com.spark.radiant.sql.catalyst.optimizer.SizeBasedJoinReOrdering,
    com.spark.radiant.sql.catalyst.optimizer.UnionReuseExchangeOptimizeRule,
    com.spark.radiant.sql.catalyst.optimizer.ExchangeOptimizeRule,
    com.spark.radiant.sql.catalyst.optimizer.ExplodeOptimizeRule,
    com.spark.radiant.sql.catalyst.optimizer.DynamicFilterOptimizer
  )

  /**
   *
   * This is method that applies the Dynamic filter to the left side of the table
   *
   * @param spark - existing spark session
   * @param inputDfPlan - Data frame to be optimized
   * @param bloomFilterCount - Length of bloomFilter that needs to be created
   * @return
   */
  def addDynamicFiltersToDF(spark: SparkSession,
     inputDfPlan: LogicalPlan,
     bloomFilterCount : Long = 0L): LogicalPlan = {
    val utils = new SparkSqlUtils()
    val dfCollectThread = utils.newDaemonSingleThreadExecutor("dfCollectThread")
    val dynamicFilterCompletionTime =
      utils.getDynamicFilterCompletionTime(spark.sparkContext.getConf)
    def doAsync(fn: => Unit): Unit = {
      dfCollectThread.submit(new Runnable() {
        override def run(): Unit = { fn }
      })
    }
    var updatedDfPlan = inputDfPlan
    try {
      doAsync {
        val bloomFilterLength = if (bloomFilterCount == 0) {
          utils.getBloomFilterSize(spark.sparkContext.getConf)
        } else {
          bloomFilterCount
        }
        val dfOptimizer = new SparkSqlDFOptimizerRule()
        updatedDfPlan = dfOptimizer.addDynamicFiltersPlan(spark,
          inputDfPlan, bloomFilterLength)
      }
    }
    catch {
      case ex: Throwable =>
        logDebug(s"exception while creating Dynamic Filter: ${ex}")
    }
    finally {
      dfCollectThread.shutdown()
    }
    dfCollectThread.awaitTermination(dynamicFilterCompletionTime, TimeUnit.SECONDS)
    dfCollectThread.shutdownNow()
    // cancel all the spark jobs for computing Dynamic Filter, If it is
    // not completed in spark.sql.dynamicFilter.completion.threshold time
    spark.sparkContext.cancelAllJobs()
    updatedDfPlan
  }

  /**
   * This is the method available to the User to optimize the dataframe
   *
   */

  def optimizeDataFrame(spark: SparkSession,
     inputDf: DataFrame,
     bloomFilterCount : Long = 0L): DataFrame = {
    try {
      var updatedPlan = addDynamicFiltersToDF(spark,
        inputDf.queryExecution.optimizedPlan, bloomFilterCount)
      val sqlUtils = new SparkSqlUtils()
      val df = sqlUtils.createDfFromLogicalPlan(spark, updatedPlan)
      val dfOptimizer = new SparkSqlDFOptimizerRule()
      updatedPlan = dfOptimizer.pushFilterBelowTypedFilterRule(df.queryExecution.optimizedPlan)
      sqlUtils.createDfFromLogicalPlan(spark, updatedPlan)
    } catch {
      case _ : Throwable =>
        inputDf
    }
  }
  /**
   * This method provides the functionality to use withColumns of Apache
   * Spark which is not exposed in the open source spark code. This will prevent
   * from adding the extra project in the logical plan and prevents
   * from the stackoverflow error.
   * [SPARK-26224][SQL] issue of withColumn while using it multiple times
   *
   * @param columnNameValue - Map of column name and value
   * @param baseDataFrame - Dataframe on which withColumn is applied
   * @return - new dataframe with updated columns
   */
  def useWithColumnsOfSpark(columnNameValue: Map[String, Column],
                            baseDataFrame: DataFrame): DataFrame = {
    try {
      val column = columnNameValue.unzip
      // scalastyle:off
      // using the reflection code to call the method withColumns
      val dataSetClass = Class.forName("org.apache.spark.sql.Dataset")
      val newConfigurationMethod =
        dataSetClass.getMethod("withColumns", classOf[Seq[String]], classOf[Seq[Column]])
      newConfigurationMethod.invoke(
        baseDataFrame, column._1, column._2).asInstanceOf[DataFrame]
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
   * Drop duplicate for the StructType Column
   * @param dataFrame - existing DataFrame
   * @param spark - existing sparkSession
   * @param colNames - sequence of ColumnName for the DropDuplicate
   *
   * [SPARK-37596][SQL] Add the support for struct type column in the DropDuplicate #34849
   */
  def dropDuplicateOfSpark(dataFrame: DataFrame,
     spark: SparkSession,
     colNames: Seq[String]): DataFrame = {
    val resolver = spark.sessionState.analyzer.resolver
    val allColumns = dataFrame.queryExecution.analyzed.output
    // get the struct column from the dropDuplicate colNames
    val structMap: mutable.Map[String, Column] = mutable.LinkedHashMap.empty
    colNames.foreach { x =>
      if (!allColumns.exists(i => resolver(i.name, x))) {
        structMap.put(x, new org.apache.spark.sql.Column(x))
      }
    }
    val updatedDF =  if (structMap.nonEmpty) {
      useWithColumnsOfSpark(structMap, dataFrame)
    } else {
      dataFrame
    }
    updatedDF.dropDuplicates(colNames).
      select(allColumns.map(new org.apache.spark.sql.Column(_)): _*)
  }

  /**
   *  Api call for adding the optimizer rule
   * @param spark - existing sparkSession
   */
  def addOptimizerRule(spark: SparkSession): Unit = {
    // Importing the extra Optimizations rule
    spark.experimental.extraOptimizations = seqRule
  }

  /**
   *  Api call for adding the optimizer rule in the extendedOperatorOptimizationRules
   * @param spark - existing sparkSession
   */
  def addOptimizerRuleInSqlExt(spark: SparkSession): Unit = {
    // Importing the extra Optimizations rule in extendedOperatorOptimizationRules
    SparkSqlUtil.injectRule(spark, seqRule)
  }
}

/**
 * SparkRadiantSqlExtension - Inject the extra Optimizations rule
 * --conf spark.sql.extensions=com.spark.radiant.sql.api.SparkRadiantSqlExtension
 */
class SparkRadiantSqlExtension extends (SparkSessionExtensions => Unit) {
  def apply(sparkExt : SparkSessionExtensions): Unit = {
    val sqlApi = new SparkRadiantSqlApi
    // inject the extra Optimizer rule
    sqlApi.seqRule.foreach { rule =>
      sparkExt.injectOptimizerRule(_ => rule)
    }
  }
}
