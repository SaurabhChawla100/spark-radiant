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

package com.spark.radiant.sql.catalyst.optimizer

import com.spark.radiant.sql.api.SparkRadiantSqlApi
import com.spark.radiant.sql.utils.SparkSqlUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object DynamicFilterOptimizer extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    try {
      val spark = SparkSession.getActiveSession.get
      if (applyDynamicFilter(spark)) {
        val dfOptimizerApi = new SparkRadiantSqlApi()
        var updatedPlan = dfOptimizerApi.addDynamicFiltersToDF(spark, plan)
        if (updatedPlan != plan) {
          val sqlUtils = new SparkSqlUtils()
          val df = sqlUtils.createDfFromLogicalPlan(spark, updatedPlan)
          val dfOptimizer = new SparkSqlDFOptimizerRule()
          updatedPlan = dfOptimizer.pushFilterBelowTypedFilterRule(df.queryExecution.optimizedPlan)
        }
        updatedPlan
      } else {
        plan
      }
    } catch {
      case ex: Throwable =>
        logDebug(s"Not able to create DynamicFilter: ${ex}")
        plan
    }
  }

  private def applyDynamicFilter(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.support.dynamicfilter",
      spark.sparkContext.getConf.get("spark.sql.support.dynamicfilter",
        "true")).toBoolean
  }
}
