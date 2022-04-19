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

package com.spark.radiant.sql.index

import com.spark.radiant.sql.utils.SparkSqlUtils

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{DataFrame, PersistBloomFilterExpr, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 *  add Bloom Filter Index on tables accessed by Spark
 */

class BloomFilterIndexImpl {
  // TODO work on this feature

  /**
   * Apply the BloomFilterIndex to dataFrame
   * @param spark
   * @param dataFrame
   * @param path
   * @param attrName
   * @return
   */
  def applyBloomFilterToDF(spark: SparkSession,
     dataFrame: DataFrame,
     path: String, attrName: String): DataFrame = {
    // TODO - This is the initial code, needs to be improved
    //  and also support is needed for different rel
    val sqlUtils = new SparkSqlUtils()
    val plan = dataFrame.queryExecution.optimizedPlan
    var hold = false
    val updatedPlan = plan.transform {
      case filter@Filter(_, rel: LogicalRelation) if
        !hold & rel.relation.schema.names.contains(attrName) =>
        val attrExpr = dataFrame.select(attrName).queryExecution.optimizedPlan
          .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Project]
          .projectList.head
        hold = true
        val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
        Filter(bloomFilterExpression, filter)
      case rel: LogicalRelation if
        !hold & rel.relation.schema.names.contains(attrName) =>
        val attrExpr = dataFrame.select(attrName).queryExecution.optimizedPlan
          .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Project]
          .projectList.head
        hold = true
        val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
        Filter(bloomFilterExpression, rel)
    }
    sqlUtils.createDfFromLogicalPlan(spark, updatedPlan)
  }

}
