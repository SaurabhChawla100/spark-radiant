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
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.sparkRadiantUtil.SparkSqlUtil
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{DataFrame, PersistBloomFilterExpr, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  DataSourceV2ScanRelation}

/**
 *  add Bloom Filter Index on tables accessed by Spark
 */

class BloomFilterIndexImpl extends LazyLogging {
  // TODO work on this feature
  val aggBlmFilter = "aggBlmFilter"
  val attrDelimiter = "~#~"

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
     path: String, attrName: List[String]): DataFrame = {
    // TODO - This is the initial code, needs to be improved
    //  and also support is needed for different rel
    val sqlUtils = new SparkSqlUtils()
    val updatedPlan = applyBloomFilterToPlan(dataFrame.queryExecution.optimizedPlan,
      path, attrName)
    sqlUtils.createDfFromLogicalPlan(spark, updatedPlan)
  }

  /**
   *
   * @param spark
   * @param dataFrame
   * @param path
   * @param attrName
   * @param numberOfItems
   */
  def saveBloomFilterFromDF(spark: SparkSession,
     dataFrame: DataFrame,
     path: String,
     attrName: List[String],
     numberOfItems: Long): Unit = {
    val sqlUtils = new SparkSqlUtils()
    var updatedPlan = dataFrame.queryExecution.optimizedPlan
    val oldAttr = updatedPlan.outputSet.toList
      .map(_.asInstanceOf[org.apache.spark.sql.catalyst.expressions.NamedExpression])
    if (oldAttr.map(_.name).count(attr => attrName.contains(attr)) == attrName.size) {
      val newAttrList = oldAttr ++ List(SparkSqlUtil.aggregatedFilter(
        attrName, oldAttr, attrDelimiter, aggBlmFilter))
      updatedPlan = Project(newAttrList, updatedPlan)
      val bloomFilter = sqlUtils.createDfFromLogicalPlan(spark, updatedPlan).
        stat.bloomFilter(aggBlmFilter, numberOfItems, 0.2)
      sqlUtils.saveBloomFilter(bloomFilter, path)
    } else {
      logger.warn("Bloom filter cannot be created because of" +
        " mismatch the aggregated bloomFilter attr")
    }
  }

  /**
   *
   * @param plan
   * @param path
   * @param attrName
   * @return Logical Plan having bloomFilter
   */
  def applyBloomFilterToPlan(
     plan: LogicalPlan,
     path: String, attrName: List[String]): LogicalPlan = {

    val oldAttr = plan.outputSet.toList
      .map(_.asInstanceOf[org.apache.spark.sql.catalyst.expressions.NamedExpression])
    var hold = false
    if (oldAttr.map(_.name).count(attr => attrName.contains(attr)) == attrName.size) {
      val newAttrList = oldAttr ++ List(SparkSqlUtil.aggregatedFilter(
        attrName, oldAttr, attrDelimiter, aggBlmFilter))

      val attrExpr = newAttrList.filter(c => c.name.equals(aggBlmFilter)).head

      val newPlan = Project(newAttrList, plan)

      val updatedPlan = newPlan.transform {
        case filter@Filter(_, rel: LogicalRelation) if
          !hold
            & rel.relation.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, filter)
        case filter@Filter(_, hiveRel: HiveTableRelation) if
          !hold
            & hiveRel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, filter)
        case filter@Filter(_, dsV2Rel: DataSourceV2Relation) if
          !hold
            & dsV2Rel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, filter)
        case filter@Filter(_, dsV2Rel: DataSourceV2ScanRelation) if
          !hold
            & dsV2Rel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, filter)
        case rel: LogicalRelation if
          !hold &
            rel.relation.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, rel)
        case hiveRel: HiveTableRelation if
          !hold &
            hiveRel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, hiveRel)
        case dsV2Rel: DataSourceV2Relation if
          !hold &
            dsV2Rel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, dsV2Rel)
        case dsV2Rel: DataSourceV2ScanRelation if
          !hold &
            dsV2Rel.schema.names.count(x => attrName.contains(x)) == attrName.size =>
          hold = true
          val bloomFilterExpression = PersistBloomFilterExpr(Literal(path), attrExpr)
          Filter(bloomFilterExpression, dsV2Rel)
      }
      Project(oldAttr, updatedPlan)
    } else {
      logger.info("Bloom filter cannot be applied because of mismatch the aggregated bloomFilter attr")
      plan
    }
  }

}
