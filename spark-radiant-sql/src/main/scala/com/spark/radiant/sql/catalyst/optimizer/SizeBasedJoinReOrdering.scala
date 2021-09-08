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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * SizeBasedJoinReOrdering - Spark-Radiant SizeBasedJoinReOrdering works well for the
 * Join which is a type of star schema, where one table consists of large number of
 * records as compared to other tables and all the join condition of smaller table
 * with large table. Spark by default perform join left to right(whether its SMJ
 * before the BHJ or vice versa). This optimizer rule performs the join smaller table
 * join first before the bigger table(BHJ first before the SMJ.)
 *
 * Before this optimization              After this optimization
 * SMJ -> BHJ                     =>      BHJ -> SMJ
 * SMJ -> BHJ -> SMJ              =>      BHJ -> SMJ -> SMJ
 *
 * This is enabled by the conf --conf spark.sql.support.sizebased.join.reorder=true
 *
 */
object SizeBasedJoinReOrdering extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    try {
      val spark = SparkSession.getActiveSession.get
      val bhjThreshold: Long = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        .split("b").head.toLong
      if (applySizeBasedJoinReOrder(spark)) {
        plan.transform {
          case join: Join if validRightSide(join.right) && validChildForReOrder(join.left)
            && validJoinType(join.joinType) =>
            var leftPlan = join.left match {
              case Project(_, child: Join) if validJoinType(child.joinType) => child
              case join: Join => join
            }
            val rightPlan = join.right
            var updatedJoin = join
            val rightOutputSet = rightPlan.output.map(x => x.exprId)
            val leftJoinCondAttr = join.condition.get.collectLeaves()
              .filter(x => !rightOutputSet.contains(x.asInstanceOf[AttributeReference].exprId))
              .map(x => x.asInstanceOf[AttributeReference].exprId)
            // check if existing join strategy is not BHJ and the other join in
            // sql is BHJ.
            if (rightPlan.stats.sizeInBytes <= bhjThreshold &&
              leftPlan.right.stats.sizeInBytes > bhjThreshold &&
              leftPlan.right.stats.sizeInBytes > join.right.stats.sizeInBytes) {
              val leftOutAttr = leftPlan.left.output.map(
                x => x.asInstanceOf[AttributeReference].exprId)
              val leftOutputAttr = leftJoinCondAttr.find {
                attr => !leftOutAttr.contains(attr)
              }
              if (validJoinType(leftPlan.joinType) && leftOutputAttr.isEmpty) {
                val rightSideForLeftJoin = leftPlan.right
                val innerJoinCondition = leftPlan.condition
                val innerHint = leftPlan.hint
                leftPlan = leftPlan.copy(left = leftPlan.left, joinType = Inner,
                  right = rightPlan, condition = join.condition, hint = join.hint)
                // updated join is having the BHJ first before any other joins(SMJ or SHJ etc)
                updatedJoin = updatedJoin.copy(left = leftPlan, joinType = Inner,
                  right = rightSideForLeftJoin, condition = innerJoinCondition, hint = innerHint)
              }
            }
            updatedJoin
        }
      } else {
        plan
      }
    } catch {
      case ex: AnalysisException =>
        logDebug(s"Not able to create SizeBasedJoinReOrdering: ${ex}")
        plan
    }
  }

  private def validRightSide(plan: LogicalPlan): Boolean = {
   plan match {
     case Project(_, Filter(_, LocalRelation(_, _, _)))
          | Project(_, Filter(_, LogicalRelation(_, _, _, _)))
          |  Project(_, Filter(_, HiveTableRelation(_, _, _, _, _))) => true
     case Filter(_, LocalRelation(_, _, _))
          | Filter(_, LogicalRelation(_, _, _, _))
          | Filter(_, HiveTableRelation(_, _, _, _, _)) => true
     case LocalRelation(_, _, _)
          |  LogicalRelation(_, _, _, _)
          |  HiveTableRelation(_, _, _, _, _) => true
     case _ => false
   }
  }

  private def validJoinType(joinType: JoinType): Boolean = {
    joinType match {
      case Inner => true
      case _ => false
    }
  }

  private def validChildForReOrder(plan: LogicalPlan): Boolean = {
    plan match {
      case Project(_, child: Join) => true
      case join: Join => true
      case _ => false
    }
  }

  private def applySizeBasedJoinReOrder(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.support.sizebased.join.reorder",
      spark.sparkContext.getConf.get("spark.sql.support.sizebased.join.reorder",
        "false")).toBoolean
  }
}
