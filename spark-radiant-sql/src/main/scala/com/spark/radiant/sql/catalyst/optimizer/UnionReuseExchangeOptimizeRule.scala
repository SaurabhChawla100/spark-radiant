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

import com.spark.radiant.sql.utils.SparkSqlUtils

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation,
  LogicalPlan, Project, Repartition, RepartitionByExpression, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * UnionReuseExchangeOptimizeRule - This rule works for scenario when union is present with
 * aggregation having same grouping column and the union is between the same table/datasource .
 * In this scenario instead of scan twice the table/datasource because of the child of Union,
 * There will be one scan and the other child of union will reuse this scan.This feature is
 * enabled using --conf spark.sql.optimize.union.reuse.exchange.rule=true
 */
object UnionReuseExchangeOptimizeRule extends Rule[LogicalPlan] with LazyLogging {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useUnionReuseExchangeRule(spark)) {
      try {
        val repartitionExist = plan.find {
          case Repartition(_, _, _) | RepartitionByExpression(_, _, _) => true
          case _ => false
        }
        if (repartitionExist.isEmpty) {
          val updatedPlan = plan.transform {
            case union: Union =>
              val left = union.children.head
              val right = union.children.last
              if (compareChildForUnionOpt(left, right)) {
                union.copy(Seq(getUpdateChildPlanForUnion(left),
                  getUpdateChildPlanForUnion(right)))
              } else {
                union
              }
          }
          updatedPlan
        } else {
          plan
        }
      } catch {
        case ex: AnalysisException =>
          logger.debug(s"exception on applying UnionReuseExchangeOptimizeRule: ${ex}")
          plan
      }
    } else {
      plan
    }
  }

  private def getUpdateChildPlanForUnion(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case agg@Aggregate(_, _, child) if validScenariosForOptPlan(child) =>
        genPlanForAggregate(agg)
      case _ => plan
    }
  }

  private def genPlanForAggregate(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case agg: Aggregate if !agg.child.isInstanceOf[RepartitionByExpression] =>
       agg.copy(child = RepartitionByExpression(agg.groupingExpressions, agg.child, None))
      case _ => plan
    }
  }

  private def useUnionReuseExchangeRule(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.optimize.union.reuse.exchange.rule",
      spark.sparkContext.getConf.get("spark.sql.optimize.union.reuse.exchange.rule",
        "false")).toBoolean
  }

  def validScenariosForOptPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case LocalRelation(_, _, _)
           |  LogicalRelation(_, _, _, _)
           |  HiveTableRelation(_, _, _, _, _) => true
      case dsv2: DataSourceV2ScanRelation => true
      case Project(_, LocalRelation(_, _, _))
           |  Project(_, LogicalRelation(_, _, _, _))
           |  Project(_, HiveTableRelation(_, _, _, _, _)) => true
      case Project(_, dsv2: DataSourceV2ScanRelation) => true
      case _ => false
    }
  }

  private def compareChildForUnionOpt(ltPlan: LogicalPlan, rtPlan: LogicalPlan): Boolean = {
    var optFlag = false
    if (ltPlan.isInstanceOf[Aggregate] && rtPlan.isInstanceOf[Aggregate]) {
      val leftAggr = ltPlan.asInstanceOf[Aggregate]
      val rightAggr = rtPlan.asInstanceOf[Aggregate]
      val utils = new SparkSqlUtils()
      if (utils.checkSameRelForOptPlan(leftAggr.child, rightAggr.child)) {
        optFlag = validateGroupingExpr(leftAggr.groupingExpressions,
          rightAggr.groupingExpressions)
      }
    }
    optFlag
  }

  private def validateGroupingExpr(leftExpr: Seq[Expression], rightExpr: Seq[Expression]): Boolean = {
    val leftExprAttrs = leftExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    val rightExprAttrs = rightExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    leftExprAttrs == rightExprAttrs
  }
}
