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

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Or}
import org.apache.spark.sql.{CustomFilter, CustomProject, SparkSession}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join,
  LocalRelation, LogicalPlan, Project, Repartition, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import com.spark.radiant.sql.utils.SparkSqlUtils

/**
 *
 * JoinReuseExchangeOptimizeRule works for scenario where
 * there is join between same table and the scan of table happens multiple times.
 * After Applying this rule File Scan will take place once. This is enabled by the
 * conf --conf spark.sql.optimize.join.reuse.exchange.rule=true
 */
object JoinReuseExchangeOptimizeRule extends Rule[LogicalPlan] with LazyLogging {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useJoinReuseExchangeRule(spark)) {
      try {
        val repartitionExist = plan.find {
          case _: Repartition => true
          case _: RepartitionByExpression => true
          case _ => false
        }
        if (repartitionExist.isEmpty) {
          val updatedPlan = plan.transform {
            case join: Join if join.joinType == Inner =>
              val bhjThreshold: Long = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
                .split("b").head.toLong
              // if BHJ is present, If BHJ not present than run this rule
              val bhjPresent = (join.left.stats.sizeInBytes <= bhjThreshold
                || join.right.stats.sizeInBytes <= bhjThreshold)
              val left = join.left
              val right = join.right
              val leftFilter = genPlanForFilter(left)
              val rightFilter = genPlanForFilter(right)
              if (!bhjPresent && leftFilter.isDefined && rightFilter.isDefined
                && compareChildForJoinOpt(left, right)) {
                join.copy(left = getUpdateChildPlanForJoin(left,
                  rightFilter.get.asInstanceOf[Filter].condition, spark),
                  right = getUpdateChildPlanForJoin(right,
                    leftFilter.get.asInstanceOf[Filter].condition, spark, true))
              } else {
                join
              }
          }
          logger.debug(s"Plan after applying JoinReuseExchangeOptimizeRule::$updatedPlan")
          updatedPlan
        } else {
          plan
        }
      } catch {
        case ex: Throwable =>
          logger.debug(s"exception on applying JoinReuseExchangeOptimizeRule: ${ex}")
          plan
      }
    } else {
      plan
    }
  }

  private def compareChildForJoinOpt(ltPlan: LogicalPlan, rtPlan: LogicalPlan): Boolean = {
    var optFlag = false
    ltPlan match {
      case leftAggr: Aggregate if rtPlan.isInstanceOf[Aggregate] =>
        val rightAggr = rtPlan.asInstanceOf[Aggregate]
        if (checkSameRelForOptPlan(leftAggr.child, rightAggr.child)) {
          optFlag = validateGroupingExpr(leftAggr.groupingExpressions,
            rightAggr.groupingExpressions)
        }
      case _ =>
    }
    optFlag
  }


  private def checkSameRelForOptPlan(leftPlan: LogicalPlan,
     rightPlan: LogicalPlan): Boolean = {
    var optFlag = false
    leftPlan match {
      case Project(_, localRel@LocalRelation(_, _, _))
        if rightPlan.isInstanceOf[Project]
          && rightPlan.asInstanceOf[Project].child.isInstanceOf[LocalRelation] =>
        optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[Project].child
          .asInstanceOf[LocalRelation].schema.fieldNames.toSet)
      case Project(_, Filter(_, localRel@LocalRelation(_, _, _)))
        if rightPlan.isInstanceOf[Project]
          && rightPlan.asInstanceOf[Project].isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
          .isInstanceOf[LocalRelation] =>
        optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter]
          .child.asInstanceOf[LocalRelation].schema.fieldNames.toSet)
      case Project(_, Filter(_, localRel@LocalRelation(_, _, _)))
        if rightPlan.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Filter].child.isInstanceOf[LocalRelation] =>
        optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[Filter]
          .child.asInstanceOf[LocalRelation].schema.fieldNames.toSet)
      case Filter(_, localRel@LocalRelation(_, _, _))
        if rightPlan.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Filter].child.isInstanceOf[LocalRelation] =>
        optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[Filter].child
          .asInstanceOf[LocalRelation].schema.fieldNames.toSet)
      case localRel@LocalRelation(_, _, _)
        if rightPlan.isInstanceOf[LocalRelation] =>
        optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[LocalRelation].schema.fieldNames.toSet)
      case Project(_, Filter(_, logRel@LogicalRelation(_, _, _, _)))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Project]
          .child.asInstanceOf[Filter].child.isInstanceOf[LogicalRelation] =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
          .asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case Project(_, Filter(_, logRel@LogicalRelation(_, _, _, _)))
        if rightPlan.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Filter].child.isInstanceOf[LogicalRelation] =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[Filter].child
          .asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case Filter(_, logRel@LogicalRelation(_, _, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter]
          .child.isInstanceOf[LogicalRelation] =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
          .asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case Filter(_, logRel@LogicalRelation(_, _, _, _))
        if rightPlan.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Filter].child.isInstanceOf[LogicalRelation] =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[Filter].child
          .asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case Project(_, logRel@LogicalRelation(_, _, _, _))
        if rightPlan.asInstanceOf[Project].child.isInstanceOf[LogicalRelation] =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[Project].child.asInstanceOf[LogicalRelation].
          relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case logRel@LogicalRelation(_, _, _, _) =>
        val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
        val rightFiles = rightPlan.asInstanceOf[LogicalRelation].
          relation.asInstanceOf[HadoopFsRelation].inputFiles
        optFlag = leftFiles.toSet == rightFiles.toSet
      case Project(_, hiveRel@HiveTableRelation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[HiveTableRelation].tableMeta
      case Project(_, Filter(_, hiveRel@HiveTableRelation(_, _, _, _, _)))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[Project].child
          .asInstanceOf[Filter].child.asInstanceOf[HiveTableRelation].tableMeta
      case Project(_, Filter(_, hiveRel@HiveTableRelation(_, _, _, _, _)))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child
            .isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[Filter]
          .child.asInstanceOf[HiveTableRelation].tableMeta
      case Filter(_, hiveRel@HiveTableRelation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child
            .asInstanceOf[Filter].child.isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[Project].child
          .asInstanceOf[Filter].child.asInstanceOf[HiveTableRelation].tableMeta
      case Filter(_, hiveRel@HiveTableRelation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child.isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[Filter]
          .child.asInstanceOf[HiveTableRelation].tableMeta
      case hiveRel@HiveTableRelation(_, _, _, _, _)
        if rightPlan.isInstanceOf[HiveTableRelation] =>
        optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[HiveTableRelation].tableMeta
      case Project(_, Filter(_, dsv2@DataSourceV2ScanRelation(_, _, _)))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case Project(_, Filter(_, dsv2@DataSourceV2ScanRelation(_, _, _)))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child
            .isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case Project(_, dsv2@DataSourceV2ScanRelation(_, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[Project]
            .child.asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case Filter(_, dsv2@DataSourceV2ScanRelation(_, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter]
            .child.isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case Filter(_, dsv2@DataSourceV2ScanRelation(_, _, _))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child.isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case dsv2@DataSourceV2ScanRelation(_, _, _)
        if rightPlan.isInstanceOf[DataSourceV2ScanRelation] =>
        optFlag = dsv2.relation.table.name() ==
          rightPlan.asInstanceOf[DataSourceV2ScanRelation].relation.table.name()
      case Project(_, Filter(_, dsv2@DataSourceV2Relation(_, _, _, _, _)))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2Relation].table
      case Project(_, Filter(_, dsv2@DataSourceV2Relation(_, _, _, _, _)))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child
            .isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2Relation].table
      case Project(_, dsv2@DataSourceV2Relation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[Project].child.asInstanceOf[DataSourceV2Relation].table
      case Filter(_, dsv2@DataSourceV2Relation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Project] &&
          rightPlan.asInstanceOf[Project].child.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter]
            .child.isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[Project].child.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2Relation].table
      case Filter(_, dsv2@DataSourceV2Relation(_, _, _, _, _))
        if rightPlan.isInstanceOf[Filter] &&
          rightPlan.asInstanceOf[Filter].child.isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[Filter].child
            .asInstanceOf[DataSourceV2Relation].table
      case dsv2@DataSourceV2Relation(_, _, _, _, _)
        if rightPlan.isInstanceOf[DataSourceV2Relation] =>
        optFlag = dsv2.table ==
          rightPlan.asInstanceOf[DataSourceV2Relation].table
      case _ =>
    }
    optFlag
  }


  private def getUpdateChildPlanForJoin(plan: LogicalPlan,
     filter: Expression,
     spark: SparkSession,
     preAppendCondition: Boolean = false): LogicalPlan = {
    plan match {
      case agg@Aggregate(_, _, child) if validScenariosForOptPlan(child) =>
        genPlanForAggregate(agg, filter, spark, preAppendCondition)
      case _ => plan
    }
  }

  private def genPlanForFilter(plan: LogicalPlan): Option[LogicalPlan] = {
    plan match {
      case agg: Aggregate if !agg.child.isInstanceOf[RepartitionByExpression] =>
        agg.child match {
          case Project(_, filter@Filter(_, _)) => Some(filter)
          case filter@Filter(_, _) if validScenariosForOptPlan(filter) => Some(filter)
          case _ => None
        }
      case _ => None
    }
  }

  private def genPlanForAggregate(plan: LogicalPlan,
     cond: Expression,
     spark: SparkSession,
     preAppendCond: Boolean): LogicalPlan = {
    plan match {
      case agg: Aggregate if !agg.child.isInstanceOf[RepartitionByExpression] =>
        val aggChildUpdated = agg.child match {
          case Project(attr, filter@Filter(_, _)) =>
            val proj = if (useCustomProject(spark)) {
              CustomProject(attr, addCustomFilterToPan(filter, spark, cond,
              agg.groupingExpressions, preAppendCond))
            } else {
              Project(attr, addCustomFilterToPan(filter, spark, cond,
                agg.groupingExpressions, preAppendCond))
            }
            proj
          case filter@Filter(_, _) =>
            addCustomFilterToPan(filter, spark, cond, agg.groupingExpressions, preAppendCond)
          case _ => agg
        }
        agg.copy(child = aggChildUpdated)
      case _ => plan
    }
  }

  private def useJoinReuseExchangeRule(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.optimize.join.reuse.exchange.rule",
      spark.sparkContext.getConf.get("spark.sql.optimize.join.reuse.exchange.rule",
        "false")).toBoolean
  }

  private def useCustomProject(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.radiant.use.custom.project",
      spark.sparkContext.getConf.get("spark.sql.radiant.use.custom.project",
        "true")).toBoolean
  }

  private def validateGroupingExpr(leftExpr: Seq[Expression],
     rightExpr: Seq[Expression]): Boolean = {
    val leftExprAttrs = leftExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    val rightExprAttrs = rightExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    leftExprAttrs == rightExprAttrs
  }

  private def addCustomFilterToPan(plan: LogicalPlan,
     spark: SparkSession,
     cond: Expression,
     groupingExpr: Seq[Expression],
     preAppendCond: Boolean): LogicalPlan = {
    val filter = plan.asInstanceOf[Filter]
    val util = new SparkSqlUtils()
    val regexPattern = "[0-9, a-z, A-Z, _, -]*\\."
    val df = util.createDfFromLogicalPlan(spark, filter.child)
    val updatedCond = df.filter(cond.sql.replaceAll(regexPattern, ""))
      .queryExecution.optimizedPlan.asInstanceOf[Filter].condition
    val oldCond = df.filter(filter.condition.sql.replaceAll(regexPattern, ""))
      .queryExecution.optimizedPlan.asInstanceOf[Filter].condition
    if (preAppendCond) {
      val filterCond = Filter(Or(updatedCond, oldCond), filter.child)
      CustomFilter(filter.condition, RepartitionByExpression(groupingExpr, filterCond, None))
    } else {
      val filterCond = Filter(Or(oldCond, updatedCond), filter.child)
      CustomFilter(filter.condition, RepartitionByExpression(groupingExpr, filterCond, None))
    }
  }


  def validScenariosForOptPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case LocalRelation(_, _, _)
           | LogicalRelation(_, _, _, _)
           | HiveTableRelation(_, _, _, _, _)
           | DataSourceV2ScanRelation(_, _, _)
           | DataSourceV2Relation(_, _, _, _, _) => true
      case Project(_, LocalRelation(_, _, _))
           | Project(_, LogicalRelation(_, _, _, _))
           | Project(_, HiveTableRelation(_, _, _, _, _))
           | Project(_, DataSourceV2ScanRelation(_, _, _))
           | Project(_, DataSourceV2Relation(_, _, _, _, _)) => true
      case Project(_, Filter(_, LocalRelation(_, _, _)))
           | Project(_, Filter(_, LogicalRelation(_, _, _, _)))
           | Project(_, Filter(_, HiveTableRelation(_, _, _, _, _)))
           | Project(_, Filter(_, DataSourceV2ScanRelation(_, _, _)))
           | Project(_, Filter(_, DataSourceV2Relation(_, _, _, _, _))) => true
      case Filter(_, LocalRelation(_, _, _))
           | Filter(_, LogicalRelation(_, _, _, _))
           | Filter(_, HiveTableRelation(_, _, _, _, _))
           | Filter(_, DataSourceV2ScanRelation(_, _, _))
           | Filter(_, DataSourceV2Relation(_, _, _, _, _)) => true
      case _ => false
    }
  }
}
