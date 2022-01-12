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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join,
  LogicalPlan, Repartition, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * ExchangeOptimizeRule - This optimizer rule works for scenarios where partial
 * aggregate exchange is present and also the exchange which is introduced by
 * SMJ and other join that add the shuffle exchange, So in total there are 2 exchange
 * present in the executed plan and cost of creating both exchange are almost same.
 * In that scenario we skip the exchange created by the partial aggregate and there will be
 * only one exchange and partial and complete aggregation done on the same exchange, now
 * instead of having 2 exchange there will be only one exchange.
 *
 */
object ExchangeOptimizeRule extends Rule[LogicalPlan] with LazyLogging {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useExchangeOptRule(spark)) {
      try {
        // check if already the plan is not having the repartition than only
        // add this optimization.
        val repartitionExist = plan.find {
          case Repartition(_, _, _) | RepartitionByExpression(_, _, _) => true
          case _ => false
        }
        if (repartitionExist.isEmpty) {
          // TODO Add the intelligence in the rule for predicting scenarios where
          //  this can be applied instead of all the aggregate present in join
          val updatedPlan = plan.transform {
            case join: Join if validJoinForExchangeOptRule(join.joinType) =>
              var joinCondition: Option[Expression] = None
              joinCondition = join.condition
              val bhjThreshold: Long = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
                .split("b").head.toLong
              val bhjPresent = (join.left.stats.sizeInBytes <= bhjThreshold
                || join.right.stats.sizeInBytes <= bhjThreshold)
              val condRef = join.condition.get.references
              if ((validForExchangeOptRule(join.left, condRef) ||
                validForExchangeOptRule(join.right, condRef)) && !bhjPresent) {
                val newLeft = getExchangeOptPlan(join.left, condRef, condRef.head)
                val newRight = getExchangeOptPlan(join.right, condRef, condRef.tail.head)
                join.copy(left = newLeft, right = newRight)
              } else {
                join
              }
          }
          log.debug(s"updatedPlan after ExchangeOptimizeRule: $updatedPlan")
          updatedPlan
        } else {
          plan
        }
      } catch {
        case ex: AnalysisException =>
          logger.debug(s"exception in ExchangeOptimizeRule optimizer rule : $ex")
          plan
      }
    } else {
      plan
    }
  }

  private def getExchangeOptPlan(logicalPlan: LogicalPlan,
     ref: AttributeSet,
     attr: Attribute): LogicalPlan = {
    logicalPlan match {
      case agg: Aggregate if !agg.child.isInstanceOf[RepartitionByExpression] =>
        val groupingExpr = agg.groupingExpressions
        var repartitionCol = groupingExpr.map(expr =>
          expr.asInstanceOf[NamedExpression]).filter(x => ref.contains(x))
        if (repartitionCol.isEmpty) {
          repartitionCol = Seq(attr)
        }
        agg.copy(child = RepartitionByExpression(repartitionCol, agg.child, None))
      case _ => logicalPlan
    }
  }

  private def validForExchangeOptRule(logicalPlan: LogicalPlan, ref: AttributeSet): Boolean = {
    logicalPlan match {
      case agg: Aggregate =>
        !agg.child.isInstanceOf[RepartitionByExpression] &&
          agg.groupingExpressions.length > 1 &&
          agg.groupingExpressions.length > agg.groupingExpressions.map(expr =>
            expr.asInstanceOf[NamedExpression]).count(x => ref.contains(x))
      case _ => false
    }
  }

  private def validJoinForExchangeOptRule(joinType: JoinType): Boolean = {
    joinType match {
      case Inner => true
      case _ => false
    }
  }

  private def useExchangeOptRule(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.skip.partial.exchange.rule",
      spark.sparkContext.getConf.get("spark.sql.skip.partial.exchange.rule",
        "false")).toBoolean
  }
}
