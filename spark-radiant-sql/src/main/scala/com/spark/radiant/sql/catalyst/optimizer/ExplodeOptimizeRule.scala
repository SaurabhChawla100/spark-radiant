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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter,
  Generate, LogicalPlan, Repartition, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions.Explode

/**
 * ExplodeOptimizeRule - This optimizer rule works for scenarios where the Explode
 * is present with aggregation, So there will be exchange after partial aggregation
 * and there are scenarios where cost of partial aggregate + exchange is high.
 * In those scenarios its better to have exchange first and than apply both
 * partial aggregate and complete aggregate on the exchange.
 *
 */
object ExplodeOptimizeRule extends Rule[LogicalPlan] with LazyLogging {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useExplodeOptRule(spark)) {
      try {
        val repartitionExist = plan.find {
          case Repartition(_, _, _) | RepartitionByExpression(_, _, _) => true
          case _ => false
        }
        if (repartitionExist.isEmpty) {
          val updatedPlan = plan.transform {
            case gen: Generate if gen.generator.isInstanceOf[Explode] =>
              gen.copy(child = applyExplodeOptRule(gen.child))
          }
          updatedPlan
        } else {
          plan
        }
      } catch {
        case ex: AnalysisException =>
          logger.debug(s"exception on applying ExplodeOptimizeRule: ${ex}")
          plan
      }
    }
    else {
      plan
    }
  }

  private def useExplodeOptRule(spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.optimize.explode.rule",
      spark.sparkContext.getConf.get("spark.sql.optimize.explode.rule",
        "false")).toBoolean
  }

  private def applyExplodeOptRule(plan: LogicalPlan): LogicalPlan = {
   val updatedPlan = plan.transform {
      case filter @ Filter(_, agg@Aggregate(_, _, _))
        if !agg.child.isInstanceOf[RepartitionByExpression] =>
        val updatedAgg = agg.copy(child = RepartitionByExpression(
          agg.groupingExpressions, agg.child, None))
        filter.copy(child = updatedAgg)
    }
    updatedPlan
  }
}
