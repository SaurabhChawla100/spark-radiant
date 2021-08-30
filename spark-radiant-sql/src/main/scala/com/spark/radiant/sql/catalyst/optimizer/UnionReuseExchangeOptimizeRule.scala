package com.spark.radiant.sql.catalyst.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation,
  LogicalPlan, RepartitionByExpression, Union}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * UnionReuseExchangeOptimizeRule - This rule works for scenario when union is present with
 * aggregation having same grouping column and the union is between the same table/datasource .
 * In this scenario instead of scan twice the table/datasource because of the child of Union,
 * There will be one scan and the other child of union will reuse this scan.This feature is
 * enabled using --conf spark.sql.optimize.union.reuse.exchange.rule=true
 */
object UnionReuseExchangeOptimizeRule extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useUnionReuseExchangeRule(spark)) {
      try {
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
      } catch {
        case ex: AnalysisException =>
          logDebug(s"exception on applying UnionReuseExchangeOptimizeRule: ${ex}")
          plan
      }
    } else {
      plan
    }
  }

  def getUpdateChildPlanForUnion(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case agg@Aggregate(_, _, child) if validScenariosForOptPlan(child) =>
        genPlanForAggregate(agg)
      case _ => plan
    }
  }

  def genPlanForAggregate(plan: LogicalPlan): LogicalPlan = {
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
      case _ => false
    }
  }

  def compareChildForUnionOpt(ltPlan: LogicalPlan, rtPlan: LogicalPlan): Boolean = {
    var optFlag = false
    if (ltPlan.isInstanceOf[Aggregate] && rtPlan.isInstanceOf[Aggregate]) {
      val leftAggr = ltPlan.asInstanceOf[Aggregate]
      val rightAggr = rtPlan.asInstanceOf[Aggregate]
      if (checkSameRelForOptPlan(leftAggr.child, rightAggr.child)) {
        optFlag = validateGroupingExpr(leftAggr.groupingExpressions,
          rightAggr.groupingExpressions)
      }
    }
    optFlag
  }

  def checkSameRelForOptPlan(leftPlan: LogicalPlan, rightPlan: LogicalPlan): Boolean = {
    var optFlag = false
    if (leftPlan.getClass.equals(rightPlan.getClass)) {
      leftPlan match {
        case localRel@LocalRelation(_, _, _) =>
          optFlag = (localRel.schema.fieldNames.toSet
          == rightPlan.asInstanceOf[LocalRelation].schema.fieldNames.toSet)
        case logRel@LogicalRelation(_, _, _, _) =>
          val leftFiles = logRel.relation.asInstanceOf[HadoopFsRelation].inputFiles
          val rightFiles = rightPlan.asInstanceOf[LogicalRelation].
            relation.asInstanceOf[HadoopFsRelation].inputFiles
          optFlag = leftFiles.toSet == rightFiles.toSet
        case hiveRel@HiveTableRelation(_, _, _, _, _) =>
          optFlag = hiveRel.tableMeta == rightPlan.asInstanceOf[HiveTableRelation].tableMeta
        case _ =>
      }
    }
    optFlag
  }

  def validateGroupingExpr(leftExpr: Seq[Expression], rightExpr: Seq[Expression]): Boolean = {
    val leftExprAttrs = leftExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    val rightExprAttrs = rightExpr.map(x => x.asInstanceOf[AttributeReference].name).toSet
    leftExprAttrs == rightExprAttrs
  }
}
