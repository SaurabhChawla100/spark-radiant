package com.spark.radiant.sql.catalyst.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, RepartitionByExpression}
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
object ExchangeOptimizeRule extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    if (useExchangeOptRule(spark)) {
      try {
        // TODO Add the intelligence in the rule for predicting secnarios where
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
        logDebug(s"updatedPlan after ExchangeOptimizeRule: $updatedPlan")
        updatedPlan
      } catch {
        case ex: AnalysisException =>
          logDebug(s"exception in ExchangeOptimizeRule optimizer rule : $ex")
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
