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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference,
  ConcatWs, EqualTo, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, TypedFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{col, concat_ws, lit, md5, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

private[sql] class SparkSqlDFOptimizerRule extends Logging with Serializable {
  val bloomFilterKey = "dynamicFilterBloomFilterKey"
  val fpp = 0.02

  private def combineExpression(sep: String, exprs: Column*): Column = {
    val u = ConcatWs(Literal.create(sep, StringType) +: exprs.map(_.expr))
    new org.apache.spark.sql.Column(u)
  }

  private def getSplittedByAndPredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        getSplittedByAndPredicates(cond1) ++ getSplittedByAndPredicates(cond2)
      case other => other :: Nil
    }
  }

  private def getBloomFilterKeyColumn(columns: List[Expression]): Expression = {
    md5(combineExpression("~#~", columns.map(c => new org.apache.spark.sql.Column(c)): _*)).expr
  }

  /**
   *
   * @param df - dateframe for which bloom filter is needed
   * @param columns - list of column that can be combined to create key
   * @param bloomFilterKeyApp - BloomFilter key name
   * @return
   */
  private def createBloomFilterKeyColumn(df: DataFrame,
     columns: List[Expression],
     bloomFilterKeyApp: String): DataFrame = {
    df.withColumn(bloomFilterKeyApp, md5(combineExpression("~#~",
      columns.map(c => new org.apache.spark.sql.Column(c)): _*)))
  }

  private def createDfFromLogicalPlan(spark: SparkSession,
     logicalPlan: LogicalPlan): DataFrame = {
    // scalastyle:off
    val cls = Class.forName("org.apache.spark.sql.Dataset")
    val method = cls.getMethod("ofRows", classOf[SparkSession], classOf[LogicalPlan])
    method.invoke(cls, spark, logicalPlan).asInstanceOf[Dataset[_]].toDF
  }

  private def getPlanFromJoinCondition(spark: SparkSession,
    bloomFilterKeyAppend: String,
    dfRight: DataFrame,
    leftPlan: LogicalPlan,
    rightOutput: Seq[Attribute],
    bloomFilterCount: Long,
    joinAttr: List[(Expression, Expression)]): LogicalPlan = {

    var leftFilter: Seq[Expression] = Seq.empty
    var rightFilter: Seq[Expression] = Seq.empty

    val exprOut = leftPlan match {
      case Filter(_, LocalRelation(output, _ ,_)) => output.map(_.exprId)
      case Filter(_, LogicalRelation(_, output ,_, _)) => output.map(_.exprId)
      case _ => leftPlan.output.map(_.exprId)
    }

    joinAttr.foreach { attr =>
      val p = attr._1.asInstanceOf[AttributeReference].exprId
      if (exprOut.contains(p)) {
        leftFilter = leftFilter :+ attr._1
        rightFilter = rightFilter :+ attr._2
      }
    }
    if (rightFilter.nonEmpty && rightFilter.length == leftFilter.length) {

      // TODO getting the task serialization exception on using Project,
      //  use better way instead of creating DF
      /* @transient val bloomFilterKeyExpr = getBloomFilterKeyColumn(rightFilter.toList)
      @transient val rightExpr = rightOutput ++ (Alias(bloomFilterKeyExpr,
       "bloomFilterKey")() :: Nil)
      @transient def dfr1 = createDfFromLogicalPlan(spark,
       Project(rightExpr, child = rightPlan.clone())) */
      val dfr = createBloomFilterKeyColumn(dfRight, rightFilter.toList, bloomFilterKeyAppend )
      val bloomFilter = dfr.stat.bloomFilter(bloomFilterKeyAppend, bloomFilterCount, fpp)
      val broadcastValue = spark.sparkContext.broadcast(bloomFilter)

      // TODO getting the task serialization exception on using Project,
      //  use better way instead of creating DF
      /* val leftBloomFilterKeyExpr = getBloomFilterKeyColumn(leftFilter.toList)
      val leftPlanExpr = leftPlan.output ++ (Alias(leftBloomFilterKeyExpr,
       "bloomFilterKey")() :: Nil)
      val expre = leftPlanExpr.filter(x => x.name== bloomFilterKey).head
      val newPlan = Filter(MightContainInBloomFilter(expre,
       broadcastValue.value),Project(leftPlanExpr, logicalRelation))
      dfl = createDfFromLogicalPlan(spark, newPlan) */
      var dfl = createDfFromLogicalPlan(spark, leftPlan)
      dfl = createBloomFilterKeyColumn(dfl, leftFilter.toList, bloomFilterKeyAppend)
      val dfl1 = dfl.filter { x =>
        broadcastValue.value.mightContain(x.getAs(bloomFilterKeyAppend))
      }
      dfl1.drop(dfl1(bloomFilterKeyAppend)).queryExecution.optimizedPlan
    } else {
      leftPlan
    }
  }

  private def getLeftOptimizedPlan(spark: SparkSession,
     leftPlan: LogicalPlan,
     rightPlan: LogicalPlan,
     rightOutput: Seq[Attribute],
     bloomFilterCount: Long,
     joinAttr: List[(Expression, Expression)]): LogicalPlan = {

    val dfr = createDfFromLogicalPlan(spark, rightPlan)
    var hold = false
    val bloomFilterKeyAppender = s"${bloomFilterKey}${joinAttr.map(x => x._2).mkString("$$")}"
    val updatedLeftPlan = leftPlan.transform {
      case typeFilter: TypedFilter =>
        val schemaStruct = typeFilter.schema
        val b = typeFilter.output.map(_.exprId)
        val u = joinAttr.filter {
          attr => b.contains(attr._1.asInstanceOf[AttributeReference].exprId)
        }
        if (schemaStruct.fieldNames.contains(bloomFilterKeyAppender) &&
          u.length!=0) {
          // TODO add better logic for closing the recursion
          hold = true
        }
        typeFilter
      case filter @ Filter(_, LocalRelation(_, _, _)) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          filter, rightOutput, bloomFilterCount, joinAttr)
      case filter @ Filter(_, LogicalRelation(_, _, _, _))  if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender,dfr, filter,
          rightOutput, bloomFilterCount, joinAttr)
      case localTableScan: LocalRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender,dfr,
          localTableScan, rightOutput, bloomFilterCount, joinAttr)
      case logicalRelation: LogicalRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender,dfr,
          logicalRelation, rightOutput, bloomFilterCount, joinAttr)
    }
    logDebug("optimized updatedLeftPlan::" + updatedLeftPlan)
    updatedLeftPlan
  }

  private def getOptimizedLogicalPlan(spark: SparkSession,
     plan: LogicalPlan,
     bloomFilterCount: Long): LogicalPlan = {
    logDebug("Initial plan: "+ plan)
    val updatedPlan = plan.transform {
      case join: Join if join.joinType == Inner =>
        var joinCondition: Option[Expression] = None
        joinCondition = join.condition
        val bhjThreshold: Long = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
          .split("b").head.toLong
        val bhjPresent = (join.left.stats.sizeInBytes <= bhjThreshold
          || join.right.stats.sizeInBytes <= bhjThreshold)

        // if bhjPresent is not present than only add this logic
        if (!bhjPresent && joinCondition.isDefined
          && joinCondition.get.find(_.isInstanceOf[Or]).isEmpty) {
          val predicates = joinCondition.map(getSplittedByAndPredicates).getOrElse(Nil)
          val joinKeys = predicates.filter {
            case EqualTo(_, _) => true
            case _ => false
          }
          val joinAttr = joinKeys.map(x =>
            (x.asInstanceOf[EqualTo].left,x.asInstanceOf[EqualTo].right)).toList
          if (joinAttr.isEmpty) {
            join
          } else {
            val ltPlan = getLeftOptimizedPlan(spark, join.left,
              join.right, join.right.output, bloomFilterCount, joinAttr)
            val updatedJoin = Join(ltPlan, join.right, Inner, join.condition, join.hint)
            logDebug(s"updatedJoin after applying DF :: ${updatedJoin}")
            updatedJoin
          }
        } else {
          join
        }
    }
    logDebug(s"updatedPlan after applying DF : ${updatedPlan}")
    updatedPlan
  }

  def pushFilterBelowTypedFilterRule(plan: LogicalPlan) : LogicalPlan = {
    plan.transform {
      case filter: Filter =>
        val updatedFilterPlan = filter.child match {
          case typeFilter @ TypedFilter(x ,y , z, u, _) =>
            val newFilter = Filter(filter.condition, typeFilter.child)
            TypedFilter(x, y, z, u, newFilter)
          case _ => filter
        }
        updatedFilterPlan
    }
  }

  def addDynamicFiltersPlan(spark: SparkSession,
    plan: LogicalPlan,
    bloomFilterCount: Long): LogicalPlan = {
    try {
      val updatedPlan = getOptimizedLogicalPlan(spark, plan, bloomFilterCount)
      updatedPlan
    }
    catch {
      case ex : Throwable => logDebug(s"exception while creating th DF: ${ex}")
        throw ex
    }
  }
}
