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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference,
  ConcatWs, EqualTo, Expression, In, Literal, Or}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter,
  LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, TypedFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{col, concat_ws, lit, md5}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

private[sql] class SparkSqlDFOptimizerRule extends Logging with Serializable {
  val bloomFilterKey = "dynamicFilterBloomFilterKey"
  val fpp = 0.02
  val dfKeySeparator = "~#~"

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
    md5(combineExpression(dfKeySeparator,
      columns.map(c => new org.apache.spark.sql.Column(c)): _*)).expr
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
    df.withColumn(bloomFilterKeyApp, md5(combineExpression(dfKeySeparator,
      columns.map(c => new org.apache.spark.sql.Column(c)): _*)))
  }

  private def createDfFromLogicalPlan(spark: SparkSession,
     logicalPlan: LogicalPlan): DataFrame = {
    // scalastyle:off
    val cls = Class.forName("org.apache.spark.sql.Dataset")
    val method = cls.getMethod("ofRows", classOf[SparkSession], classOf[LogicalPlan])
    method.invoke(cls, spark, logicalPlan).asInstanceOf[Dataset[_]].toDF
  }

  /**
   *
   * @param spark - existing spark Session
   * @param bloomFilterAppendedKey - Column name for BloomFilter key in Dynamic Filter
   * @param DataframeForGenDf - Data frame used for generating the values needed
   *                          for Dynamic filter
   * @param planForDf - Bigger table plan where the Dynamic Filter is added
   * @param outputForGenDF - Output of smaller table for generating the Dynamic Filter
   * @param bloomFilterCount - size of bloom filter
   * @param joinAttr - Attributes present in the join condition
   * @return - Return optimized logical plan having Dynamic Filter
   */
  private def getPlanFromJoinCondition(spark: SparkSession,
     bloomFilterAppendedKey: String,
     DataframeForGenDf: DataFrame,
     planForDf: LogicalPlan,
     outputForGenDF: Seq[Attribute],
     bloomFilterCount: Long,
     joinAttr: List[(Expression, Expression)]): LogicalPlan = {

    var filterNeededForDF: Seq[Expression] = Seq.empty
    var filterToCreateDF: Seq[Expression] = Seq.empty

    val exprOut = planForDf match {
      case Filter(_, LocalRelation(output, _ ,_)) => output.map(_.exprId)
      case Filter(_, LogicalRelation(_, output ,_, _)) => output.map(_.exprId)
      case Filter(_, HiveTableRelation(_, output ,_, _,_)) => output.map(_.exprId)
      case Filter(_, InMemoryRelation(output, _, _)) => output.map(_.exprId)
      case _ => planForDf.output.map(_.exprId)
    }
    joinAttr.foreach { attr =>
      val exprId1 = attr._1.asInstanceOf[AttributeReference].exprId
      val exprId2 = attr._2.asInstanceOf[AttributeReference].exprId
      if (exprOut.contains(exprId1)) {
        filterNeededForDF = filterNeededForDF :+ attr._1
        filterToCreateDF = filterToCreateDF :+ attr._2
      } else if (exprOut.contains(exprId2)) {
        filterNeededForDF = filterNeededForDF :+ attr._2
        filterToCreateDF = filterToCreateDF :+ attr._1
      }
    }
    if (filterToCreateDF.nonEmpty && filterToCreateDF.length == filterNeededForDF.length) {

      // TODO getting the task serialization exception on using Project,
      //  use better way instead of creating DF
      /* @transient val bloomFilterKeyExpr = getBloomFilterKeyColumn(rightFilter.toList)
      @transient val rightExpr = rightOutput ++ (Alias(bloomFilterKeyExpr,
       "bloomFilterKey")() :: Nil)
      @transient def dfr1 = createDfFromLogicalPlan(spark,
       Project(rightExpr, child = rightPlan.clone())) */

      val thresholdPushDownLength = spark.sparkContext.getConf.getInt(
        "spark.sql.dynamicFilter.pushdown.threshold", 5000)
      val dfr = createBloomFilterKeyColumn(DataframeForGenDf, filterToCreateDF.toList, bloomFilterAppendedKey)
      val rightFilterKey = filterToCreateDF.head.asInstanceOf[AttributeReference]
      val bloomFilterDfr = dfr.select(rightFilterKey.name).limit(thresholdPushDownLength+1).collect()
      val pushDownFileScanValues = if (bloomFilterDfr.length <= thresholdPushDownLength) {
        bloomFilterDfr.map(x => Literal(x.get(0)))
      } else {
        null
      }
      val bloomFilter = dfr.stat.bloomFilter(bloomFilterAppendedKey, bloomFilterCount, fpp)
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

      var dfl: DataFrame = createDfFromLogicalPlan(spark, planForDf)
      dfl = createBloomFilterKeyColumn(dfl, filterNeededForDF.toList, bloomFilterAppendedKey)
      if (pushDownFileScanValues != null) {
        var leftDynamicFilterPlan = dfl.queryExecution.optimizedPlan
        leftDynamicFilterPlan =
          Filter(In(filterNeededForDF.head, pushDownFileScanValues), leftDynamicFilterPlan)
        dfl = createDfFromLogicalPlan(spark, leftDynamicFilterPlan)
      }
      val dfl1 = dfl.filter { x =>
        broadcastValue.value.mightContain(x.getAs(bloomFilterAppendedKey))
      }
      dfl1.drop(dfl1(bloomFilterAppendedKey)).queryExecution.optimizedPlan
    } else {
      planForDf
    }
  }

  /**
   *
   * @param spark - existing spark Session
   * @param planForDF - Bigger table plan where the Dynamic Filter is needed
   * @param predicatesPlanUsedInDF - smaller table which is used for creating
   *                               Dynamic Filter
   * @param predicateOutputInDF - Output of smaller table for generating the
   *                            Dynamic Filter
   * @param bloomFilterCount - size of bloom filter
   * @param joinAttr - Attributes present in the join condition
   * @return
   */
  private def getDynamicFilteredPlan(spark: SparkSession,
     planForDF: LogicalPlan,
     predicatesPlanUsedInDF: LogicalPlan,
     predicateOutputInDF: Seq[Attribute],
     bloomFilterCount: Long,
     joinAttr: List[(Expression, Expression)]): LogicalPlan = {

    var updatedJoinAttr: List[(Expression, Expression)] = List.empty

    val exprOut = planForDF match {
      case _ => planForDF.output.map(_.exprId)
    }
    joinAttr.foreach { attr =>
      val exprId1 = attr._1.asInstanceOf[AttributeReference].exprId
      val exprId2 = attr._2.asInstanceOf[AttributeReference].exprId
      if (exprOut.contains(exprId1)) {
        updatedJoinAttr = updatedJoinAttr :+ (attr._1, attr._2)
      } else if (exprOut.contains(exprId2)) {
        updatedJoinAttr = updatedJoinAttr :+ (attr._2, attr._1)
      }
    }
    val bloomFilterKeyAppender = s"${bloomFilterKey}${updatedJoinAttr.map(x => x._2).mkString("$$")}"
    val dfr = createDfFromLogicalPlan(spark, predicatesPlanUsedInDF)
    var hold = false
    val updatedDynamicFilteredPlan = planForDF.transform {
      case typeFilter: TypedFilter =>
        val schemaStruct = typeFilter.schema
        val typeFilterRef = typeFilter.output.map(_.exprId)
        val checkAttr = updatedJoinAttr.filter {
          attr => typeFilterRef.contains(attr._1.asInstanceOf[AttributeReference].exprId)
        }
        if (schemaStruct.fieldNames.contains(bloomFilterKeyAppender) &&
          checkAttr.nonEmpty) {
          // TODO add better logic for closing the recursion
          hold = true
        }
        typeFilter
      case filter @ Filter(_, LocalRelation(_, _, _)) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          filter, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case filter @ Filter(_, LogicalRelation(_, _, _, _))  if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr, filter,
          predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case filter @ Filter(_, HiveTableRelation(_, _, _, _, _))  if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr, filter,
          predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case filter @ Filter(_, InMemoryRelation(_, _, _)) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          filter, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case localTableScan: LocalRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          localTableScan, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case logicalRelation: LogicalRelation if !hold =>
       getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          logicalRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case hiveTableRelation: HiveTableRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          hiveTableRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case inMemoryRelation: InMemoryRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, dfr,
          inMemoryRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
    }
    logDebug(s"optimized DynamicFilteredPlan:: ${updatedDynamicFilteredPlan}")
    updatedDynamicFilteredPlan
  }

  // Helper method to find when should we apply the size validation
  // if the plan is directly on that table than compare size
  private def sizeScanValidation(plan: LogicalPlan): Boolean = {
    plan match {
      case Project(_, Filter(_, LocalRelation(_, _, _)))
           | Project(_, Filter(_, LogicalRelation(_, _, _, _)))
           | Project(_, Filter(_, HiveTableRelation(_, _, _, _, _)))
           | Project(_, Filter(_, InMemoryRelation(_, _, _))) => true
      case Filter(_, LocalRelation(_, _, _))
           | Filter(_, LogicalRelation(_, _, _, _))
           | Filter(_, HiveTableRelation(_, _, _, _, _))
           | Filter(_, InMemoryRelation(_, _, _)) => true
      case LocalRelation(_, _, _)
           | LogicalRelation(_, _, _, _)
           | HiveTableRelation(_, _, _, _, _)
           | InMemoryRelation(_, _, _) => true
      case _ => false
    }
  }

  private def compareJoinSides()(implicit spark: SparkSession):  Boolean = {
    spark.conf.get("spark.sql.dynamicfilter.comparejoinsides",
      spark.sparkContext.getConf.get("spark.sql.dynamicfilter.comparejoinsides",
        "false")).toBoolean
  }

  private def useDynamicFilterInBHJ()(implicit spark: SparkSession):  Boolean = {
    spark.conf.get("spark.sql.use.dynamicfilter.bhj",
      spark.sparkContext.getConf.get("spark.sql.use.dynamicfilter.bhj",
        "false")).toBoolean
  }

  private def validJoinForDynamicFilter(joinType: JoinType): Boolean = {
    joinType match {
      case  Inner | LeftSemi | RightOuter | LeftOuter | LeftAnti => true
      case _ => false
    }
  }

  private def getOptimizedLogicalPlan(
     plan: LogicalPlan,
     bloomFilterCount: Long)(implicit spark: SparkSession): LogicalPlan = {
    logDebug("Initial plan: "+ plan)
    val updatedPlan = plan.transform {
      case join: Join if validJoinForDynamicFilter(join.joinType) =>
        var joinCondition: Option[Expression] = None
        joinCondition = join.condition
        val bhjThreshold: Long = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
          .split("b").head.toLong
        // use DF in the BHJ for scenarios where one table is so huge and there is
        // benefit of using DF for reducing the scan and as well as the number of
        // records in filter
        val bhjPresent = (join.left.stats.sizeInBytes <= bhjThreshold
          || join.right.stats.sizeInBytes <= bhjThreshold) && !useDynamicFilterInBHJ

        // if bhjPresent is not present than only add this logic
        if (!bhjPresent && joinCondition.isDefined
          && joinCondition.get.find(_.isInstanceOf[Or]).isEmpty) {
          val predicates = joinCondition.map(getSplittedByAndPredicates).getOrElse(Nil)
          val joinKeys = predicates.filter {
            case EqualTo(_, _) => true
            case _ => false
          }
          var joinAttr = joinKeys.map(x =>
            (x.asInstanceOf[EqualTo].left,x.asInstanceOf[EqualTo].right)).toList
          // filter out both left and right side attribute. This will remove
          // the join condition which is of type comparison of one of the predicate of table
          // and another one is string, int, double etc for eg a.value = 'testDF', this
          // is not needed for Dynamic filter
          joinAttr = joinAttr.filter( x =>
            x._1.isInstanceOf[AttributeReference] && x._2.isInstanceOf[AttributeReference])
          if (joinAttr.isEmpty) {
            join
          } else {
              var ltPlan = join.left
              var rtPlan = join.right
              // Finding out the candidate where dynamic filter needs to be applied.
              // This valid only for inner joins
              if (join.joinType == Inner && compareJoinSides &&
                sizeScanValidation(join.right) &&
                join.left.stats.sizeInBytes < join.right.stats.sizeInBytes) {
                rtPlan = join.left
                ltPlan = join.right
              }
            if (join.joinType == LeftOuter || join.joinType == LeftAnti) {
              rtPlan = getDynamicFilteredPlan(spark, rtPlan,
                ltPlan, ltPlan.output, bloomFilterCount, joinAttr)
            } else {
              ltPlan = getDynamicFilteredPlan(spark, ltPlan,
                rtPlan, rtPlan.output, bloomFilterCount, joinAttr)
            }
            val updatedJoin = Join(ltPlan, rtPlan, join.joinType, join.condition, join.hint)
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
          case typeFilter @ TypedFilter(_, _, argsSchema, _, _)
            if argsSchema.names.exists(_.contains(bloomFilterKey)) =>
            typeFilter.copy(child = Filter(filter.condition, typeFilter.child))
          case _ => filter
        }
        updatedFilterPlan
    }
  }

  def addDynamicFiltersPlan(spark: SparkSession,
    plan: LogicalPlan,
    bloomFilterCount: Long): LogicalPlan = {
    try {
      val updatedPlan = getOptimizedLogicalPlan(plan, bloomFilterCount)(spark)
      updatedPlan
    }
    catch {
      case ex : Throwable =>
        logDebug(s"exception while creating the addDynamicFiltersPlan: ${ex}")
        throw ex
    }
  }
}
