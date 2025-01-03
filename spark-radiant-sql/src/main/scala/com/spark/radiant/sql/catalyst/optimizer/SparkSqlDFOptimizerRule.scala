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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, Cast, ConcatWs, EqualTo, Expression, In, Literal, Md5, Or}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, TypedFilter}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.{col, md5}
import org.apache.spark.sql.sources.{Filter => V2Filter}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.sql.{Column, CustomFilter, DataFrame, Dataset, MightContainInBloomFilter, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

private[sql] class SparkSqlDFOptimizerRule extends LazyLogging with Serializable {
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
   * @param plan - LogicalPlan for which bloom filter is needed
   * @param columns - list of column that can be combined to create key
   * @param bloomFilterKeyApp - BloomFilter key name
   * @return
   */
  private def createBloomFilterKeyColumn(plan: LogicalPlan,
     columns: List[Expression],
     bloomFilterKeyApp: String): LogicalPlan = {

    val oldAttr = plan.outputSet.toList
      .map(_.asInstanceOf[org.apache.spark.sql.catalyst.expressions.NamedExpression])
    val newAttrList = oldAttr ++ List(Alias(
      Md5(Cast(child = ConcatWs(List(Literal(dfKeySeparator)) ++ columns),
        dataType = BinaryType)), bloomFilterKeyApp)())
    Project(newAttrList, plan)
  }

  private def createDfFromLogicalPlan(spark: SparkSession,
     logicalPlan: LogicalPlan): DataFrame = {
    // scalastyle:off classforname
    val cls = Class.forName("org.apache.spark.sql.Dataset")
    // scalastyle:on classforname
    val method = cls.getMethod("ofRows", classOf[SparkSession], classOf[LogicalPlan])
    method.invoke(cls, spark, logicalPlan).asInstanceOf[Dataset[_]].toDF
  }

  /**
   *
   * @param spark - existing spark Session
   * @param bloomFilterAppendedKey - Column name for BloomFilter key in Dynamic Filter
   * @param planForGenDf - plan for generating the values needed
   *                          for Dynamic filter
   * @param planForDf - Bigger table plan where the Dynamic Filter is added
   * @param outputForGenDF - Output of smaller table for generating the Dynamic Filter
   * @param bloomFilterCount - size of bloom filter
   * @param joinAttr - Attributes present in the join condition
   * @return - Return optimized logical plan having Dynamic Filter
   */
  private def getPlanFromJoinCondition(spark: SparkSession,
     bloomFilterAppendedKey: String,
     planForGenDf: LogicalPlan,
     planForDf: LogicalPlan,
     outputForGenDF: Seq[Attribute],
     bloomFilterCount: Long,
     joinAttr: List[(Expression, Expression)]): LogicalPlan = {

    var filterNeededForDF: Seq[Expression] = Seq.empty
    var filterToCreateDF: Seq[Expression] = Seq.empty

    val exprOut = planForDf match {
      case Filter(_, LocalRelation(output, _, _)) => output.map(_.exprId)
      case Filter(_, LogicalRelation(_, output, _, _)) => output.map(_.exprId)
      case Filter(_, HiveTableRelation(_, output, _, _, _)) => output.map(_.exprId)
      case Filter(_, InMemoryRelation(output, _, _)) => output.map(_.exprId)
      case Filter(_, dsv2: DataSourceV2ScanRelation) => dsv2.output.map(_.exprId)
      case Filter(_, DataSourceV2Relation(_, output, _, _, _)) => output.map(_.exprId)
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
      // conf to push all the join key values to the datasource pushed down filter
      val useAllJoinKey = pushDownAllJoinKeyValues()(spark)
      val dfr = createDfFromLogicalPlan(spark,
        createBloomFilterKeyColumn(planForGenDf,
          filterToCreateDF.toList,
          bloomFilterAppendedKey))
      val rightFilterKeyValue = if (useAllJoinKey) {
        val filterToCreateDFKey = filterToCreateDF.map(
          x => x.asInstanceOf[AttributeReference].name) ++ List(bloomFilterAppendedKey)
        dfr.select(filterToCreateDFKey.map(x => col(x)): _*)
          .limit(thresholdPushDownLength + 1).collect()
      } else {
        // push down only join key value to datasource/fileSourceScan
        val rightFilterKey =
          List(filterToCreateDF.head.asInstanceOf[AttributeReference].name,
            bloomFilterAppendedKey)
        dfr.select(rightFilterKey.map(x => col(x)): _*).
          limit(thresholdPushDownLength + 1).collect()
      }
      var createBloomFilterValue: List[String] = List.empty
      var pushDownFileScanValues: List[Set[Literal]] = List.empty
      if (rightFilterKeyValue.length <= thresholdPushDownLength) {
        var rightKeyIndex = 0
        while(rightKeyIndex < rightFilterKeyValue.head.length) {
          val lit = List(rightFilterKeyValue.map(x => Literal(x.get(rightKeyIndex))).toSet)
          pushDownFileScanValues = pushDownFileScanValues ++ lit
          rightKeyIndex = rightKeyIndex + 1
        }
        createBloomFilterValue =
          rightFilterKeyValue.map(_.getAs(bloomFilterAppendedKey).toString).toList
      }
      val bloomFilter = if (createBloomFilterValue.nonEmpty) {
        val utils = new SparkSqlUtils()
        val filter = if (createBloomFilterFromRDD()(spark)) {
          utils.createBloomFilterUsingRDD(
          spark.sparkContext.parallelize(createBloomFilterValue, 4),
          bloomFilterCount, fpp)
        } else {
          val bloomFilter = BloomFilter.create(bloomFilterCount, fpp)
          createBloomFilterValue.foreach(x => bloomFilter.put(x))
          bloomFilter
        }
        filter
      } else {
        dfr.stat.bloomFilter(bloomFilterAppendedKey, bloomFilterCount, fpp)
      }
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

      var leftDynamicFilterPlan = createBloomFilterKeyColumn(planForDf,
        filterNeededForDF.toList, bloomFilterAppendedKey)
      if (pushDownFileScanValues.nonEmpty) {
        var inExpr: Expression = In(filterNeededForDF.head, pushDownFileScanValues.head.toList)
        // push all the join key values to the datasource pushed down filter
        if (useAllJoinKey && filterNeededForDF.size == pushDownFileScanValues.size -1) {
          var leftKeyIndex = 1
          while(leftKeyIndex < filterNeededForDF.size) {
            inExpr = And(inExpr,
              In(filterNeededForDF(leftKeyIndex), pushDownFileScanValues(leftKeyIndex).toList))
            leftKeyIndex = leftKeyIndex + 1
          }
        }
        leftDynamicFilterPlan = Filter(inExpr, leftDynamicFilterPlan)
      }
      val dfl = if (codegenSupportToBloomFilter()(spark)) {
        val newdfl = createDfFromLogicalPlan(spark, leftDynamicFilterPlan)
        val utils = new SparkSqlUtils()
        val plan = newdfl.queryExecution.optimizedPlan
        val rightExpression = plan.output.filter(x => x.name.contains(bloomFilterKey)).head
        val encodedBloomFilter = utils.serializeDynamicFilterBloomFilter(bloomFilter)
        val newPlan = CustomFilter(MightContainInBloomFilter(Literal(encodedBloomFilter),
          rightExpression), plan)
       createDfFromLogicalPlan(spark, newPlan)
      } else {
        createDfFromLogicalPlan(spark, leftDynamicFilterPlan).filter { x =>
          broadcastValue.value.mightContain(x.getAs(bloomFilterAppendedKey))
        }
      }
      dfl.drop(dfl(bloomFilterAppendedKey)).queryExecution.optimizedPlan
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
    val rightSideDFPlan: LogicalPlan = predicatesPlanUsedInDF
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
      case customFilter: CustomFilter =>
        val schemaStruct = customFilter.schema
        val customFilterRef = customFilter.output.map(_.exprId)
        val checkAttr = updatedJoinAttr.filter {
          attr => customFilterRef.contains(attr._1.asInstanceOf[AttributeReference].exprId)
        }
        if (schemaStruct.fieldNames.contains(bloomFilterKeyAppender) &&
          checkAttr.nonEmpty) {
          // TODO add better logic for closing the recursion
          hold = true
        }
        customFilter
      // TODO Add Filter instead of CustomFilter
      /*
      case filter@Filter(condition, _) if !hold =>
        val utils = new SparkSqlUtils()
        val exprs = utils.getSplittedByAndPredicates(condition)
        if (exprs.exists(expr => expr.isInstanceOf[MightContainInBloomFilter])) {
          hold = true
        }
        filter
       */
      case filter@Filter(_, LocalRelation(_, _, _)
           | LogicalRelation(_, _, _, _)
           | HiveTableRelation(_, _, _, _, _)
           | InMemoryRelation(_, _, _)) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          filter, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case filter@Filter(_, dsv2: DataSourceV2ScanRelation) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          filter, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      // For supporting the DataSourceV2 when this rule is added as the part
      // of spark.sql.extensions
      case filter@Filter(_, DataSourceV2Relation(_, _, _, _, _)) if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          filter, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case localTableScan: LocalRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          localTableScan, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case logicalRelation: LogicalRelation if !hold =>
       getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          logicalRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case hiveTableRelation: HiveTableRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          hiveTableRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case dataSourceV2ScanRelation: DataSourceV2ScanRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          dataSourceV2ScanRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
      case inMemoryRelation: InMemoryRelation if !hold =>
        getPlanFromJoinCondition(spark, bloomFilterKeyAppender, rightSideDFPlan,
          inMemoryRelation, predicateOutputInDF, bloomFilterCount, updatedJoinAttr)
    }
    logger.debug(s"optimized DynamicFilteredPlan:: ${updatedDynamicFilteredPlan}")
    updatedDynamicFilteredPlan
  }

  // Helper method to find when should we apply the size validation
  // if the plan is directly on that table than compare size
  private def sizeScanValidation(plan: LogicalPlan): Boolean = {
    plan match {
      case Project(_, Filter(_, LocalRelation(_, _, _)))
           | Project(_, Filter(_, LogicalRelation(_, _, _, _)))
           | Project(_, Filter(_, HiveTableRelation(_, _, _, _, _)))
           | Project(_, Filter(_, DataSourceV2Relation(_, _, _, _, _)))
           | Project(_, Filter(_, InMemoryRelation(_, _, _))) => true
      case Project(_, Filter(_, dsv2: DataSourceV2ScanRelation)) => true
      case Filter(_, LocalRelation(_, _, _))
           | Filter(_, LogicalRelation(_, _, _, _))
           | Filter(_, HiveTableRelation(_, _, _, _, _))
           | Filter(_, DataSourceV2Relation(_, _, _, _, _))
           | Filter(_, InMemoryRelation(_, _, _)) => true
      case Filter(_, dsv2: DataSourceV2ScanRelation) => true
      case LocalRelation(_, _, _)
           | LogicalRelation(_, _, _, _)
           | HiveTableRelation(_, _, _, _, _)
           | DataSourceV2Relation(_, _, _, _, _)
           | InMemoryRelation(_, _, _) => true
      case dsv2: DataSourceV2ScanRelation => true
      case _ => false
    }
  }

  private def compareJoinSides()(implicit spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.dynamicfilter.comparejoinsides",
      spark.sparkContext.getConf.get("spark.sql.dynamicfilter.comparejoinsides",
        "false")).toBoolean
  }

  private def codegenSupportToBloomFilter()(implicit spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.dynamicfilter.support.codegen",
      spark.sparkContext.getConf.get("spark.sql.dynamicfilter.support.codegen",
        "false")).toBoolean
  }

  private def pushDownAllJoinKeyValues()(implicit spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.dynamicFilter.pushdown.allJoinKey",
      spark.sparkContext.getConf.get("spark.sql.dynamicFilter.pushdown.allJoinKey",
        "true")).toBoolean
  }

  private def useDynamicFilterInBHJ()(implicit spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.use.dynamicfilter.bhj",
      spark.sparkContext.getConf.get("spark.sql.use.dynamicfilter.bhj",
        "false")).toBoolean
  }

  private def createBloomFilterFromRDD()(implicit spark: SparkSession): Boolean = {
    spark.conf.get("spark.sql.create.bloomfilter.rdd",
      spark.sparkContext.getConf.get("spark.sql.create.bloomfilter.rdd",
        "true")).toBoolean
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
    logger.debug(s"Initial plan: $plan")
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

        // check Custom Filter is present or not and apply DF to
        // the plan if CustomFilter not present
        val customFilterPresent = join.left.find(_.isInstanceOf[CustomFilter]).isDefined ||
          join.right.find(_.isInstanceOf[CustomFilter]).isDefined

        // if bhjPresent is not present than only add this logic
        if (!customFilterPresent && !bhjPresent && joinCondition.isDefined
          && joinCondition.get.find(_.isInstanceOf[Or]).isEmpty) {
          val predicates = joinCondition.map(getSplittedByAndPredicates).getOrElse(Nil)
          val joinKeys = predicates.filter {
            case EqualTo(_, _) => true
            case _ => false
          }
          var joinAttr = joinKeys.map(x =>
            (x.asInstanceOf[EqualTo].left, x.asInstanceOf[EqualTo].right)).toList
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
            logger.debug(s"updatedJoin after applying DF :: ${updatedJoin}")
            updatedJoin
          }
        } else {
          join
        }
    }
    logger.info(s"updatedPlan after applying DF : ${updatedPlan}")
    updatedPlan
  }

  def pushFilterBelowTypedFilterRule(plan: LogicalPlan) : LogicalPlan = {
    plan.transform {
      case filter: Filter =>
        val updatedFilterPlan = filter.child match {
          case typeFilter@TypedFilter(_, _, argsSchema, _, _)
            if argsSchema.names.exists(_.contains(bloomFilterKey)) =>
            typeFilter.copy(child = Filter(filter.condition, typeFilter.child))
          case customFilter@CustomFilter(_, _)
            if customFilter.schema.names.exists(_.contains(bloomFilterKey)) =>
            customFilter.copy(child = Filter(filter.condition, customFilter.child))
          case _ => filter
        }
        updatedFilterPlan
    }
  }

  /**
   *
   * This method pushes the Filter to FileScan for V2 data sources.
   * Dynamic filter is pushed down to Parquet and ORC for V2 dataSource.
   * This will work with Spark-3.1.1 and later version of spark.
   */
  def pushDownFilterToV2Scan(plan: LogicalPlan)
     (implicit spark: SparkSession) : LogicalPlan = {
    try {
      plan.transform {
        case filter@Filter(_, v2Scan: DataSourceV2ScanRelation) =>
          // Push down filter to orc and parquet
          val existingPushedFilter = v2Scan.scan match {
            case parquet: ParquetScan => parquet.pushedFilters
            case orc: OrcScan => orc.pushedFilters
            case _ => Array.empty
          }
          var filterToAdd: List[V2Filter] = List.empty
          val utils = new SparkSqlUtils()
          val v2DeterministicPushdown = spark.sparkContext.getConf.getBoolean(
            "spark.sql.dynamicFilter.v2pushdown.deterministic", true)
          val pushDownFilter = if (v2DeterministicPushdown) {
            getSplittedByAndPredicates(filter.condition).filter(_.deterministic)
          } else {
            getSplittedByAndPredicates(filter.condition)
          }
          if (pushDownFilter.nonEmpty) {
            val pushDownCond = pushDownFilter.reduceLeft(And)
            val convertedSourcesFilter = utils.invokeObjectTranslateFilterMethod(
              "org.apache.spark.sql.execution.datasources.DataSourceStrategy",
              "translateFilter", pushDownCond, true)
            val updatedFilter: List[V2Filter] =
              utils.getSplitByAndFilter(convertedSourcesFilter)
            if (existingPushedFilter.nonEmpty && updatedFilter.nonEmpty) {
              filterToAdd = updatedFilter.filter(!existingPushedFilter.contains(_))
              filterToAdd = existingPushedFilter.toList ++ filterToAdd
              // TODO Add the support for Spark-3.0.x
              val updatedScan = v2Scan.scan match {
                case parquet: ParquetScan =>
                  parquet.copy(pushedFilters = filterToAdd.toArray)
                case orc: OrcScan =>
                  orc.copy(pushedFilters = filterToAdd.toArray)
                case scan =>
                  scan
              }
              filter.copy(child = v2Scan.copy(scan = updatedScan))
            } else {
              filter
            }
          } else {
            filter
          }
      }
    } catch {
      case ex: Throwable =>
        logger.debug(s"exception while creating pushDownFilterToV2Scan: $ex")
        plan
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
        logger.debug(s"exception while creating the addDynamicFiltersPlan: ${ex}")
        throw ex
    }
  }
}
