package com.spark.radiant.sql.catalyst.optimizer

import com.spark.radiant.sql.api.SparkRadiantSqlApi

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import java.io.File
import scala.reflect.io.Directory

class ExchangeOptimizeRuleSuite extends AnyFunSuite
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  def createSparkSession(sparkConf: SparkConf) : SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config(sparkConf)
      .getOrCreate()
    spark
  }

  var spark: SparkSession = null
  var sparkConf: SparkConf = null

  protected def sparkContext = spark.sparkContext

  override protected def beforeAll(): Unit = {
    sparkConf = new SparkConf()
    spark = createSparkSession(sparkConf)
    spark.sql("set spark.sql.skip.partial.exchange.rule=true")
    spark.sql("set spark.sql.optimize.union.reuse.exchange.rule=true")
    sparkContext.setLogLevel("ERROR")
    var df = spark.createDataFrame(Seq((1, 1), (1, 2),
      (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("test11", "test12")
    df.createOrReplaceTempView("testDf1")
    df = spark.createDataFrame(Seq((1, 1, 4), (1, 2, 5),
      (2, 1, 6), (2, 1, 7), (2, 3, 8), (3, 2, 9),
      (3, 3, 7))).toDF("test21", "test22", "test23")
    df.createOrReplaceTempView("testDf2")
    df = spark.createDataFrame(Seq((1, 1, 4), (1, 2, 5),
      (3, 3, 7))).toDF("test31", "test32", "test33")
    df.createOrReplaceTempView("testDf3")

    // adding Extra optimizer rule
    val sparkRadiantSqlApi = new SparkRadiantSqlApi()
    sparkRadiantSqlApi.addOptimizerRule(spark)
  }

  def deleteDir(path: String): Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
  }

  override protected def afterAll(): Unit = {
    deleteDir("src/test/resources/TestExchangeOptParquet1")
    deleteDir("src/test/resources/TestExchangeOptParquet2")
    spark.stop()
  }

  test("test SparkSession is working") {
    val u = sparkContext.parallelize(1 to 2).map{x => x}.sum
    assert(u==3)
  }

  test("test the ExchangeOptimizeRule is not applied if its BHJ") {
    val df = spark.sql("select a.*  from (select test11, test12, count(*) from testDf1 group" +
      " by test11, test12) a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{x => x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isEmpty)
  }

  test("test the ExchangeOptimizeRule is applied") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select a.*  from (select test11, test12, count(*) from testDf1 group" +
      " by test11, test12) a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{x => x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isDefined)
  }

  test("test the ExchangeOptimizeRule is not applied") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select a.*  from (select test11, count(*) from testDf1 group" +
      " by test11) a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{x => x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isEmpty)
  }

  test("test the ExchangeOptimizeRule is applied on both sides of join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select a.*, b.*  from (select test11, test12, count(*)" +
      " from testDf1 group by test11, test12) a join (select test21, test22, count(*)" +
      " from testDf2 group by test21, test22) b on a.test11 = b.test21" +
      " where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan
    var optSize = 0
    updateDFPlan.transform {
      case r: RepartitionByExpression => optSize = optSize + 1
        r
    }
    assert(optSize == 2)
  }

  test("test the ExchangeOptimizeRule is applied on right side of join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select a.*  from testDf2 b join " +
      "(select test11, test12, count(*) from testDf1 group by test11, test12) a" +
      " on a.test11=b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{x => x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isDefined)
  }

  test("test the ExchangeOptimizeRule is applied on executedPlan") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    var df = spark.sql("select a.*  from (select test11, test12, count(*) from testDf1 group" +
      " by test11, test12) a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    // without ExchangeOptimizeRule
    spark.sql("set spark.sql.skip.partial.exchange.rule=false")
    var updateDFPlan = df.queryExecution.executedPlan
    var exchangeCount = 0
    updateDFPlan.transform {
      case ad: AdaptiveSparkPlanExec => ad.executedPlan
      case ex: ShuffleExchangeExec => exchangeCount = exchangeCount + 1
        ex
    }
    assert(exchangeCount == 3)
    // with ExchangeOptimizeRule
    spark.sql("set spark.sql.skip.partial.exchange.rule=true")
    df = spark.sql("select a.*  from (select test11, test12, count(*) from testDf1 group" +
      " by test11, test12) a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    exchangeCount = 0
    updateDFPlan = df.queryExecution.executedPlan
    updateDFPlan.transform {
      case ad: AdaptiveSparkPlanExec => ad.executedPlan
      case ex: ShuffleExchangeExec => exchangeCount = exchangeCount + 1
        ex
    }
    assert(exchangeCount == 2)
  }

  test("test the UnionReuseExchangeOptimizeRule is applied") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select test11, count(*) as count from testDf1" +
      " group by test11 union select test11, sum(test11) as count" +
      " from testDf1 group by test11")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{x =>
      x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isDefined)
    df.collect()
    val executedPlan = df.queryExecution.executedPlan.transform {
      case ad: AdaptiveSparkPlanExec => ad.executedPlan
      case sh: ShuffleQueryStageExec => sh.plan
      case ex => ex
    }
    val reuseExchange =
      executedPlan.collectLeaves().filter(_.isInstanceOf[ReusedExchangeExec])
    assert(reuseExchange.nonEmpty)
  }

  test("test the ExchangeOptimizeRule is not applied if repartition already present") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select test11, test12, count(*) from" +
      " testDf1 group by test11, test12").repartition(10)
    val df1 = spark.sql("select * from testDf2")
    val df2 = df.join(df1, df("test11") === df1("test21"), "inner")
    val updateDFPlan = df2.queryExecution.optimizedPlan.find{x => x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isEmpty)
  }

  test("test the UnionReuseExchangeOptimizeRule is not applied if repartition already present") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select test11, count(*) as count from testDf1" +
      " group by test11").repartition(10)
    val df1 = spark.sql("select test11, sum(test11) as count" +
      " from testDf1 group by test11").repartition(10)
    val df2 = df.union(df1)
    val updateDFPlan = df2.queryExecution.optimizedPlan.find{x =>
      x.isInstanceOf[RepartitionByExpression]}
    assert(updateDFPlan.isEmpty)
    df2.collect()
    val executedPlan = df2.queryExecution.executedPlan.transform {
      case ad: AdaptiveSparkPlanExec => ad.executedPlan
      case sh: ShuffleQueryStageExec => sh.plan
      case ex => ex
    }
    val reuseExchange =
      executedPlan.collectLeaves().filter(_.isInstanceOf[ReusedExchangeExec])
    assert(reuseExchange.isEmpty)
  }
}
