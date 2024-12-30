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

import scala.reflect.io.Directory
import java.io.File

import com.spark.radiant.sql.api.SparkRadiantSqlApi

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.CustomFilter
import org.apache.spark.sql.catalyst.plans.logical.TypedFilter
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * DynamicFilterOptimizerSuite test suite for optimizer
 *
 */

class DynamicFilterOptimizerSuite extends AnyFunSuite
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {
  def createSparkSession(sparkConf: SparkConf): SparkSession = {
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
    // sparkConf.set("spark.sql.extensions", "com.spark.radiant.sql.api.SparkRadiantSqlExtension")
    spark = createSparkSession(sparkConf)
    spark.sql("set spark.sql.support.dynamicfilter=true")
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
    // create the parquet datasource file
    spark.createDataFrame(Seq((1, 1), (1, 2),
      (2, 1), (2, 1), (2, 3), (3, 2), (3, 3), (3, 4), (4, 1), (3, 5))).toDF("test11", "test12").
      repartition(1).write.mode("overwrite").
      format("parquet").save("src/test/resources/Testparquet1")
    spark.createDataFrame(Seq((1, 1, 4), (1, 2, 5),
      (2, 1, 6), (2, 1, 7), (2, 3, 8), (3, 2, 9),
      (3, 3, 7))).toDF("test21", "test22", "test23").
      repartition(1).write.mode("overwrite").
      format("parquet").save("src/test/resources/Testparquet2")

    // adding Extra optimizer rule
    val sparkRadiantSqlApi = new SparkRadiantSqlApi()
    sparkRadiantSqlApi.addOptimizerRule(spark)
  }

  def deleteDir(path: String): Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
  }

  override protected def afterAll(): Unit = {
    deleteDir("src/test/resources/Testparquet1")
    deleteDir("src/test/resources/Testparquet2")
    spark.stop()
  }

  test("test SparkSession is working") {
    val u = sparkContext.parallelize(1 to 2).map{ x => x }.sum
    assert(u === 3)
  }

  test("test the Dynamic filter is not applied to left side of the table if its BHJ") {
    val df = spark.sql("select * from testDf1 a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isEmpty)
  }

  test("test the Dynamic filter is applied to left side of the table for InMemoryRelation") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df1 = spark.createDataFrame(Seq((1, 1), (1, 2),
      (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("test11", "test12")
    df1.cache()
    df1.createOrReplaceTempView("testDf1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table if its BHJ") {
    spark.sql("set spark.sql.use.dynamicfilter.bhj=true")
    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    spark.sql("set spark.sql.use.dynamicfilter.bhj=false")
  }

  test("test the Dynamic filter is applied to left side of the table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table for subquery") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on a.test11=b.test21" +
      " where b.test22 in (select test22 from testDf2 where test22 = 2)")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table for scalar-subquery") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on a.test11=b.test21" +
      " where b.test22 = (select distinct test22 from testDf2 where test23 = 9)")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table between 3 tables") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7 and b.test21=c.test31")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied if sequence of join conditions are not same") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where b.test21=a.test11" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7 and b.test21=c.test31")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is not applied to left side of the table if its non-equi join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11 < b.test21 where b.test22=2")
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isEmpty)
  }

  test("test the push down Dynamic filter is applied to left side of the table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf2 b join testDf1 a on" +
      " a.test11=b.test21 where a.test12=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val pushedFilter = fileScan.get.asInstanceOf[FileSourceScanExec].metadata.get("PushedFilters")
      val expected = "Some([IsNotNull(test21), In(test21, [1,3])])"
      val expected1 = "Some([In(test21, [1,3]), IsNotNull(test21)])"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expected1)
    }
  }

  test("test the push down Dynamic filter is applied to left side of the table codegen enable") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.dynamicfilter.support.codegen=true")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf2 b join testDf1 a on" +
      " a.test11=b.test21 where a.test12=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[CustomFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val pushedFilter = fileScan.get.asInstanceOf[FileSourceScanExec].metadata.get("PushedFilters")
      val expected = "Some([IsNotNull(test21), In(test21, [1,3])])"
      val expected1 = "Some([In(test21, [1,3]), IsNotNull(test21)])"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expected1)
    }
    spark.sql("set spark.sql.dynamicfilter.support.codegen=false")
  }

  test("test the push down Dynamic filter is applied to bigger table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.dynamicfilter.comparejoinsides=true")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val fileSourceScanExec = fileScan.get.asInstanceOf[FileSourceScanExec]
      val pushedFilter = fileSourceScanExec.metadata.get("PushedFilters")
      val expected = "Some([IsNotNull(test22)," +
        " EqualTo(test22,2), IsNotNull(test21), In(test21, [1,2,3,4])])"
      val expectedPattern = "Some([IsNotNull(test22), EqualTo(test22,2)," +
        " In(test21, [1,2,3,4]), IsNotNull(test21)])"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expectedPattern)
      val expectedDFOutput = "test21,test22,test23"
      val dfAppliedSchema = fileSourceScanExec.schema.names.mkString(",")
      assert(dfAppliedSchema === expectedDFOutput)
    }
    spark.sql("set spark.sql.dynamicfilter.comparejoinsides=false")
  }

  test("test the Dynamic filter is applied for right outer join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a right join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied for left semi join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a left semi join testDf2 b on" +
      " a.test11=b.test21")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied for left anti join on right side of table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a left anti join testDf2 b on" +
      " a.test11=b.test21")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied for left outer join on right side of table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a left join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test Dynamic filter is applied to DataSourceV2 Relation") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.sources.useV1SourceList=avro,csv,json,kafka,orc,text")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select a.test11 from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2 and b.test23=5")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find { x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val batchScan = leaves.find(_.isInstanceOf[BatchScanExec])
    if (batchScan.isDefined) {
      val batchScanExec = batchScan.get.asInstanceOf[BatchScanExec]
      val pushedFilter = batchScanExec.scan.asInstanceOf[ParquetScan].pushedFilters
      val expected = "IsNotNull(test11), EqualTo(test11,1)"
      assert(pushedFilter.mkString(", ") === expected)
      val expectedDFOutput = "test11"
      val dfAppliedSchema = batchScanExec.schema.names.mkString(",")
      assert(dfAppliedSchema.contains(expectedDFOutput))
      assert(df.collect() === Array(Row(1), Row(1)))
      spark.sql("set spark.sql.sources.useV1SourceList=avro,csv,json,kafka,orc,parquet,text")
    }
  }

  test("test the push down all join key values to left side of the table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 and a.test12=b.test22 where b.test23=9")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val pushedFilter = fileScan.get.asInstanceOf[FileSourceScanExec].metadata.get("PushedFilters")
      val expected =
        "Some([IsNotNull(test11), IsNotNull(test12), EqualTo(test11,3), EqualTo(test12,2)])"
      val expected1 =
        "Some([EqualTo(test11,3), EqualTo(test12,2)], IsNotNull(test11), IsNotNull(test12))"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expected1)
    }
  }

  test("test the push down all join key values to left side of the table with codgen") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.dynamicfilter.support.codegen=true")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 and a.test12=b.test22 where b.test23=9")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[CustomFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val pushedFilter = fileScan.get.asInstanceOf[FileSourceScanExec].metadata.get("PushedFilters")
      val expected =
        "Some([IsNotNull(test11), IsNotNull(test12), EqualTo(test11,3), EqualTo(test12,2)])"
      val expected1 =
        "Some([EqualTo(test11,3), EqualTo(test12,2)], IsNotNull(test11), IsNotNull(test12))"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expected1)
      spark.sql("set spark.sql.dynamicfilter.support.codegen=false")
    }
  }

  test("test the push down first join key values to left side of the table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.dynamicFilter.pushdown.allJoinKey=false")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 and a.test12=b.test22 where b.test23=9")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val pushedFilter = fileScan.get.asInstanceOf[FileSourceScanExec].metadata.get("PushedFilters")
      val expected =
        "Some([IsNotNull(test11), IsNotNull(test12), EqualTo(test11,3)])"
      val expected1 =
        "Some([EqualTo(test11,3), IsNotNull(test11), IsNotNull(test12)])"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expected1)
    }
  }

  test("test the sequential creation of BloomFilter in DF") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    spark.sql("set spark.sql.dynamicfilter.comparejoinsides=true")
    spark.sql("set spark.sql.create.bloomfilter.rdd=false")

    spark.read.parquet("src/test/resources/Testparquet1").createOrReplaceTempView("testDf1")
    spark.read.parquet("src/test/resources/Testparquet2").createOrReplaceTempView("testDf2")

    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updateDFPlan = df.queryExecution.optimizedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
    // test pushDown dynamic filter
    val executedPlan = df.queryExecution.executedPlan
    val leaves = executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan.collectLeaves()
      case other => other.collectLeaves()
    }
    val fileScan = leaves.find(_.isInstanceOf[FileSourceScanExec])
    if (fileScan.isDefined) {
      val fileSourceScanExec = fileScan.get.asInstanceOf[FileSourceScanExec]
      val pushedFilter = fileSourceScanExec.metadata.get("PushedFilters")
      val expected = "Some([IsNotNull(test22)," +
        " EqualTo(test22,2), IsNotNull(test21), In(test21, [1,2,3,4])])"
      val expectedPattern = "Some([IsNotNull(test22), EqualTo(test22,2)," +
        " In(test21, [1,2,3,4]), IsNotNull(test21)])"
      assert(pushedFilter.toString === expected || pushedFilter.toString === expectedPattern)
      val expectedDFOutput = "test21,test22,test23"
      val dfAppliedSchema = fileSourceScanExec.schema.names.mkString(",")
      assert(dfAppliedSchema === expectedDFOutput)
      assert(df.limit(2).collect() === Array(Row(1, 2, 5, 1, 1), Row(1, 2, 5, 1, 2)))
      assert(df.count() === 6)
    }
    spark.sql("set spark.sql.dynamicfilter.comparejoinsides=false")
  }
}
