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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.TypedFilter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import com.spark.radiant.sql.api.SparkRadiantSqlApi

import java.io.File
import scala.reflect.io.Directory

/**
 * DataFrameOptimizerSuite test suite for optimizer
 *
 */

class DataFrameOptimizerSuite extends AnyFunSuite
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
   sparkContext.setLogLevel("ERROR")
   var df = spark.createDataFrame(Seq((1, 1), (1, 2),
     (2, 1), (2, 1), (2, 3), (3, 2), (3, 3))).toDF("test11", "test12")
   df.createOrReplaceTempView("testDf1")
   df = spark.createDataFrame(Seq((1, 1, 4), (1, 2, 5),
     (2, 1, 6), (2, 1, 7), (2, 3, 8), (3, 2, 9), (3, 3, 7))).toDF("test21", "test22", "test23")
   df.createOrReplaceTempView("testDf2")
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  def deleteDir(path: String): Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
  }

  test("test SparkSession is working") {
    val u = sparkContext.parallelize(1 to 2).map{x => x}.sum
    assert(u==3)
  }

  test("test the Dynamic filter is not applied to left side of the table if its BHJ") {
    val df = spark.sql("select * from testDf1 a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isEmpty)
  }

  test("test the Dynamic filter is applied to left side of the table") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b" +
      " on a.test11=b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied if sequence of join condition is different") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b" +
      " on b.test21=a.test11 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table for subquery") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on a.test11=b.test21" +
      " where b.test22 in (select test22 from testDf2 where test22 = 2)")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is applied to left side of the table for scalar-subquery") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on a.test11=b.test21" +
      " where b.test22 = (select distinct test22 from testDf2 where test23 = 9)")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isDefined)
    assert(updateDFPlan.get.schema.names.exists(_.contains(dfOptimizer.bloomFilterKey)))
  }

  test("test the Dynamic filter is not applied to left side of the table if its non-equi join") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val df = spark.sql("select * from testDf1 a join testDf2 b on" +
      " a.test11 < b.test21 where b.test22=2")
    val dfOptimizer = new SparkSqlDFOptimizerRule()
    val updatedPlan = dfOptimizer.addDynamicFiltersPlan(spark,
      df.queryExecution.optimizedPlan, 10000)
    val updateDFPlan = updatedPlan.find{ x => x.isInstanceOf[TypedFilter] }
    assert(updateDFPlan.isEmpty)
  }

  test("test struct type column in the DropDuplicate") {
    val df = spark.createDataFrame(Seq(("d1", StructDropDup(1, 2)),
      ("d1", StructDropDup(1, 2)))).toDF("a", "b")
    val sparkRadiantSqlApi = new SparkRadiantSqlApi()
    val updatedDF = sparkRadiantSqlApi.dropDuplicateOfSpark(df, spark, Seq("a", "b.c1"))
    assert(updatedDF.collect===Array(Row("d1", Row(1, 2))))
  }

  test("create bloomFilter,save and read") {
    val df = spark.createDataFrame(Seq(("d1", StructDropDup(1, 2)),
      ("d2", StructDropDup(1, 2)))).toDF("a", "b")
    val sparkRadiantSqlApi = new SparkRadiantSqlApi()
    // create bloomFilter
    val bf = df.stat.bloomFilter("a", 1000, 0.2)
    val path = "src/test/resources/BloomFilter"
    // save the bloomFilter to the persistent store
    sparkRadiantSqlApi.saveBloomFilter(bf, s"$path/TestBloomFilter")
    // read the bloomFilter from the persistent store and apply the condition
    val df1 = sparkRadiantSqlApi.applyBloomFilterToDF(spark,
      df.filter("a='d2'"),
      s"$path/TestBloomFilter", List("a"))
    deleteDir(path)
  }
}

case class StructDropDup(c1: Int, c2: Int)
