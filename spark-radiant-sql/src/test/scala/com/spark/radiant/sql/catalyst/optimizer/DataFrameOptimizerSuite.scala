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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.TypedFilter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

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
}
