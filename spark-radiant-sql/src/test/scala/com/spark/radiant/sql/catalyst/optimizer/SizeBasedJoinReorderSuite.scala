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

import com.spark.radiant.sql.api.SparkRadiantSqlApi
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import scala.reflect.io.Directory
import java.io.File

class SizeBasedJoinReorderSuite extends AnyFunSuite
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
    spark.sql("set spark.sql.support.sizebased.join.reorder=true")
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
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=30")
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

  test("test the Join reorder is not applied for BHJ is applied before the SMJ") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=30")
    spark.sql("set spark.sql.support.sizebased.join.reorder=false")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7")
    df.queryExecution.sparkPlan match {
      case smj: SortMergeJoinExec => assert(false)
      case bhj: BroadcastHashJoinExec => assert(true)
    }
  }

  test("test the Join reorder applied for BHJ is applied before the SMJ") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=30")
    spark.sql("set spark.sql.support.sizebased.join.reorder=true")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7")
    df.queryExecution.sparkPlan match {
      case smj: SortMergeJoinExec => assert(true)
      case bhj: BroadcastHashJoinExec => assert(false)
    }
  }

  test("test the Join reorder is not applied since both the join is SMJ") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=15")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7")
    assert(df.queryExecution.sparkPlan.isInstanceOf[SortMergeJoinExec])
  }

  test("test the Join reorder is not applied since both the join is not star schema") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=30")
    val df = spark.sql("select * from testDf1 a , testDf2 b ,testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7 and b.test21 = c.test31")
    assert(df.queryExecution.sparkPlan.isInstanceOf[BroadcastHashJoinExec])
    var joinStrList : List[String] = List.empty
    df.queryExecution.sparkPlan.transform {
      case smj: SortMergeJoinExec =>
        joinStrList = joinStrList :+smj.getClass.getCanonicalName
        smj
      case bhj: BroadcastHashJoinExec =>
        joinStrList = joinStrList :+ bhj.getClass.getCanonicalName
        bhj
    }
    assert(joinStrList.head == "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec")
    assert(joinStrList.last == "org.apache.spark.sql.execution.joins.SortMergeJoinExec")
  }

  test("test the Join reorder is applied when project is present") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=15")
    val df = spark.sql("select c.test31 from testDf1 a , testDf2 b ," +
      "testDf3 c where a.test11=b.test21" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7")
    var joinStrList : List[String] = List.empty
    df.queryExecution.sparkPlan.asInstanceOf[ProjectExec].child match {
      case smj: SortMergeJoinExec => assert(true)
      case bhj: BroadcastHashJoinExec => assert(false)
    }
    df.queryExecution.sparkPlan.transform {
      case smj: SortMergeJoinExec =>
        joinStrList = joinStrList :+smj.getClass.getCanonicalName
        smj
      case bhj: BroadcastHashJoinExec =>
        joinStrList = joinStrList :+ bhj.getClass.getCanonicalName
        bhj
    }
    assert(joinStrList.head == "org.apache.spark.sql.execution.joins.SortMergeJoinExec")
    assert(joinStrList.last == "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec")
  }

  test("test the Join reorder is applied when projection is present one one side only") {
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=15")
    val df = spark.sql("select a.test11 from testDf1 a, testDf2 b, testDf3 c1," +
      "testDf3 c where a.test11=b.test21 and a.test11 = c1.test31" +
      " and b.test22 = 2 and a.test11 = c.test31 and c.test33=7")
    var joinStrList : List[String] = List.empty
    df.queryExecution.sparkPlan.asInstanceOf[ProjectExec].child match {
      case smj: SortMergeJoinExec => assert(true)
      case bhj: BroadcastHashJoinExec => assert(false)
    }
    df.queryExecution.sparkPlan.transform {
      case smj: SortMergeJoinExec =>
        joinStrList = joinStrList :+smj.getClass.getCanonicalName
        smj
      case bhj: BroadcastHashJoinExec =>
        joinStrList = joinStrList :+ bhj.getClass.getCanonicalName
        bhj
    }
    assert(joinStrList.head === "org.apache.spark.sql.execution.joins.SortMergeJoinExec")
    assert(joinStrList.last === "org.apache.spark.sql.execution.joins.BroadcastHashJoinExec")
    assert(df.collect() === Array(Row(3), Row(3)))
  }
}
