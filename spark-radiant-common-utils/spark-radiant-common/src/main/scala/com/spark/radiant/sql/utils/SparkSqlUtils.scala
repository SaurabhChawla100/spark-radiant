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

package com.spark.radiant.sql.utils

import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.io.ByteArrayOutputStream
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

import org.apache.spark.util.sketch.BloomFilter
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{Filter => V2Filter}
import org.apache.spark.sql.types.{IntegerType, LongType, ShortType}

import scala.reflect.runtime.{universe => ru}

/**
 * SparkSqlUtils utility class
 *
 */

class SparkSqlUtils extends Serializable {

  def getSplittedByAndPredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        getSplittedByAndPredicates(cond1) ++ getSplittedByAndPredicates(cond2)
      case other => other :: Nil
    }
  }

  def createDfFromLogicalPlan(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    // scalastyle:off classforname
    val cls = Class.forName("org.apache.spark.sql.Dataset")
    // scalastyle:on classforname
    val method = cls.getMethod("ofRows", classOf[SparkSession], classOf[LogicalPlan])
    method.invoke(cls, spark, logicalPlan).asInstanceOf[Dataset[_]].toDF
  }


  def getBloomFilterSize(conf: SparkConf): Long = {
    val defaultValue = 10000000L
    try {
      conf.getLong("spark.sql.dynamicFilter.bloomFilter.size", defaultValue)
    } catch {
      case _: Exception =>
        defaultValue
    }
  }

  def getDynamicFilterCompletionTime(conf: SparkConf): Long = {
    val defaultValue = 60
    try {
      conf.getLong("spark.sql.dynamicFilter.completion.threshold", defaultValue)
    } catch {
      case _: Exception =>
        defaultValue
    }
  }

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  def saveBloomFilter(
     filter: BloomFilter,
     path: String): Unit = {
    val inputPath = new Path(path)
    val hadoopConf = getHadoopConf()
    val fs = inputPath.getFileSystem(hadoopConf)
    val fileOutputStream: FSDataOutputStream = fs.create(new Path(path))
    val out = new ByteArrayOutputStream()
    try {
      filter.writeTo(out)
      fileOutputStream.write(out.toByteArray)
    } catch {
      case ex: Exception =>
        throw ex;
    } finally {
      fileOutputStream.close()
      out.close()
    }
  }

  def readBloomFilter(path: String): BloomFilter = {
    val deserialized = {
      var fileInputStream: FSDataInputStream = null
      try {
        val hadoopConf = getHadoopConf()
        val fs = new Path(path).getFileSystem(hadoopConf)
        fileInputStream = fs.open(new Path(path))
        BloomFilter.readFrom(fileInputStream)
      } finally {
        fileInputStream.close()
      }
    }
    deserialized
  }

  def mergeSaveBloomFilter(
     inputBloomFilter1: BloomFilter,
     inputBloomFilter2: BloomFilter,
     path: String): Unit = {
    try {
      val mergedBloomFilter = inputBloomFilter1.mergeInPlace(inputBloomFilter2)
      saveBloomFilter(mergedBloomFilter, path)
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  /**
   * Helper method to invoke TranslateFilterMethod of DataSourceStrategy
   */
  def invokeObjectTranslateFilterMethod(objectName: String,
     methodName: String,
     filterExp: AnyRef, supportNestedPredicatePushdown: Boolean): V2Filter = {
    try {
      val rm = scala.reflect.runtime.currentMirror
      val moduleSymbol = rm.staticModule(objectName)
      val classSymbol = moduleSymbol.moduleClass.asClass
      val moduleMirror = rm.reflectModule(moduleSymbol)
      val objectInstance = moduleMirror.instance
      val objectType = classSymbol.toType
      val methodSymbol = objectType.decl(ru.TermName(methodName)).asMethod
      val instanceMirror = rm.reflect(objectInstance)
      val methodMirror = instanceMirror.reflectMethod(methodSymbol)
      val output = methodMirror.apply(filterExp, supportNestedPredicatePushdown) match {
        case Some(filterValue) => filterValue
        case otherValue => otherValue
      }
      output.asInstanceOf[V2Filter]
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  def getSplitByAndFilter(filter: V2Filter): List[V2Filter] = {
    filter match {
      case org.apache.spark.sql.sources.And(cond1, cond2) =>
        getSplitByAndFilter(cond1) ++ getSplitByAndFilter(cond2)
      case other => (other :: Nil)
    }
  }

  def createBloomFilterUsingRDD(rdd: RDD[_],
     bloomFilterCount: Long,
     fpp: Double): BloomFilter = {
    // create bloomFilter from the RDD
    val bloomFilter = rdd.treeAggregate(null.asInstanceOf[BloomFilter])(
      (filter, value) => {
        val theFilter: BloomFilter = if (filter == null) {
          BloomFilter.create(bloomFilterCount, fpp)
        } else {
          filter
        }
        value match {
          case Long| Int| Short| IntegerType| LongType| ShortType =>
            theFilter.putLong(value.asInstanceOf[Long])
          case _ => theFilter.put(value)
        }
        theFilter
      },
      (filter1, filter2) => {
        if (filter1 == null) {
          filter2
        } else if (filter2 == null) {
          filter1
        } else {
          filter1.mergeInPlace(filter2)
        }
      }
    )
    bloomFilter
  }

  def getHadoopConf(): Configuration = {
    // scalastyle:off classforname
    val sparkHadoopUtilClass = Class.forName(
      "org.apache.spark.deploy.SparkHadoopUtil")
    // scalastyle:on classforname
    val sparkHadoopUtil = sparkHadoopUtilClass.newInstance()
    val newConfigurationMethod = sparkHadoopUtilClass.getMethod(
      "newConfiguration", classOf[SparkConf])
    newConfigurationMethod.invoke(
      sparkHadoopUtil, SparkEnv.get.conf).asInstanceOf[Configuration]
  }
}
