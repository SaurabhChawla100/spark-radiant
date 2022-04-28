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

package org.apache.spark.sql.sparkRadiantUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast,
  ConcatWs, Literal, Md5, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.BinaryType

/**
 * SparkSqlUtils utility class for calling the private package methods, variable
 * present inside the org.apache.spark.sql
 *
 */
object SparkSqlUtil {

  /**
   * Adding the optimizer rule in the extendedOperatorOptimizationRules
   * @param sparkSession
   * @param seqRule
   */
  def injectRule(sparkSession: SparkSession, seqRule: Seq[Rule[LogicalPlan]]): Unit = {
    // inject the extra Optimizer rule
    seqRule.foreach { rule =>
      sparkSession.extensions.injectOptimizerRule(_ => rule)
    }
  }

  /**
   * creating the aggregated filter column
   * @param attrName
   * @param attr
   * @param attrDelimiter
   * @param aggBlmFilter
   */
  def aggregatedFilter(attrName: List[String],
     attr: List[NamedExpression],
     attrDelimiter: String,
     aggBlmFilter: String): NamedExpression = {
    Alias(
      Md5(Cast(child = ConcatWs(
        List(Literal(attrDelimiter)) ++ attr.filter(attr => attrName.contains(attr.name))),
        dataType = BinaryType)), aggBlmFilter)()
  }

}
