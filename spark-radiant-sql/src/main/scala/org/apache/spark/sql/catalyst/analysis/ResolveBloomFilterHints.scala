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

package org.apache.spark.sql.catalyst.analysis

import com.spark.radiant.sql.index.BloomFilterIndexImpl
import org.apache.spark.sql.catalyst.expressions.StringLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule

import java.util.Locale

object ResolveBloomFilterHints extends Rule[LogicalPlan] {
  val BloomFilter_Hints: Set[String] =
    Set("PERSIST_BLOOM_FILTER")

 private def applyBloomFilterToPlan(plan: LogicalPlan,
     path: String, attrName: Seq[Any]): LogicalPlan = {
   val index = new BloomFilterIndexImpl()
   index.applyBloomFilterToPlan(plan, path, attrName.map(x => x.toString).toList)
  }

  /**
   * This function handles hints for "BloomFilter".
   * The "BloomFilter" hint
   * adds the BloomFilter in the Sql Plan.
   */
  private def addPersistBloomFilter(hint: UnresolvedHint,
     plan: LogicalPlan): LogicalPlan = {
    val hintName = hint.name.toUpperCase(Locale.ROOT)
    hint.parameters match {
      case param @ Seq(StringLiteral(path), _*) => applyBloomFilterToPlan(plan, path, param.tail)
    }
  }

  /**
   * Resolve BLOOM_FILTER Hint to Spark SQL
   * @param plan
   * @return
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case hint @ UnresolvedHint(hintName, _, child) => hintName.toUpperCase(Locale.ROOT) match {
      case "PERSIST_BLOOM_FILTER" =>
        addPersistBloomFilter(hint, child)
      case _ => hint
    }
  }
}
