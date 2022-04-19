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

package org.apache.spark.sql

import com.spark.radiant.sql.utils.SparkSqlUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Predicate}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.util.sketch.BloomFilter

case class PersistBloomFilterExpr(left: Expression,
    right: Expression) extends BinaryExpression with Predicate {

  override def prettyName: String = "persist_bloom_filter_expr"

  override def nullable: Boolean = true

  override protected def withNewChildrenInternal(
     newLeft: Expression,
     newRight: Expression): PersistBloomFilterExpr =
    copy(left = newLeft,
      right = newRight)

  override def doGenCode(ctx: CodegenContext, exprCode: ExprCode): ExprCode = {
    if (persistBloomFilter == null) {
      exprCode.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    } else {
      val bloomFilter = ctx.addReferenceObj("bloomFilter",
        persistBloomFilter, classOf[BloomFilter].getName)
      val valueEval = right.genCode(ctx)
      val mightContainValue = valueEval.value
      exprCode.copy(code = code"""
      ${valueEval.code}
      boolean ${exprCode.isNull} = ${valueEval.isNull};
      ${CodeGenerator.javaType(dataType)} ${exprCode.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${exprCode.isNull}) {
        byte[] arr = ((org.apache.spark.unsafe.types.UTF8String)${mightContainValue}).getBytes();
        String mightVal = new String(arr, java.nio.charset.StandardCharsets.UTF_8);
        ${exprCode.value} = $bloomFilter.mightContain(mightVal);
      }""")
    }
  }

  override def eval(input: InternalRow): Any = {
    if (persistBloomFilter == null) {
      null
    } else {
      val value = right.eval(input)
      if (value == null) null else persistBloomFilter.mightContain(value.toString)
    }
  }

  // The bloom filter created from `PersistBloomFilterExpr`.
  @transient private lazy val persistBloomFilter = {
    val utils = new SparkSqlUtils()
    val path = left.eval()
    val bf = utils.readBloomFilter(path.toString)
    bf
  }

  override def dataType: DataType = BooleanType
}
