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

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionSet, Generator, NamedExpression, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{PROJECT, TreePattern}

case class CustomProject(projectList: Seq[NamedExpression], child: LogicalPlan)
  extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def maxRows: Option[Long] = child.maxRows
  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  final override val nodePatterns: Seq[TreePattern] = Seq(PROJECT)

  override lazy val resolved: Boolean = {
    val hasSpecialExpressions = projectList.exists ( _.collect {
      case agg: AggregateExpression => agg
      case generator: Generator => generator
      case window: WindowExpression => window
    }.nonEmpty
    )

    !expressions.exists(!_.resolved) && childrenResolved && !hasSpecialExpressions
  }

  override lazy val validConstraints: ExpressionSet =
    getAllValidConstraints(projectList)

  override def metadataOutput: Seq[Attribute] =
    getTagValue(CustomProject.hiddenOutputTag).getOrElse(Nil)

  override protected def withNewChildInternal(newChild: LogicalPlan): CustomProject =
    copy(child = newChild)
}

object CustomProject {
  val hiddenOutputTag: TreeNodeTag[Seq[Attribute]] = TreeNodeTag[Seq[Attribute]]("hidden_output")
}
