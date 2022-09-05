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
  private def addPersistBloomFilter(plan: LogicalPlan,
     hint: UnresolvedHint): LogicalPlan = {
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
        addPersistBloomFilter(child, hint)
      case _ => hint
    }
  }
}
