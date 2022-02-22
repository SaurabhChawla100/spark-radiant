# spark-radiant 1.0.3 Release Notes

a) **Metrics Collector** - This Metrics Collector is newly added as part of spark-radiant-core module.This helps in getting the overall 
information about how the spark Application ran.

SparkJobMetricsCollector is used for collecting the important metrics to Spark Job metrics,
Stage metrics, Task Metrics(Task Failure info, Task skewness info).

This is enabled by using the configuration
--conf spark.extraListeners=com.spark.radiant.core.SparkJobMetricsCollector and providing the jars in the class path
using
   
Steps to run
  
 ```
   ./bin/spark-shell --conf spark.extraListeners=com.spark.radiant.core.SparkJobMetricsCollector --packages "io.github.saurabhchawla100:spark-radiant-sql:1.0.3,io.github.saurabhchawla100:spark-radiant-core:1.0.3"
 ```

 ```
       
       Spark-Radiant Metrics Collector
       Total Time taken by Application:: 895 sec
       
       *****Driver Metrics*****
       Time spend in the Driver: 307 sec
       Percentage of time spend in the Driver: 34. Try adding more parallelism to the Spark job for Optimal Performance
       
       *****Stage Info Metrics*****
       ***** Stage Info Metrics Stage Id:0 *****
       {
       "Stage Id": 0,
       "Final Stage Status": succeeded,
       "Number of Task": 10,
       "Total Executors ran to complete all Task": 2,
       "Stage Completion Time": 858 ms,
       "Average Task Completion Time": 139 ms
       "Number of Task Failed in this Stage": 0
       "Few Skew task info in Stage": Skew task in not present in this stage
       "Few Failed task info in Stage": Failed task in not present in this stage
       }
       ***** Stage Info Metrics Stage Id:1 *****
       {
       "Stage Id": 1,
       "Final Stage Status": succeeded,
       "Number of Task": 10,
       "Total Executors ran to complete all Task": 2,
       "Stage Completion Time": 53 ms,
       "Average Task Completion Time": 9 ms
       "Number of Task Failed in this Stage": 0
       "Few Skew task info in Stage": Skew task in not present in this stage
       "Few Failed task info in Stage": Failed task in not present in this stage
       }
       ***** Stage Info Metrics Stage Id:2 *****
       {
       "Stage Id": 2,
       "Final Stage Status": succeeded,
       "Number of Task": 100,
       "Total Executors ran to complete all Task": 4,
       "Stage Completion Time": 11206 ms,
       "Average Task Completion Time": 221 ms
       "Number of Task Failed in this Stage": 0
       "Few Skew task info in Stage": List({
       "Task Id": 0,
       "Executor Id": 3,
       "Number of records read": 11887,
       "Number of shuffle read Record": 11887,
       "Number of records write": 0,
       "Number of shuffle write Record": 0,
       "Task Completion Time": 10656 ms
       "Final Status of task": SUCCESS
       "Failure Reason for task": NA
       }, {
       "Task Id": 4,
       "Executor Id": 1,
       "Number of records read": 11847,
       "Number of shuffle read Record": 11847,
       "Number of records write": 0,
       "Number of shuffle write Record": 0,
       "Task Completion Time": 10013 ms
       "Final Status of task": SUCCESS
       "Failure Reason for task": NA
       })
       "Few Failed task info in Stage": Failed task in not present in this stage
       }
       ***** Stage Info Metrics Stage Id:3 *****
       {
       "Stage Id": 3,
       "Final Stage Status": failed,
       "Number of Task": 10,
       "Total Executors ran to complete all Task": 2,
       "Stage Completion Time": 53 ms,
       "Average Task Completion Time": 9 ms
       "Number of Task Failed in this Stage": 1
       "Few Skew task info in Stage": Skew task in not present in this stage
       "Few Failed task info in Stage": List({
       "Task Id": 12,
       "Executor Id": 1,
       "Number of records read in task": 0,
       "Number of shuffle read Record in task": 0,
       "Number of records write in task": 0,
       "Number of shuffle write Record in task": 0,
       "Final Status of task": FAILED,
       "Task Completion Time": 7 ms,
       "Failure Reason for task": java.lang.Exception: Retry Task
             at $line14.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.$anonfun$res0$1(<console>:33)
             at scala.runtime.java8.JFunction1$mcII$sp.apply(JFunction1$mcII$sp.java:23)
             at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
             at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
       })
       }
  ```

b) **Add the support for struct type column in the DropDuplicate** - Till now on using the struct col
    in the DropDuplicate we will get the below exception.

```
            case class StructDropDup(c1: Int, c2: Int)
            val df = Seq(("d1", StructDropDup(1, 2)),
            ("d1", StructDropDup(1, 2))).toDF("a", "b")
            df.dropDuplicates("a", "b.c1")
         
            org.apache.spark.sql.AnalysisException: Cannot resolve column name "b.c1" among (a, b)
            at org.apache.spark.sql.Dataset.$anonfun$dropDuplicates$1(Dataset.scala:2576)
            at scala.collection.TraversableLike.$anonfun$flatMap$1(TraversableLike.scala:245)
            at scala.collection.Iterator.foreach(Iterator.scala:941)
            at scala.collection.Iterator.foreach$(Iterator.scala:941)
            at scala.collection.AbstractIterator.foreach(Iterator.scala:1429)
```

      
  Added the support to use the struct col in the DropDuplicate
 
  
```
            import com.spark.radiant.sql.api.SparkRadiantSqlApi
            case class StructDropDup(c1: Int, c2: Int)
            val df = Seq(("d1", StructDropDup(1, 2)),
            ("d1", StructDropDup(1, 2))).toDF("a", "b")
            val sparkRadiantSqlApi = new SparkRadiantSqlApi()
            val updatedDF = sparkRadiantSqlApi.dropDuplicateOfSpark(df, spark, Seq("a", "b.c1"))
               
            updatedDF.show
               +---+------+
               |  a|     b|
               +---+------+
               | d1|{1, 2}|
               +---+------+
   
```
The same support is added in this PR in Apache Spark
[SPARK-37596][Spark-SQL] Add the support for struct type column in the DropDuplicate
   
This works well for the map type column in the dropDuplicate
      
```
         
         val df = spark.createDataFrame(Seq(("d1", Map(1 -> 2)), ("d1", Map(1 -> 2))))
         val updatedDF = sparkRadiantSqlApi.dropDuplicateOfSpark(df, spark, Seq("_2.1"))
         updatedDF.show
         
         +---+--------+
         | _1|      _2|
         +---+--------+
         | d1|{1 -> 2}|
         +---+--------+
```

c) **Improvement in Dynamic Filter** - There some new features and improvement added in the Dynamic Filter for the spark-radiant 1.0.3

   1) Dynamic filter support is available in scala, pyspark, spark-sql, JavaSpark, R. This is done by 
      importing the rule for Dynamic Filter on the catalyst optimizer of the Apache Spark.
```
      ./bin/spark-shell
      --packages "io.github.saurabhchawla100:spark-radiant-sql:1.0.3,io.github.saurabhchawla100:spark-radiant-core:1.0.3"
      --conf spark.sql.extensions=com.spark.radiant.sql.api.SparkRadiantSqlExtension
    
        ./bin/spark-submit
        --packages "io.github.saurabhchawla100:spark-radiant-sql:1.0.3,io.github.saurabhchawla100:spark-radiant-core:1.0.3"
        --class com.test.spark.examples.SparkTestDF /spark/examples/target/scala-2.12/jars/spark-test_2.12-3.2.1.jar
        --conf spark.sql.extensions=com.spark.radiant.sql.api.SparkRadiantSqlExtension
```

   2) Support for Dynamic Filter is added in the DataSourceV2 tables and InMemory Relation.
     ![Dynamic Filter in DataSourceV2](../Snapshots/DynamicFilterDSv2.png)

   3) PushDown Dynamic Filter to FileScan for V2 datasource works for ORC and Parquet.This will work with Spark-3.1.1 and later version of spark.
   4) spark.sql.dynamicFilter.pushdown.allJoinKey - This conf is used to push all the join key values derived from the other side of the join.
      This will help in exact data-skipping compared to only one key value used for data-skipping. This will provide better performance
      compared to pushing only one join key value to datasource as the part of pushed filter. The default value is true. 
      This will be available from spark-radiant-sql 1.0.3.
   5) Creating a bloomfilter from the RDD in the Dynamic Filter - This will add the improvement in creating the Bloomfilter
      used as part of Dynamic filter.

d) **Improvement in Size based Join Reordering**  
        
   1) Support for Size based Join Reordering is added in the DataSourceV2 tables.
   2) Size based Join Reordering support is available in scala, pyspark, spark-sql, Java, R using the conf 
     `--conf spark.sql.extensions=com.spark.radiant.sql.api.SparkRadiantSqlExtension`.
   3) Fix the support for size based join reordering when select column of only one table present.
   
e) **Improvement in UnionReuseExchangeOptimizeRule** - 
   1) Support for UnionReuseExchangeOptimizeRule is added in the DataSourceV2 tables.
   2)  UnionReuseExchangeOptimizeRule support is available in scala, pyspark, spark-sql, Java, R using the conf
    `--conf spark.sql.extensions=com.spark.radiant.sql.api.SparkRadiantSqlExtension`.
