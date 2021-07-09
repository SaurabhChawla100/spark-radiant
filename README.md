# spark-radiant

spark-radiant is Apache Spark Performance Optimizer Project.

### Providing Spark as a Service with your own Fork of Spark

One of the main problem for providing the Spark as a Service is to maintain your own code base with the frequent changes in the Open Source
Spark. New version of Apache Spark will release in every six months.So Inorder to bring the newer version of Spark, it will take a lot
of time and changes in the codebase to merge your changes into the Spark Branch.

### Why don't the organization merge all the Code in the Open Source Spark instead of having their own fork of Spark ?

Below are the few scenarios for maintaining own fork
1) There are certain features that are applicable to use case of the organization only.
2) Open Source Community has not accepted the change for any reason, But the change works for your Customers.
3) Last but most important, the organization does not want to open source the change for the competitive reason.

### What if ?

1) We have the liberty of moving the with pace of changes in Apache Spark master.
2) We can release Spark on the same day the new version of Apache Spark Released.
3) We have our owm Spark Optimizer Project and integrate with the Runtime Spark.
4) Maintain our own Project and update that project as per the new changes in Spark.
5) Even we don't have to modify our project in some new release with Apache Spark

### Turning what if into reality !

spark-radiant is Apache Spark Performance Optimizer Project.The main idea is to build some optimization features on top of Apache Spark
to improve the performance optimization and reduce total cost optimization. This project will have some optimization related to
catalyst optimizer rules, Enhanced AutoScaling in Spark, Collecting Important Metrics related to spark job, BloomFilter Index in Spark etc.

## How to use Spark-Radiant

**Build** 

This project used the Maven Build.

git clone https://github.com/SaurabhChawla100/spark-radiant.git

cd spark-radiant

mvn clean install -DskipTests / mvn clean package -DskipTests 

**Add the Dependency to your project**

Build the project locally and add the dependency to your project

For Sql Optimization(Ready for use)

```
<dependency>
    <groupId>com.github.saurabhchawla100</groupId>
    <artifactId>spark-radiant-sql</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

For Core Optimization (WIP)
```
<dependency>
    <groupId>com.github.saurabhchawla100</groupId>
    <artifactId>spark-radiant-core</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>

```

### Prerequisite
spark-radiant have the below prerequisites

a) This is supported with spark-3.0.x and latter version of spark
b) Supported scala version 2.12.x

### Use spark-radiant at runtime
This spark-radiant project has 2 modules, you can use those modules in your project

1) **spark-radiant-sql** - This contains the optimization related to Spark Sql Performance Improvement

   a) **Using Dynamic Filtering in Spark** - Please refer the docs for [Dynamic Filtering](docs/dynamicFilterInSpark.md).

   b) **Add multiple value in the withColumn Api of Spark** - This is WIP

   c) **BloomFilter Index** - This is WIP

2) **spark-radiant-core** - This contains the optimization related to total cost optimization.
   
   a) **Enhanced Autoscaling in Spark**- This is WIP
   
   b) **Metrics Collector** - This is WIP
