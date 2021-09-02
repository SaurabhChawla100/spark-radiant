import sys

from pyspark.sql import SparkSession, DataFrame
from py4j.java_gateway import java_import

class SparkRadiantSqlApi:
    def __init__(self, spark):
        """
        Initializes SparkRadiantSqlApi object.
        :return: SparkRadiantSqlApi object
        """
        self.spark = spark

    def addExtraOptimizerRule(self):
        self.spark._jvm.com.spark.radiant.sql.api.SparkRadiantSqlApi().addOptimizerRule(self.spark._jsparkSession)
