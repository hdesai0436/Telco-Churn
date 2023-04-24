from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

spark = SparkSession.builder.appName('churn').config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").getOrCreate()
