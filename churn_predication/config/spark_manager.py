from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
spark = SparkSession.\
builder.\
appName("pyspark-notebook2").\
config("spark.executor.memory", "1g").\
config("spark.mongodb.read.connection.uri","mongodb+srv://desai:desai123@cluster0.kkmblvo.mongodb.net/?retryWrites=true&w=majority").\
config("spark.mongodb.write.connection.uri","mongodb+srv://desai:desai123@cluster0.kkmblvo.mongodb.net/?retryWrites=true&w=majority").\
config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3").\
getOrCreate()