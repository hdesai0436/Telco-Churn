from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from dotenv import load_dotenv
load_dotenv()
import os

__access_key_id = os.environ.get('AWS_ACCESS_KEY_ID_ENV_KEY',)
__secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY_ENV_KEY',)

spark = SparkSession.\
builder.\
appName("pyspark-notebook2").\
config("spark.executor.memory", "1g").\
config("spark.mongodb.read.connection.uri","mongodb+srv://desai:desai123@cluster0.kkmblvo.mongodb.net/?retryWrites=true&w=majority").\
config("spark.mongodb.write.connection.uri","mongodb+srv://desai:desai123@cluster0.kkmblvo.mongodb.net/?retryWrites=true&w=majority").\
config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3").\
config('spark.jars.packages',"com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3").\
getOrCreate()


spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", __access_key_id )
spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", __secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "'us-east-1.amazonaws.com")
spark._jsc.hadoopConfiguration().set(" fs.s3.buffer.dir","tmp")