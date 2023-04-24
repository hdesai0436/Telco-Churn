from pymongo import MongoClient
from churn_predication.config.spark_manager import spark
from pyspark import SparkContext
from pyspark.sql import SQLContext


class ChurnData:
    def __init__(self):
        self.connection_string = "mongodb+srv://desai:desai123@cluster0.kkmblvo.mongodb.net/?retryWrites=true&w=majority"
        self.client = MongoClient(self.connection_string)

    def get_database(self):
        db = self.client.curn_database
        return db
    def get_collection(self):
        db = self.get_database()
        return db.data_churn
    
    def read_data(self):
        sc = SparkContext()
        s = spark(sc)
        data = s.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://+desai:desai123@server_details:27017/curn_database.data_churn?authSource=admin").load()









