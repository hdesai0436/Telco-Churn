from churn_predication.logger import logging
from churn_predication.exception import ChurnException
from churn_predication.config.spark_manager import spark
import os
import sys
from pyspark.sql import DataFrame
from churn_predication.entity.artifact_entity import DataIngestionArtifact

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact):
         self.data_ingestion_artifact: DataIngestionArtifact = data_ingestion_artifact
    

    def read_data(self) -> DataFrame:
         try:
              
              dataframe: DataFrame = spark.read_excel( self.data_ingestion_artifact.raw_data).limit(10)
              return dataframe

         except Exception as e:
              logging.exception(e)
              raise ChurnException(e,sys)

        

