import os
import sys
from churn_predication.logger import logging
from churn_predication.exception import ChurnException
from churn_predication.entity.config_entity import DataIngestionConfig
from churn_predication.config.spark_manager import spark
from pyspark.sql.functions import *

import pandas as pd


class DataIngestion:
    def __init__(self):
        self.ingestion_config =DataIngestionConfig()

        
    def initiate_data_ingestion(self):
        logging.info('Exporting data from mongodb to atifacts folder as raw data')
        try:
            df = spark.read.format("mongodb").option('database', 'curn_database').option('collection', 'data_churn').load()
            logging.info('read dataset as dataframe')
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path),exist_ok=True)
            
            df.write.csv(self.ingestion_config.raw_data_path,
                  header=True)
            logging.info(f"Data is downloaded in {self.ingestion_config.raw_data_path}")
            logging.info('data Ingestion part is complated')
            return (
                self.ingestion_config.raw_data_path
            )
        except Exception as e:
            logging.exception(e)
            raise(ChurnException(e,sys))

