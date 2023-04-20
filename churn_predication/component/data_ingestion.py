import os
import sys
from churn_predication.logger import logging
from churn_predication.exception import ChurnException
from churn_predication.entity.config_entity import DataIngestionConfig
from churn_predication.config.spark_manager import spark


import pandas as pd


class DataIngestion:
    def __init__(self):
        self.ingestion_config =DataIngestionConfig()

        
    def initiate_data_ingestion(self):
        logging.info('Enter the data ingestion method')
        try:
            df = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").option("inferSchema", "true").load('data\Telco_customer_churn.xlsx')  
            logging.info('read dataset as dataframe')
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path),exist_ok=True)
            
            df.write.csv(self.ingestion_config.raw_data_path,index = None,
                  header=True)
            logging.info(f"Data is downloaded in {self.ingestion_config.raw_data_path}")
            logging.info('data Ingestion part is complated')
            return (
                self.ingestion_config.raw_data_path
            )
        except Exception as e:
            logging.exception(e)
            raise(ChurnException(e,sys))

if __name__ == '__main__':
    obj = DataIngestion()
    obj.initiate_data_ingestion()