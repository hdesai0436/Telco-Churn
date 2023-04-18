import os
import sys
from churn_predication.logger import logging
from churn_predication.exception import ChurnException
from churn_predication.entity.config_entity import DataIngestionConfig

import pandas as pd


class DataIngestion:
    def __init__(self):
        self.ingestion_config =DataIngestionConfig()




    def initiate_data_ingestion(self):
        logging.info('Enter the data ingestion method')
        try:
            df = pd.read_excel('data\Telco_customer_churn.xlsx')
            logging.info('read dataset as dataframe')
            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path),exist_ok=True)
            
            df.to_excel(self.ingestion_config.raw_data_path)
            logging.info(f"Data is downloaded in {self.ingestion_config.raw_data_path}")
            return (
                self.ingestion_config.raw_data_path
            )

        except Exception as e:
            raise(ChurnException(e,sys))

if __name__ == '__main__':
    obj = DataIngestion()
    obj.initiate_data_ingestion()