import os
import sys
from churn_predication.component.data_ingestion import DataIngestion
from churn_predication.component.data_validation import DataValidation
from churn_predication.exception import ChurnException
from churn_predication.logger import logging

class TrainingPipeline:
    def __init__(self):
        pass

    def start_data_ingestion(self):
        logging.info('start trainning pipeline start_data_ingestion  method')
        try:
            data_ingestion = DataIngestion()
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info('exited start_data_ingestion method pipeline class')
            return data_ingestion_artifact
            
        except Exception as e:
            raise ChurnException(e,sys)
        
    def start_data_validation(self):
        try:
            data_validation = DataValidation()
            data_validation_artifact = data_validation.initiate_data_validation()
            return data_validation_artifact
        except Exception as e:
            raise ChurnException(e,sys)
        

    def start(self):
        try:
            data_ingestion_artifacts = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation()
        except Exception as e:
            raise ChurnException(e,sys)