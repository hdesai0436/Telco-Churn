import os
import sys
from churn_predication.component.data_ingestion import DataIngestion
from churn_predication.component.data_validation import DataValidation
from churn_predication.component.data_tranformation import DataTransformation
from churn_predication.component.model_trainer import ModelTrainer
from churn_predication.component.model_eval import ModelEvaluation
from churn_predication.exception import ChurnException
from churn_predication.logger import logging

class TrainingPipeline:
    def __init__(self):
        pass

    def start_data_ingestion(self):
        
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
        
    def start_data_transformation(self):
        try:
            data_transformation = DataTransformation()
            data_transformation_artifacts = data_transformation.initiate_data_transformation()
            return data_transformation_artifacts
        except Exception as e:
            raise ChurnException(e,sys)

    def start_model_trainer(self):
        try:
            model_trainer = ModelTrainer()
            model_trainer_artifacts = model_trainer.initiate_model_training()
            return model_trainer
        except Exception as e:
            raise ChurnException(e,sys)
        
    def eval(self):
        e = ModelEvaluation()
        a = e.eval()
        return a

    def start(self):
        try:
            data_ingestion_artifacts = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation()
            data_transformation_artifacts = self.start_data_transformation()
            model_train = self.start_model_trainer()
            ev = self.eval()
        except Exception as e:
            raise ChurnException(e,sys)

if __name__ == '__main__':
    a = TrainingPipeline()
    a.start()