import os
import sys
from churn_predication.logger import logging
from churn_predication.entity.config_entity import ModelTrainerConfig
from churn_predication.entity.config_entity import DataValidationConfig,ModelEvulationConfig,Modelevulationartifacts
from churn_predication.entity.schema import ChurnDataSchema
from churn_predication.config.spark_manager import spark
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from churn_predication.utils import get_score
from churn_predication.constant import *

from sklearn.metrics import classification_report,confusion_matrix
from churn_predication.entity.estimator import S3Estimator
import re

class ModelEvaluation:
    def __init__(self, modeltrain_artifacts= ModelTrainerConfig(), data_validation_artifacts= DataValidationConfig(),model_evulation = ModelEvulationConfig(),schema=ChurnDataSchema()):
        super().__init__()
        self.file_path =data_validation_artifacts
        self.modeltrain_artifacts = modeltrain_artifacts
        
        self.schema = schema
        self.bucket_name = model_evulation.bucket_name
        self.s3_model_dir_key = model_evulation.model_dir_key
        self.model_dir= model_evulation.model_dir
        self.best_model_download = None
    
    def read_data(self) -> DataFrame:
        try:
            
            dataframe: DataFrame = spark.read.csv(self.file_path.test_data_file_path,header='true',schema=self.schema.dataframe_schema)
            logging.info('Read the test data')
            return dataframe
        except Exception as e:
            raise e


    def eval(self):
        is_model_accepted, is_active = False, False
        dataframe: DataFrame = self.read_data()
        dataframe: DataFrame = dataframe.na.drop()
        trained_model_file_path = self.modeltrain_artifacts.trained_model_file_path
        trained_model = PipelineModel.load(trained_model_file_path)

       
        
        predictionAndTarget = trained_model.transform(dataframe)
        m = S3Estimator(bucket_name=self.bucket_name,model_key=self.s3_model_dir_key,model_dir = self.model_dir)
        best_model_path = m.get_latest_model_path(key=self.s3_model_dir_key)
    
        if self.best_model_download != best_model_path:
            m.load(key=self.s3_model_dir_key,extract_dir=self.model_dir)
        best_model_path = m.get_latest_model_path_folder_download()
        best_model = PipelineModel.load(best_model_path) 
        best_model_result = best_model.transform(dataframe)
        
        trained_model_f1_score = get_score(dataframe=predictionAndTarget, metric_name=f'weightedRecall',
                                            label_col="labelIndex",
                                            prediction_col="prediction")
        best_model_f1_score = get_score(dataframe=best_model_result, metric_name=f'weightedRecall',
                                            label_col="labelIndex",
                                            prediction_col="prediction")

        changed_accuracy = trained_model_f1_score - best_model_f1_score
        if changed_accuracy >= .20:
            is_model_accepted = True

        model_evaluation_artifacts = Modelevulationartifacts(model_accepted=is_model_accepted)
        return model_evaluation_artifacts

    def intiate_model_evaluation(self):
        try:
            model_accepted = True
            if not S3Estimator(bucket_name=self.bucket_name,model_key=self.s3_model_dir_key,model_dir = self.model_dir).is_model_available(key=self.s3_model_dir_key):
                model_evaluation_artifact = Modelevulationartifacts(model_accepted=model_accepted)
            else:
                model_evaluation_artifact = self.eval()
            return model_evaluation_artifact

        except Exception as e:
            raise ChurnException(e,sys)

       


# if __name__ == '__main__':
#     a = ModelEvaluation()
#     a.eval()
