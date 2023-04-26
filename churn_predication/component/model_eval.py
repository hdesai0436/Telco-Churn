import os
import sys
from churn_predication.logger import logging
from churn_predication.entity.config_entity import ModelTrainerConfig
from churn_predication.entity.config_entity import DataValidationConfig
from churn_predication.entity.schema import ChurnDataSchema
from churn_predication.config.spark_manager import spark
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from churn_predication.utils import get_score
from churn_predication.constant import *

class ModelEvaluation:
    def __init__(self, modeltrain_artifacts= ModelTrainerConfig(), data_validation_artifacts= DataValidationConfig(),schema=ChurnDataSchema()):
        super().__init__()
        self.file_path =data_validation_artifacts
        self.modeltrain_artifacts = modeltrain_artifacts
        
        self.schema = schema
   
    
    def read_data(self) -> DataFrame:
        try:
            
            dataframe: DataFrame = spark.read.csv(self.file_path.test_data_file_path,header='true',schema=self.schema.dataframe_schema)
           
            return dataframe
        except Exception as e:
            raise e


    def eval(self):

        dataframe: DataFrame = self.read_data()
        dataframe: DataFrame = dataframe.na.drop()
        trained_model_file_path = self.modeltrain_artifacts.trained_model_file_path
        trained_model = PipelineModel.load(trained_model_file_path)

      
        predictionAndTarget = trained_model.transform(dataframe).select("labelIndex", "prediction")
        for i in MODEL_TRAINER_MODEL_METRIC_NAMES:
            i = get_score(dataframe=predictionAndTarget, metric_name=f'{i}',
                                            label_col="labelIndex",
                                            prediction_col="prediction")
            print(i)
        # acc = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "accuracy"})

        # f1 = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "f1"})
        # weightedPrecision = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "weightedPrecision"})
        # weightedRecall = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "weightedRecall"})
        # auc = evaluator.evaluate(predictionAndTarget)
        # print(auc)



