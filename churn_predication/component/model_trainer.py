import os
import sys
from pyspark.ml.feature import OneHotEncoder,StringIndexer,VectorAssembler,StandardScaler
from pyspark.ml.pipeline import Pipeline,PipelineModel
from churn_predication.config.spark_manager import spark
from churn_predication.exception import ChurnException
from churn_predication.logger import logging
from pyspark.sql import DataFrame
from churn_predication.entity.config_entity import DataTransformationConfig
from churn_predication.entity.schema import ChurnDataSchema
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from typing import List


class ModelTrainer:
    def __init__(self, data_transformation_artifact = DataTransformationConfig(),schema=ChurnDataSchema()):
        super().__init__()
        self.file_path = data_transformation_artifact
        self.schema = schema
        

    
    def get_train_and_test_dataframe(self) -> List[DataFrame]:
        try:
            train_file_path = self.file_path.train_data_file_path
            test_file_path = self.file_path.test_data_file_path
            train_dataframe: DataFrame = spark.read.parquet(train_file_path)
            test_dataframe: DataFrame = spark.read.parquet(test_file_path)
            dataframes: List[DataFrame] = [train_dataframe, test_dataframe]
            return dataframes
        except Exception as e:
            raise ChurnException(e,sys)
        
    def get_model(self) -> Pipeline:
        try:
            stages = []
            logging.info('Creating Gradient Booting classifiler')
            gbt = GBTClassifier(labelCol='labelIndex', featuresCol=self.schema.scaled_vector_input_features,maxIter=20 , maxDepth=20)
            stages.append(gbt)
            pipeline = Pipeline(stages=stages)
            return pipeline
        except Exception as e:
            raise ChurnException(e,sys)
        
    def export_train_model(self,model:PipelineModel):
        try:
            transformed_pipeline_path = self.file_path.export_pipeline_file_path
            tranformed_pipeline = PipelineModel.load(transformed_pipeline_path)
            update_stages = tranformed_pipeline.stages + model.stages
            tranformed_pipeline.stages = update_stages
        except Exception as e:
            raise ChurnException(e,sys)

        
    def initiate_model_training(self):
        try:
            dataframes = self.get_train_and_test_dataframe()
            train_dataframe, test_dataframe = dataframes[0], dataframes[1]
            model = self.get_model()
            trained_model = model.fit(train_dataframe)
            evaluatorMulti = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction")
            evaluator = BinaryClassificationEvaluator(labelCol="labelIndex", rawPredictionCol="prediction", metricName='areaUnderROC')
            predictionAndTarget = trained_model.transform(test_dataframe).select("genderIndex", "prediction")


            acc = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "accuracy"})
            print(acc)
        except Exception as e:
            raise ChurnException(e,sys)
    
        
    


