import os
import sys
from pyspark.ml.feature import OneHotEncoder,StringIndexer,VectorAssembler,StandardScaler
from pyspark.ml.pipeline import Pipeline,PipelineModel
from churn_predication.config.spark_manager import spark
from churn_predication.exception import ChurnException
from churn_predication.logger import logging
from pyspark.sql import DataFrame
from churn_predication.entity.config_entity import DataTransformationConfig
from churn_predication.entity.config_entity import ModelTrainerConfig
from churn_predication.entity.schema import ChurnDataSchema
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from typing import List


class ModelTrainer:
    def __init__(self,modeltrain_artifacts= ModelTrainerConfig(), data_transformation_artifact = DataTransformationConfig(),schema=ChurnDataSchema()):
        super().__init__()
        self.file_path = data_transformation_artifact
        self.modeltrain_artifacts = modeltrain_artifacts
        self.schema = schema
        

    
    def get_train_and_test_dataframe(self) -> DataFrame:
        try:
            logging.info(f'Getting trainning data from {self.file_path.transformed_train_data_file_path}')
            train_file_path = self.file_path.transformed_train_data_file_path
          
            train_dataframe: DataFrame = spark.read.parquet(train_file_path)
            
            dataframes:DataFrame = train_dataframe
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
            logging.info('Finieshed creating model')
            return pipeline
        except Exception as e:
            raise ChurnException(e,sys)
        
    def export_train_model(self,model:PipelineModel):
        try:
            logging.info(f'Getting Transformation model pipeline from {self.file_path.export_pipeline_dir}')
            transformed_pipeline_path = self.file_path.export_pipeline_dir
            tranformed_pipeline = PipelineModel.load(transformed_pipeline_path)
            update_stages = tranformed_pipeline.stages + model.stages
            tranformed_pipeline.stages = update_stages
            logging.info("Creating trained model directory")
            os.makedirs(os.path.dirname(self.modeltrain_artifacts.trained_model_file_path), exist_ok=True)
            tranformed_pipeline.save(self.modeltrain_artifacts.trained_model_file_path)
            logging.info(f'"Model trainer reference artifact: {self.modeltrain_artifacts.trained_model_file_path}')
            return (self.modeltrain_artifacts.trained_model_file_path)
        except Exception as e:
            raise ChurnException(e,sys)


    

        
    def initiate_model_training(self):
        try:
            logging.info('start model training')
            dataframes = self.get_train_and_test_dataframe()
            
            model = self.get_model()
            trained_model = model.fit(dataframes)
            ref_artifact = self.export_train_model(model=trained_model)

            return ref_artifact
           
            
        except Exception as e:
            raise ChurnException(e,sys)
    
        
    


