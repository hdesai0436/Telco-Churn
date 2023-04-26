import os
import sys
from pyspark.ml.feature import OneHotEncoder,StringIndexer,VectorAssembler,StandardScaler
from pyspark.ml.pipeline import Pipeline
from churn_predication.config.spark_manager import spark
from churn_predication.exception import ChurnException
from churn_predication.logger import logging
from pyspark.sql import DataFrame
from churn_predication.entity.config_entity import DataValidationConfig
from churn_predication.entity.config_entity import DataTransformationConfig
from churn_predication.entity.schema import ChurnDataSchema
import pyspark.sql.functions as F
from pyspark.sql.functions import *

class DataTransformation():
    def __init__(self,data_transformation_config =DataTransformationConfig(), data_validation_artifacts= DataValidationConfig(), schema= ChurnDataSchema()):
        try:
            super().__init__()
            self.file_path =data_validation_artifacts
            self.data_transformation_config = data_transformation_config
            self.schema = schema
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def read_data(self) -> DataFrame:
        try:
            logging.info('Reading clean train data csv file')
            print(f'{self.file_path.train_data_file_path}')
            dataframe: DataFrame = spark.read.csv(self.file_path.train_data_file_path,header='true',schema=self.schema.dataframe_schema)
            return dataframe
        except Exception as e:
            raise ChurnException(e,sys)
        
    

    def get_data_transformation_pipeline(self, ) -> Pipeline:
        logging.info('start get_data_transformation pipeline')
        try:
           stages = [

            ]
           
           label_indexer = StringIndexer(inputCol="Churn Label", outputCol="labelIndex")
           stages.append(label_indexer)
           logging.info('finished label StringIndexer')
           for im_one_hot_feature, string_indexer_col in zip(self.schema.one_hot_encoding_features,
                                                             self.schema.string_indexer_one_hot_features):
               string_indexer = StringIndexer(inputCol=im_one_hot_feature,outputCol=string_indexer_col)
               stages.append(string_indexer)
           logging.info('finished categorical feature  StringIndexer')
           one_hot_encoder = OneHotEncoder(inputCols=self.schema.string_indexer_one_hot_features,outputCols=self.schema.tf_one_hot_encoding_features)
           stages.append(one_hot_encoder)
           logging.info('finished one hot encoder on categorical feature')
           vector_assembler = VectorAssembler(inputCols=self.schema.input_feature,
                                               outputCol=self.schema.vector_assembler_output)

           stages.append(vector_assembler)
           logging.info('finished VectorAssembler feature')
           standard_scaler = StandardScaler(inputCol=self.schema.vector_assembler_output,
                                             outputCol=self.schema.scaled_vector_input_features)
           stages.append(standard_scaler)
           logging.info('finished standard_scaler feature')
           pipeline = Pipeline(
                stages=stages
            )
           logging.info('finished get_data_transformation pipeline')
           return pipeline
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def initiate_data_transformation(self):
        try:
            logging.info(f">>>>>>>>>>>Started data transformation <<<<<<<<<<<<<<<")
            dataframe: DataFrame = self.read_data()
            logging.info('drop null values row')
            dataframe: DataFrame = dataframe.na.drop()
            logging.info(f"Number of row: [{dataframe.count()}] and column: [{len(dataframe.columns)}]")
            
            pipeline = self.get_data_transformation_pipeline()

            transformed_pipeline = pipeline.fit(dataframe)

            require_columns = [self.schema.scaled_vector_input_features,'labelIndex']
            transformed_dataframe = transformed_pipeline.transform(dataframe)
            transformed_dataframe = transformed_dataframe.select(require_columns)
           
    
            
            os.makedirs(self.data_transformation_config.transformed_train_data_dir,exist_ok=True)
            export_pipeline_file_path = self.data_transformation_config.export_pipeline_dir
            
            logging.info(f"Saving transformation pipeline at: [{export_pipeline_file_path}]")
            transformed_pipeline.save(export_pipeline_file_path)
            logging.info(f"Saving transformed train data at: [{self.data_transformation_config.transformed_train_data_file_path}]")
            transformed_dataframe.write.parquet(self.data_transformation_config.transformed_train_data_file_path)
           
            
            return (
                 self.data_transformation_config.export_pipeline_dir,
                 self.data_transformation_config.transformed_train_data_file_path
                 
            )
        except Exception as e:
            raise ChurnException(e,sys)