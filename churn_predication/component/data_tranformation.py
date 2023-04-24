import os
import sys
from pyspark.ml.feature import OneHotEncoder,StringIndexer,VectorAssembler
from pyspark.ml.pipeline import Pipeline
from churn_predication.config.spark_manager import spark
from churn_predication.exception import ChurnException
from churn_predication.logger import logging
from pyspark.sql import DataFrame
from churn_predication.entity.config_entity import DataValidationConfig
from churn_predication.entity.config_entity import DataTransformationConfig
from churn_predication.entity.schema import ChurnDataSchema

class DataTransformation():
    def __init__(self,data_transformation_config =DataTransformationConfig(), data_validation_artifacts= DataValidationConfig(), schema= ChurnDataSchema()):
        try:
            super().__init__()
            self.file_path =data_validation_artifacts.clean_data_path()
            self.data_transformation_config = data_transformation_config
            self.schema = schema
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark.read.csv(self.file_path)
            return dataframe
        except Exception as e:
            raise ChurnException(e,sys)

    def get_data_transformation_pipeline(self, ) -> Pipeline:
        try:
           stages = [

            ]
           
           for im_one_hot_feature, string_indexer_col in zip(self.schema.im_one_hot_encoding_features,
                                                             self.schema.string_indexer_one_hot_features):
               string_indexer = StringIndexer(inputCol=im_one_hot_feature,outputCol=string_indexer_col)
               stages.append(string_indexer)
           one_hot_encoder = OneHotEncoder(inputCol=self.schema.string_indexer_one_hot_features,outputCol=self.schema.tf_one_hot_encoding_features)
           stages.append(one_hot_encoder)

           vector_assembler = VectorAssembler(inputCols=self.schema.input_features,
                                               outputCol=self.schema.vector_assembler_output)

           stages.append(vector_assembler)
           pipeline = Pipeline(
                stages=stages
            )
           return pipeline
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def initiate_data_transformation(self):
        try:
            logging.info(f">>>>>>>>>>>Started data transformation <<<<<<<<<<<<<<<")
            dataframe: DataFrame = self.read_data()
            logging.info(f"Number of row: [{dataframe.count()}] and column: [{len(dataframe.columns)}]")
            logging.info(f"Splitting dataset into train and test set using ration:")
            train_dataframe, test_dataframe = dataframe.randomSplit([0.8, 0.2])
            logging.info(f"Train dataset has number of row: [{train_dataframe.count()}] and"
                        f" column: [{len(train_dataframe.columns)}]")
            
            logging.info(f"Train dataset has number of row: [{test_dataframe.count()}] and"
                        f" column: [{len(test_dataframe.columns)}]")
            
            pipeline = self.get_data_transformation_pipeline()

            transformed_pipeline = pipeline.fit(train_dataframe)

            require_columns = [self.schema.vector_assembler_output,self.schema.target_column]
            transformed_trained_dataframe = transformed_pipeline.transform(train_dataframe)
            transformed_trained_dataframe = transformed_trained_dataframe.select(require_columns)
            transformed_trained_dataframe.write.csv(self.data_transformation_config.train_path, header='True')
            return (
                self.data_transformation_config.train_path
            )
        except Exception as e:
            raise ChurnException(e,sys)