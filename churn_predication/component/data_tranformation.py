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
            self.file_path =data_validation_artifacts.clean_data_path
            self.data_transformation_config = data_transformation_config
            self.schema = schema
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark.read.csv(self.file_path,header='true',schema=self.schema.dataframe_schema)
            return dataframe
        except Exception as e:
            raise ChurnException(e,sys)
        
    def balance_dataset(self) -> DataFrame:
        try:
            logging.info('starting balancing dataset')
            dataframe: DataFrame = self.read_data()
            major_df = dataframe.filter(col("Churn Label") == 'No')
            minor_df = dataframe.filter(col("Churn Label") == 'Yes')
            ratio = int(major_df.count()/minor_df.count())

            oversampled_df = minor_df.sample(withReplacement=True, fraction=2.6, seed=1)
            
            # combine both oversampled minority rows and previous majority rows 
            combined_df = major_df.unionAll(oversampled_df)
            logging.info('finished balancing dataset')
            return combined_df
        except Exception as e:
            raise ChurnException(e,sys)

    def get_data_transformation_pipeline(self, ) -> Pipeline:
        try:
           stages = [

            ]
           label_indexer = StringIndexer(inputCol="Churn Label", outputCol="labelIndex")
           stages.append(label_indexer)
           for im_one_hot_feature, string_indexer_col in zip(self.schema.one_hot_encoding_features,
                                                             self.schema.string_indexer_one_hot_features):
               string_indexer = StringIndexer(inputCols=im_one_hot_feature,outputCol=string_indexer_col)
               stages.append(string_indexer)
           one_hot_encoder = OneHotEncoder(inputCol=self.schema.string_indexer_one_hot_features,outputCols=self.schema.tf_one_hot_encoding_features)
           stages.append(one_hot_encoder)

           vector_assembler = VectorAssembler(inputCols=self.schema.input_features,
                                               outputCol=self.schema.vector_assembler_output)

           stages.append(vector_assembler)

           standard_scaler = StandardScaler(inputCol=self.schema.vector_assembler_output,
                                             outputCol=self.schema.scaled_vector_input_features)
           stages.append(standard_scaler)
           pipeline = Pipeline(
                stages=stages
            )
           return pipeline
        
        except Exception as e:
            raise ChurnException(e,sys)
        
    def initiate_data_transformation(self):
        try:
            logging.info(f">>>>>>>>>>>Started data transformation <<<<<<<<<<<<<<<")
            dataframe: DataFrame = self.balance_dataset()
            dataframe: DataFrame = dataframe.na.drop()
            logging.info(f"Number of row: [{dataframe.count()}] and column: [{len(dataframe.columns)}]")
            

            
            pipeline = self.get_data_transformation_pipeline()

            transformed_pipeline = pipeline.fit(dataframe)

            require_columns = [self.schema.scaled_vector_input_features,'labelIndex']
            transformed_dataframe = transformed_pipeline.transform(dataframe)
            transformed_dataframe = transformed_dataframe.select(require_columns)
            logging.info(f"Splitting dataset into train and test set using ration:")
            train, test = transformed_dataframe.randomSplit([0.8, 0.2])
            logging.info(f"Train dataset has number of row: [{train.count()}] and"
                        f" column: [{len(train.columns)}]")
            
            logging.info(f"Train dataset has number of row: [{test.count()}] and"
                        f" column: [{len(test.columns)}]")
            

            # creating required directory

            os.makedirs(self.data_transformation_config.export_pipeline_file_path,exist_ok=True)
            os.makedirs(self.data_transformation_config.train_path_dir,exist_ok=True)
            os.makedirs(self.data_transformation_config.test_path_dir,exist_ok=True)
            export_pipeline_file_path = self.data_transformation_config.export_pipeline_file_path
            
            logging.info(f"Saving transformation pipeline at: [{export_pipeline_file_path}]")
            transformed_pipeline.save(export_pipeline_file_path)
            logging.info(f"Saving transformed train data at: [{self.data_transformation_config.train_data_file_path}]")
            train.write.parquet(self.data_transformation_config.train_data_file_path)
            logging.info(f"Saving transformed test data at: [{self.data_transformation_config.test_data_file_pat}]")
            test.write.parquet(self.data_transformation_config.test_data_file_path)
            
            return (
                 self.data_transformation_config.export_pipeline_file_path,
                 self.data_transformation_config.train_data_file_path,
                 self.data_transformation_config.test_data_file_path
            )
        except Exception as e:
            raise ChurnException(e,sys)