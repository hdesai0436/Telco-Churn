from churn_predication.logger import logging
from churn_predication.exception import ChurnException
from churn_predication.config.spark_manager import spark
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import pandas as pd
from churn_predication.entity.config_entity import DataIngestionConfig
from churn_predication.entity.schema import ChurnDataSchema
from churn_predication.entity.config_entity import DataValidationConfig
from typing import List,Dict
from collections import namedtuple
MissingReport = namedtuple("MissingReport", ["total_row", "missing_row","missing_percentage"])

class DataValidation(ChurnDataSchema):
    def __init__(self,schema=ChurnDataSchema(),file_path = DataIngestionConfig(),data_validation_config = DataValidationConfig()):
         try:
              super().__init__()
              self.schema = schema
              self.file_path = file_path.raw_data_file_path
              self.data_validation_config = data_validation_config
         except Exception as e:
              raise ChurnException(e,sys) from e
         
         

    def read_data(self) -> DataFrame:
         logging.info('Enter in validation read_data method')
         try:
              dataframe: DataFrame = spark.read.csv(self.file_path,header=True)
              logging.info(f'data frame is created using  file: {self.file_path}')
              logging.info(f'Number of row: {dataframe.count()} and columns: {len(dataframe.columns)}')
              
              return dataframe

         except Exception as e:
              logging.exception(e)
              raise ChurnException(e,sys)
         
    def get_missing_report(self, dataframe: DataFrame) -> Dict[str,MissingReport]:
         logging.info('preparing missing reports each columns')
         try:

          missing_report: Dict[str:MissingReport] = dict()
          number_of_row = dataframe.count()
          
          for column in dataframe.columns:
               missing_row = dataframe.filter(col(f"{column}").isNull()).count()
               missing_percentage = (missing_row*100) / number_of_row
               missing_report[column] = MissingReport(total_row=number_of_row,
                                                       missing_row=missing_row,
                                                       missing_percentage=missing_percentage
                                                       )
          logging.info(f'Missing report created: {missing_report}')
          logging.info('exited get_missing_method')
          return missing_report

         except Exception as e:
              raise ChurnException(e,sys) from e
         
         
         
    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float = 0.2) -> List[str]:
         logging.info('start get_unwanted_and_high_missing_value_columns method in data validation class')
         try:
              logging.info('Get the missing value report')
              missing_reports: Dict[str,MissingReport] = self.get_missing_report(dataframe=dataframe)
              logging.info('Get the unwanted columns')
              unwanted_columns : List[str] = self.schema.unwanted_columns
              for column in missing_reports:
                   if missing_reports[column].missing_percentage > (threshold*100):
                        unwanted_columns.append(column)
                        logging.info(f'Missing report {column}: [{missing_reports[column]}]')
              unwanted_columns = list(set(unwanted_columns))
              logging.info('finished get_unwanted_and_high_missing_value_columns method')
              return unwanted_columns
         except Exception as e:
              raise ChurnException(e,sys)

    def drop_unwanted_columns(self,dataframe:DataFrame) -> DataFrame:
         try:
              logging.info('star dropping unwanted columns')
              unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe,)
              logging.info(f'dropping columns are {",".join(unwanted_columns)}')
              dataframe: DataFrame = dataframe.drop(*unwanted_columns)
              logging.info(f'Remaining number of columns are [{dataframe.columns}] ')
              logging.info('finished dropping unwanted columns')
              return dataframe



         except Exception as e:
              raise(ChurnException(e,sys))
    def balance_dataset(self) -> DataFrame:
        try:
            logging.info('starting balancing dataset')
            dataframe: DataFrame = self.read_data()
            logging.info('spliting dataframe base on minimum and maximum values')
            major_df = dataframe.filter(col("Churn Label") == 'No')
            minor_df = dataframe.filter(col("Churn Label") == 'Yes')
            ratio = int(major_df.count()/minor_df.count())
            
            oversampled_df = minor_df.sample(withReplacement=True, fraction=2.5, seed=10)
            

            logging.info('balance dataset with oversampling data')
            # combine both oversampled minority rows and previous majority rows 
            combined_df = major_df.unionAll(oversampled_df)
            major_df = combined_df.filter(col("Churn Label") == 'No')
            minor_df = combined_df.filter(col("Churn Label") == 'Yes')
            print(major_df.count())
            print(minor_df.count())
            logging.info('finished balancing dataset')
            return combined_df
        except Exception as e:
            raise ChurnException(e,sys)
         
    
    def initiate_data_validation(self):
         try:
              logging.info('Initiating data Preprocessing')
              dataframe: DataFrame = self.balance_dataset()
              logging.info('Dropping columns unwanted')
              os.makedirs(self.data_validation_config.train_data_dir,exist_ok=True)
              os.makedirs(self.data_validation_config.test_data_dir,exist_ok=True)
              dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)
              logging.info(f"Splitting dataset into train and test set using ration: 80:20")
              train, test = dataframe.randomSplit([0.8, 0.2],seed=1025)
              train.write.csv(self.data_validation_config.train_data_file_path,header=True)
              test.write.csv(self.data_validation_config.test_data_file_path,header=True)
              logging.info(f"Train dataset has number of row: [{train.count()}] and"
                        f" column: [{len(train.columns)}]")

              logging.info(f"Train dataset has number of row: [{test.count()}] and"
                        f" column: [{len(test.columns)}]")
              
              logging.info(f'clean train data file saved {self.data_validation_config.train_data_file_path}')
              logging.info(f'clean test data file saved {self.data_validation_config.test_data_file_path}')
              logging.info('finish initiate_data_validation part')
              return (
          
                  self.data_validation_config.train_data_file_path,
                  self.data_validation_config.test_data_file_path

              )
         except Exception as e:
              raise ChurnException(e,sys)

       

    

