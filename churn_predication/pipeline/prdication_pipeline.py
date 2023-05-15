from churn_predication.constant.predication.file_config import S3_DATA_BUCKET_NAME, PYSPARK_S3_ROOT,INPUT_DIR
from pyspark.sql import DataFrame
from churn_predication.config.spark_manager import spark
import os
from churn_predication.entity.schema import ChurnDataSchema
from typing import List, Tuple
from churn_predication.entity.estimator import S3Estimator
from churn_predication.constant.model import S3_MODEL_DIR_KEY, S3_MODEL_BUCKET_NAME, MODEL_SAVED_DIR
from pyspark.ml.pipeline import PipelineModel
class PredicationPipeline:
    def __init__(self,schema=ChurnDataSchema()):
        super().__init__()
        self.schema = schema
        self.__pyspark_s3_root = PYSPARK_S3_ROOT
        self.bucket_name = S3_MODEL_BUCKET_NAME
        self.s3_model_dir_key = S3_MODEL_DIR_KEY
        self.model_dir = MODEL_SAVED_DIR
    


    def get_pyspark_s3_file_path(self, dir_path) -> str:
        return os.path.join(self.__pyspark_s3_root, dir_path)



    def read(self,file_path: str) -> DataFrame:
        file_path = self.get_pyspark_s3_file_path(dir_path=file_path)
        df =  spark.read.csv(file_path,header='true',schema=self.schema.dataframe_schema)
        return df

    def is_valid_file(self,file_path):
        dataframe: DataFrame = self.read(file_path)
        columns = dataframe.columns
        print(columns)
        missing_columns = []
        for col in self.schema.required_prediction_columns:
            print(col)
            if col not in columns:
                missing_columns.append(col)

        if len(missing_columns) >0:
            return False
        return True

    def get_valid_file(self,file_paths: List[str]):
        valid_file_paths: List[str] = []
        invalid_file_paths: List[str] = []
        for file_path in file_paths:
            is_valid = self.is_valid_file(file_path)
            print(is_valid)
            if is_valid:
                valid_file_paths.append(file_path)
            else:
                invalid_file_paths.append(file_path)

        return valid_file_paths,invalid_file_paths
    
    def start_batch_predication(self):
        files= [INPUT_DIR]
    
        valid,invalid = self.get_valid_file(file_paths=files)
        m = S3Estimator(bucket_name=self.bucket_name,model_key=self.s3_model_dir_key,model_dir = self.model_dir)
        best_model_path = m.get_latest_model_path(key=self.s3_model_dir_key)
    
       
        m.load(key=self.s3_model_dir_key,extract_dir=self.model_dir)
        best_model_path = m.get_latest_model_path_folder_download()
        best_model = PipelineModel.load(best_model_path) 
        for v in valid:
            dataframe: DataFrame = self.read(v)
            
           
            
            best_model_result = best_model.transform(dataframe)
            required_columns = self.schema.required_prediction_columns + ['prediction']
            transformed_dataframe=best_model_result.select(required_columns)
            print(transformed_dataframe.show(10))
if __name__ == '__main__':
    a = PredicationPipeline()
    
    a.start_batch_predication()
    