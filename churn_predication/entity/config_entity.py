from dataclasses import dataclass
import os
from churn_predication.constant import *
from churn_predication.constant.model import S3_MODEL_DIR_KEY, S3_MODEL_BUCKET_NAME
@dataclass
class get_pipeline_config():
    artifact_dir = PIPELINE_ARTIFACT_DIR
    pipeline_name = PIPELINE_NAME

@dataclass
class DataIngestionConfig:
    pipe = get_pipeline_config()
    data_ingestion_master_dir = os.path.join(pipe.artifact_dir,
                                                 DATA_INGESTION_DIR)
    data_ingestion_dir = os.path.join(data_ingestion_master_dir, TIMESTAMP)
    raw_data_file_path = os.path.join(data_ingestion_master_dir, DATA_INGESTION_FILE_NAME)



@dataclass
class DataValidationConfig:
    pipe = get_pipeline_config()
    data_validation_dir = os.path.join(pipe.artifact_dir,
                                               DATA_VALIDATION_DIR, TIMESTAMP)
    train_data_dir: str = os.path.join(data_validation_dir, DATA_VALIDATION_TRAIN_DIR)
    test_data_dir: str = os.path.join(data_validation_dir, DATA_VALIDATION_TEST_DIR)

    train_data_file_path:str = os.path.join(train_data_dir,DATA_VALIDATION_FILE_NAME_TRAIN)
    test_data_file_path:str = os.path.join(test_data_dir,DATA_VALIDATION_FILE_NAME_TEST)


@dataclass

class DataTransformationConfig:
    pipe = get_pipeline_config()
    data_transformation_dir= os.path.join(pipe.artifact_dir,
                                               DATA_TRANSFORMATION_DIR, TIMESTAMP)
    transformed_train_data_dir = os.path.join(
                data_transformation_dir, DATA_TRANSFORMATION_TRAIN_DIR
            )
    export_pipeline_dir = os.path.join(
                data_transformation_dir, DATA_TRANSFORMATION_PIPELINE_DIR
            )
    transformed_train_data_file_path = os.path.join(transformed_train_data_dir, DATA_TRANSFORMATION_FILE_NAME)

@dataclass 

class ModelTrainerConfig:
    pipe = get_pipeline_config()
    model_trainer_dir= os.path.join(pipe.artifact_dir,
                                               MODEL_TRAINER_DIR, TIMESTAMP)
    trained_model_file_path = os.path.join(
                model_trainer_dir, MODEL_TRAINER_TRAINED_MODEL_DIR, MODEL_TRAINER_MODEL_NAME
            )  
                                               


@dataclass
class ModelEvulationConfig:
    pipe = get_pipeline_config()
    model_evaluation_dir= os.path.join(pipe.artifact_dir,
                                               MODEL_EVALUATION_DIR, TIMESTAMP)

    model_evaluation_report_file_path = os.path.join(
                model_evaluation_dir, MODEL_EVALUATION_REPORT_DIR, MODEL_EVALUATION_REPORT_FILE_NAME
            )
@dataclass
class ModelPusherConfig:
    model_dir  = S3_MODEL_DIR_KEY
    bucket_name=S3_MODEL_BUCKET_NAME

