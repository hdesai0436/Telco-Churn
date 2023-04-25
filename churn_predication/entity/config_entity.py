from dataclasses import dataclass
import os

@dataclass
class DataIngestionConfig:
    raw_data_path: str=os.path.join('artifacts', 'raw_data.csv')


@dataclass
class DataValidationConfig:
    clean_data_path: str=os.path.join('artifacts', 'clean_data.csv')


@dataclass

class DataTransformationConfig:
    train_path_dir: str = os.path.join('artifacts', 'train_data_dir')
    test_path_dir: str = os.path.join('artifacts','test_path_dir')
    export_pipeline_file_path: str = os.path.join('artifacts','pipeline')