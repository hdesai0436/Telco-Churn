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
    train_path: str = os.path.join('artifacts', 'train.csv')