from dataclasses import dataclass
import os

@dataclass
class DataIngestionConfig:
    raw_data_path: str=os.path.join('artifacts', 'raw_data.csv')