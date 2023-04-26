import os
from datetime import datetime
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "churn_artifacts")

from .data_ingestion import *
from .data_validation import *
from .data_transformation import *
from .model_trainer import *
from .model_evalution import *