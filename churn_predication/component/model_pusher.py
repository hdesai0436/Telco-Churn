from churn_predication.entity.config_entity import ModelTrainerConfig,ModelPusherConfig
from churn_predication.entity.estimator import S3Estimator
import os
from churn_predication.logger import logging
import sys
from churn_predication.exception import ChurnException

class ModelPusher:
    def __init__(self,modeltrain_artifacts= ModelTrainerConfig(),model_pusher= ModelPusherConfig()):
        super().__init__()
        self.modeltrain_artifacts = modeltrain_artifacts
        self.model_pusher_artifacts = model_pusher
        
    def push_model(self):
        try:
            logging.info('model start pushing in s3 bucket')
            m = S3Estimator(bucket_name=self.model_pusher_artifacts.bucket_name,model_dir=self.model_pusher_artifacts.model_dir)
            file_path = self.modeltrain_artifacts.trained_model_file_path
            m.save(model_dir=os.path.dirname(file_path), key=self.model_pusher_artifacts.model_dir)
            logging.info('Model save in S3 bucket in zip file')
        except Exception as e:
            raise ChurnException(e,sys)


