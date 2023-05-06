import sys 
from churn_predication.config.aws_connection_config import AWSConnectionConfig
from churn_predication.constant.model import MODEL_SAVED_DIR
import time
import shutil
import os
from churn_predication.exception import ChurnException
from churn_predication.logger import logging

class S3Estimator:
    def __init__(self,bucket_name,model_dir,region_name = 'us-east-1',):
        super().__init__()
        aws_connect_config = AWSConnectionConfig(region_name=region_name)
        self.s3_client = aws_connect_config.s3_client
        self.resource =aws_connect_config.s3_resource
    
        response = self.s3_client.list_buckets()
        avaliable_buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in avaliable_buckets:
            
            self.s3_client.create_bucket(Bucket=bucket_name,
                                            )
            logging.info('Sucessfully created bucket in s3')
        self.bucket = self.resource.Bucket(bucket_name)
        self.bucket_name = bucket_name
        self.model_dir = model_dir
        self.compression_format = "zip"
        self.model_file_name = "model.zip"
        self.timestamp = str(time.time())[:10]
        
    
    def get_save_model_path(self,key:str = None) -> str:
        try:
            if key is None:
                key = self.key
            if not key.endswith("/"):
                key = f"{key}/"
            return f"{key}{self.model_dir}/{self.timestamp}/{self.model_file_name}"
        except Exception as e:
            raise ChurnException(e,sys)
    def get_all_model_path(self,key):
        try:
            if key is None:
                key = self.key
            if not key.endswith("/"):
                key = f"{key}/"
            key = f"{key}{self.model_dir}/"

            paths = []
            for key_summary in self.bucket.objects.filter(Prefix=key):
                if key_summary.key.endswith(self.model_file_name):
                    paths.append(key_summary.key)
            return paths
        except Exception as e:
            raise ChurnException(e,sys)

    def get_latest_model_path(self, key):
        try:
            if key is None:
                key = self.key
            if not key.endswith("/"):
                key = f"{key}/"
            key = f"{key}{self.model_dir}/"
            timestamps = []
            for key_summary in self.bucket.objects.filter(Prefix=key):
                tmp_key = key_summary.key
                timestamp = re.findall(r'\d+', tmp_key)
                timestamps.extend(list(map(int, timestamp)))
                # timestamps.append(int(s3_key.replace(s3_key, key).replace(f"/{self.__model_file_name}")))
            if len(timestamps) == 0:
                return None
            timestamp = max(timestamps)

            model_path = f"{key}{timestamp}/{self.__model_file_name}"
            return model_path
        except Exception as e:
            raise ChurnException(e,sys)

    def decompress_model(self, zip_model_file_path, extract_dir) -> None:
            os.makedirs(extract_dir, exist_ok=True)
            shutil.unpack_archive(filename=zip_model_file_path,
                                extract_dir=extract_dir,
                                format=self.compression_format)
    def compress_model_dir(self, model_dir) -> str:
        try:
            if not os.path.exists(model_dir):
                raise Exception(f"Provided model dir:{model_dir} not exist.")

            # preparing temp model zip file path
            tmp_model_file_name = os.path.join(os.getcwd(),
                                            f"tmp_{self.timestamp}",
                                            self.model_file_name.replace(f".{self.compression_format}", ""))

            # remove tmp model zip file path is already present
            if os.path.exists(tmp_model_file_name):
                os.remove(tmp_model_file_name)

            # creating zip file of model dir at tmp model zip file path
            shutil.make_archive(base_name=tmp_model_file_name,
                                format=self.compression_format,
                                root_dir=model_dir
                                )
            tmp_model_file_name = f"{tmp_model_file_name}.{self.compression_format}"
            return tmp_model_file_name
        except Exception as e:
            raise ChurnException(e,sys)

    def save(self, model_dir, key):
        """
        This function save provide model dir to cloud
        It internally compress the model dir then upload it to cloud
        Args:
            model_dir: Model dir to compress
            key: cloud storage key where model will be saved
        Returns:
        """
        try:
            model_zip_file_path = self.compress_model_dir(model_dir=model_dir)
            save_model_path = self.get_save_model_path(key=key)
            self.s3_client.upload_file(model_zip_file_path, self.bucket_name, save_model_path)
            shutil.rmtree(os.path.dirname(model_zip_file_path))
        except Exception as e:
            raise ChurnException(e,sys)

    def is_model_available(self, key) -> bool:
        """
        Args:
            key: Cloud storage to key to check if model available or not
        Returns:
        """
        return bool(len(self.get_all_model_path(key)))

    def load(self, key, extract_dir, ) -> str:
        """
        This function download the latest  model if complete cloud storage key for model not provided else
        model will be downloaded using provided model key path in extract dir
        Args:
            key:
            extract_dir:
        Returns: Model Directory
        """
        try:
        # obtaining latest model directory
            if not key.endswith(self.model_file_name):
                model_path = self.get_latest_model_path(key=key, )
                if not model_path:
                    raise Exception(f"Model is not available. please check your bucket {self.bucket_name}")
            else:
                model_path = key
            timestamp = re.findall(r'\d+', model_path)[0]
            extract_dir = os.path.join(extract_dir, timestamp)
            os.makedirs(extract_dir, exist_ok=True)
            # preparing file location to download model from s3 bucket
            download_file_path = os.path.join(extract_dir, self.model_file_name)

            # download model from s3 into download file location
            self.s3_client.download_file(self.bucket_name, model_path, download_file_path)

            # unzipping file
            self.decompress_model(zip_model_file_path=download_file_path,
                                extract_dir=extract_dir
                                )

            # removing zip file after  unzip
            os.remove(download_file_path)

            model_dir = os.path.join(extract_dir, os.listdir(extract_dir)[0])
            return model_dir
        except Exception as e:
            raise ChurnException(e,sys)