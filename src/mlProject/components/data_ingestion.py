import os
import urllib.request as request
import zipfile
from src.mlProject import logger
from src.mlProject.utils.common import get_size
from pathlib import Path
from src.mlProject.entity.config_entity import (DataIngestionConfig)


class DataIngestion:
    def __init__(self, config:DataIngestionConfig):
        self.config = config

    def download_file(self):
        try:
            if not Path(self.config.local_data_file).exists():
                filename, headers = request.urlretrieve(
                    url=self.config.source_URL,
                    filename=self.config.local_data_file
                )
                logger.info(f"{filename} downloaded with the following info:\n{headers}")
            else:
                logger.info(f"File already exists of size: {get_size(Path(self.config.local_data_file))}")
        except (error.URLError, error.HTTPError) as e:
            logger.error(f"Error downloading file: {e}")

    def extract_zip_file(self):
        unzip_path = Path(self.config.unzip_dir)
        os.makedirs(unzip_path, exist_ok=True)
        
        try:
            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)
        except zipfile.BadZipFile:
            logger.error(f"The file {self.config.local_data_file} is not a valid ZIP file.")
        except Exception as e:
            logger.error(f"Error extracting ZIP file: {e}")


  