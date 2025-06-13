from pymongo import MongoClient
import requests
import json
import os
from typing import Optional, Dict, List
from datetime import datetime
from pydantic import ValidationError

from core.config import FairdConfigManager
from core.models.dataset_meta import DatasetMetadata
from services.datasource.interfaces.datasource_interface import FairdDatasourceInterface
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class MetaCatMongoService(FairdDatasourceInterface):

    metacat_url = "http://10.0.82.71:8080"  # MetaCat服务的URL
    metacat_token = "your_metacat_token"  # MetaCat服务的访问令牌
    datasets = {}  # 数据集name（标识）和id的映射
    dataset_count = 0  # 数据集总数量

    def __init__(self):
        super().__init__()
        self.config = FairdConfigManager.get_config()
        self.metacat_url = self.config.metacat_url
        self.metacat_token = self.config.metacat_token
        ## mongoDB
        self.mongo_client = MongoClient(self.config.mongo_db_url)

    def list_dataset(self, token: str, page: int = 1, limit: int = 10) -> List[str]:
        url = f"{self.metacat_url}/api/fair/listDatasets"
        if token is None or token == "":
            token = self.metacat_token
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        params = {
            "page": page,
            "limit": limit
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 检查请求是否成功
            if response.status_code != 200:
                logger.info(f"Error fetching dataset list: {response.status_code}")
                return None
            data = response.json().get("data")
            dataset_list = data.get("datasetIds", [])
            self.dataset_count = data.get("count", 0)
            ds_names = []
            for dataset in dataset_list:
                name = "dacp://" + self.config.external_domain + ":" + str(self.config.external_port) + "/" + dataset['name']
                ds_names.append(name)
                self.datasets[name] = dataset['id']
            return ds_names
        except requests.RequestException as e:
            logger.info(f"Error fetching dataset list: {e}")
        except json.JSONDecodeError as e:
            logger.info(f"Error decoding JSON response: {e}")
        except KeyError as e:
            logger.info(f"Error parsing response: {e}")
        except Exception as e:
            logger.info(f"Unexpected error: {e}")
        return None

    def get_dataset_meta(self, token: str, dataset_name: str) -> Optional[DatasetMetadata]:
        url = f"{self.metacat_url}/api/fair/getDatasetById"
        if token is None or token == "":
            token = self.metacat_token
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        # 获取数据集ID
        dataset_id = self.datasets[dataset_name]
        if dataset_id is None:
            logger.info(f"dataset {dataset_name} not found in the dataset list.")
            return None
        params = {
            "datasetId": dataset_id
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 检查请求是否成功
            if response.status_code != 200:
                logger.info(f"Error fetching dataset details: {response.status_code}")
                return None
            data = response.json()
            metadata_obj = data.get("data", "{}").get("metadata", {})
            metadata = parse_metadata(metadata_obj)
            return metadata
        except Exception as e:
            logger.info(f"Error fetching dataset info from metacat: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.info(f"Error decoding JSON resonse: {e}")
            return None
        except KeyError as e:
            logger.info(f"Error parsing response: {e}")
            return None

    def list_dataframes(self, token: str, dataset_name: str, page: int = None, limit: int = None):
        try:
            dataset_id = self.datasets[dataset_name]
            root_path = self.config.storage_local_path
            dataset_file_collection = self.mongo_client['metacat']['dataset_file_2025']
            cursor = dataset_file_collection.find({"datasetId": dataset_id})
            dataframes = []
            for file in cursor:
                df = {}
                df['id'] = file['_id']
                df['datasetId'] = file['datasetId']
                df['fId'] = file['fId']
                df['name'] = file['name']
                df['path'] = file['path']
                if file['path'].startswith(root_path):
                    df['path'] = "/" + os.path.relpath(file['path'], root_path)
                df['size'] = file['size']
                df['suffix'] = file['suffix']
                df['type'] = file['type']
                df['dataframeName'] = f"{dataset_name}{df['path']}"
                dataframes.append(df)
            return dataframes
        except Exception as e:
            logger.info(f"Error fetching dataset files from MongoDB: {e}")
            return None

def parse_metadata(raw_data: dict) -> Optional[DatasetMetadata]:
    """解析元数据字段"""
    processed_data = raw_data.copy()
    # 转换分号分隔的字符串为列表
    if "basic" in processed_data:
        basic = processed_data["basic"]
        if "keywords" in basic and isinstance(basic["keywords"], str):
            basic["keywords"] = basic["keywords"].split(";")
        # 转换日期字符串
        if "dateCreated" in basic:
            date_str = basic["dateCreated"]
            basic["dateCreated"] = datetime.strptime(date_str, "%Y-%m-%d").date()
    # 解析为DatasetMetadata对象
    try:
        dataset_metadata = DatasetMetadata.model_validate(processed_data)
        logger.info("元数据解析成功:", dataset_metadata)
        return dataset_metadata
    except ValidationError as e:
        logger.info(f"元数据解析失败:\n{e.json()}")
        return None