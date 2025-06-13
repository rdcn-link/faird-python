import requests
import json
import os
from typing import Optional, Dict, List
from datetime import datetime
from pydantic import ValidationError
from neo4j import GraphDatabase
import neo4j

from core.config import FairdConfigManager
from core.models.dataset_meta import DatasetMetadata
from services.datasource.interfaces.datasource_interface import FairdDatasourceInterface
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class MetaCatNeo4jService(FairdDatasourceInterface):

    datasets = {}  # 数据集name（标识）和id的映射
    dataset_count = 0  # 数据集总数量

    def __init__(self):
        super().__init__()
        self.config = FairdConfigManager.get_config()
        self.metacat_url = self.config.metacat_url
        self.metacat_token = self.config.metacat_token
        ## Neo4j
        self.neo4j_driver = GraphDatabase.driver(
            self.config.neo4j_url,
            auth=(self.config.neo4j_user, self.config.neo4j_password)
        )

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
                logger.error(f"Error fetching dataset list: {response.status_code}")
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
            logger.error(f"Error fetching dataset list: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {e}")
        except KeyError as e:
            logger.error(f"Error parsing response: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
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
            logger.error(f"dataset {dataset_name} not found in the dataset list.")
            return None
        params = {
            "datasetId": dataset_id
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 检查请求是否成功
            if response.status_code != 200:
                logger.error(f"Error fetching dataset details: {response.status_code}")
                return None
            data = response.json()
            metadata_obj = data.get("data", "{}").get("metadata", {})
            metadata = parse_metadata(metadata_obj)
            return metadata
        except Exception as e:
            logger.error(f"Error fetching dataset info from metacat: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON resonse: {e}")
            return None
        except KeyError as e:
            logger.error(f"Error parsing response: {e}")
            return None

    def get_dataframes_length(self, dataset_name: str) -> int:
        try:
            dataset_id = self.datasets[dataset_name]
            query = ('MATCH (n:DatasetFile {datasetId: "' + dataset_id + '"}) '
                     'WHERE n.isFile = true OR n.type = "dir" '
                     'RETURN COUNT(n) AS total_count')
            with self.neo4j_driver.session() as session:
                result = session.run(query)
                record = result.single()
                if record:
                    return record.get("total_count", 0)
                else:
                    return 0
        except Exception as e:
            logger.error(f"Error fetching dataframes length from Neo4j: {e}")
            return 0

    def list_dataframes(self, token: str, dataset_name: str, page: int = None, limit: int = None):
        try:
            dataset_id = self.datasets[dataset_name]
            root_path = self.config.storage_local_path
            query_template = ('MATCH (n:DatasetFile{datasetId:"' + dataset_id + '"}) '
                     'WHERE n.isFile=true or n.type ="dir" '
                     'RETURN n.datasetId as datasetId,n.name as name,n.suffix as suffix,n.type as type,n.path as path,n.size as size,n.time as time '
                     'SKIP $skip LIMIT $limit')
            dataframes = []
            skip = (page - 1) * limit
            with self.neo4j_driver.session() as session:
                result = session.run(query_template, skip=skip, limit=limit)
                for record in result:
                    time_value = record.get('time')
                    if isinstance(time_value, neo4j.time.DateTime):
                        time_str = time_value.iso_format()
                    else:
                        time_str = str(time_value)  # 如果是字符串，直接使用
                    df = {
                        'datasetId': record.get('datasetId'),
                        'name': record.get('name'),
                        'path': record.get('path'),
                        'size': record.get('size'),
                        'suffix': record.get('suffix'),
                        'type': record.get('type'),
                        'time': time_str
                    }
                    if df['path'].startswith(root_path):
                        df['path'] = "/" + os.path.relpath(df['path'], root_path)
                    df['dataframeName'] = f"{dataset_name}{df['path']}"
                    dataframes.append(df)
            return dataframes
        except Exception as e:
            logger.error(f"Error fetching dataset files from Neo4j: {e}")
            return None

    def list_user_auth_dataframes(self, username: str):
        try:
            root_path = self.config.storage_local_path
            query = ('MATCH (u:me_user {username: "' + username + '"}) '
                     'OPTIONAL MATCH (u)-[:user_file]->(f1:DatasetFile) '
                     'WHERE f1.isFile = true OR f1.type = "dir" '
                     'OPTIONAL MATCH (u)<-[:role_user]-(r:me_role)-[:role_file]->(f2:DatasetFile) '
                     'WHERE f2.isFile = true OR f2.type = "dir" '
                     'WITH u,  COLLECT(f1) + COLLECT(f2) AS allFiles '
                     'UNWIND allFiles AS f '
                     'RETURN DISTINCT f.datasetId AS datasetId,f.name AS name,f.suffix AS suffix,f.type AS type,f.path AS path,f.size AS size,f.time AS time')
            dataframes = []
            with self.neo4j_driver.session() as session:
                result = session.run(query)
                for record in result:
                    # neo4j.time to string
                    time_value = record.get('time')
                    if isinstance(time_value, neo4j.time.DateTime):
                        time_str = time_value.iso_format()
                    else:
                        time_str = str(time_value)  # 如果是字符串，直接使用

                    # get dataset_name
                    dataset_id = record.get('datasetId')
                    dataset_name = find_key_by_value(self.datasets, dataset_id)
                    df = {
                        # 'id': record.get('id'),
                        'datasetId': record.get('datasetId'),
                        'name': record.get('name'),
                        'path': record.get('path'),
                        'size': record.get('size'),
                        'suffix': record.get('suffix'),
                        'type': record.get('type'),
                        'time': time_str
                    }
                    if df['path'].startswith(root_path):
                        df['path'] = "/" + os.path.relpath(df['path'], root_path)
                    df['dataframeName'] = f"{dataset_name}{df['path']}"
                    dataframes.append(df)
            return dataframes
        except Exception as e:
            logger.error(f"Error fetching dataset files from Neo4j: {e}")
            return None

    def check_permission(self, dataset_name: str, username: str) -> bool:
        url = f"{self.metacat_url}/api/fair/checkPermission"
        headers = {
            "Authorization": self.metacat_token,
            "Content-Type": "application/json"
        }
        # 获取数据集ID
        dataset_id = self.datasets[dataset_name]
        if dataset_id is None:
            logger.error(f"dataset {dataset_name} not found in the dataset list.")
            return None
        params = {
            "datasetId": dataset_id,
            "username": username
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 检查请求是否成功
            if response.status_code != 200:
                logger.error(f"Error fetching dataset details: {response.status_code}")
                return None
            data = response.json()
            has_permission = data.get("data", "{}").get("result", False)
            return has_permission
        except Exception as e:
            logger.error(f"Error fetching dataset info from metacat: {e}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON resonse: {e}")
            return False
        except KeyError as e:
            logger.error(f"Error parsing response: {e}")
            return False


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
        logger.info(f"元数据解析成功: {dataset_metadata}")
        return dataset_metadata
    except ValidationError as e:
        logger.error(f"元数据解析失败:\n{e.json()}")
        return None

def find_key_by_value(d, value):
    for key, val in d.items():
        if val == value:
            return key
    return None