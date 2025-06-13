from typing import Optional, List

from local_sdk.dataframe import DataFrame
from parser import csv_parser
from parser import  tif_parser
import os
import configparser
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class FairdConfig:
    def __init__(self):
        # 获取环境变量 FAIRD_HOME
        faird_home = os.getenv("FAIRD_HOME")
        if not faird_home:
            raise EnvironmentError("环境变量 FAIRD_HOME 未设置")

        # 加载配置文件
        config_path = os.path.join(faird_home, "faird.conf")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件未找到: {config_path}")

        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding="utf-8")

    def get(self, section, option, fallback=None):
        """获取配置项"""
        return self.config.get(section, option, fallback=fallback)

    def get_storage_type(self):
        """获取存储类型"""
        return self.get("storage", "storage.type", fallback="local")

    def get_local_path(self):
        """获取本地存储路径"""
        return self.get("storage", "storage.local.path")

    def get_ftp_config(self):
        """获取 FTP 配置"""
        return {
            "url": self.get("ftp", "storage.ftp.url"),
            "username": self.get("ftp", "storage.ftp.username"),
            "password": self.get("ftp", "storage.ftp.password"),
        }


def list_datasets(page: Optional[int] = 1, limit: Optional[int] = 10) -> List[str]:
    try:
        config = FairdConfig()
        storage_type = config.get_storage_type()

        if storage_type == "local":
            local_path = config.get_local_path()
            logger.info(f"使用本地存储，路径: {local_path}")
            dataset_ids = []
            if os.path.isdir(local_path):
                for item in os.listdir(local_path):
                    if item == '.DS_Store':
                        continue
                    dataset_ids.append(item)
            return dataset_ids
        elif storage_type == "ftp":
            ftp_config = config.get_ftp_config()
            logger.info(f"使用 FTP 存储，URL: {ftp_config['url']}, 用户名: {ftp_config['username']}")
            return []
        else:
            logger.info(f"未知存储类型: {storage_type}")
            return []
    except Exception as e:
        logger.info(f"加载配置失败: {e}")

def list_dataframes(dataset_id: str) -> List[str]:
    config = FairdConfig()
    local_path = config.get_local_path()
    dataset_folder_path = os.path.join(local_path, dataset_id)
    # 遍历文件夹中的所有文件路径
    dataframe_ids = []
    if os.path.isdir(dataset_folder_path):
        for root, _, files in os.walk(dataset_folder_path):
            for file in files:
                if file == '.DS_Store':
                    continue
                file_path = os.path.join(root, file)
                dataframe_ids.append(file_path)
    return dataframe_ids

def open(dataframe_id: str) -> DataFrame:
    """
    Open the specified dataframe and return a DataFrame object.

    Args:
        dataframe_id (str): The unique identifier of the dataframe.
    Returns:
        DataFrame: A DataFrame object containing the parsed data.
    """

    # Determine the file extension
    file_extension = os.path.splitext(dataframe_id)[1].lower()

    # Use a dictionary to simulate a switch case for parser selection
    parser_switch = {
        ".csv": csv_parser.CSVParser,
        ".json": None,
        ".xml": None,
        ".nc": nc_parser_1.NCParser,
        ".tiff": tif_parser.TIFParser,
        ".tif": tif_parser.TIFParser,

    }

    # Get the corresponding parser class
    parser_class = parser_switch.get(file_extension)
    if not parser_class:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    # Instantiate the parser and parse the file
    parser = parser_class()
    arrow_table = parser.parse(dataframe_id)
    return DataFrame(id=dataframe_id, data=arrow_table)

