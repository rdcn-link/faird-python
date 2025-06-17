import math

import pyarrow as pa
import pyarrow.ipc as ipc
import os
import uuid
from parser.abstract_parser import BaseParser
from services.datasource.services import *
from core.config import FairdConfigManager
from utils.logger_utils import get_logger
logger = get_logger(__name__)

class DirParser(BaseParser):

    def sample_dir(self, file_path:str, dataset_name: str) -> pa.Table:
        data_source_service = None;
        if FairdConfigManager.get_config().access_mode == "interface":
            data_source_service = metacat_service.MetaCatService()
        elif FairdConfigManager.get_config().access_mode == "mongodb":
            data_source_service = metacat_mongo_service.MetaCatMongoService()
        elif FairdConfigManager.get_config().access_mode == "neo4j":
            data_source_service = metacat_neo4j_service.MetaCatNeo4jService()
        else:
            logger.error("Failed to load data source service.")

        files_data = []
        all_files = data_source_service.list_dataframes("", dataset_name, 1, 11)
        for file_dict in all_files:
            if file_dict["type"] == "dir":
                continue
            files_data.append({
                "name": file_dict["name"],
                "path": file_dict["path"],
                "suffix": file_dict["suffix"],
                "type": file_dict["type"],
                "size": file_dict["size"],
                "time": file_dict["time"] if file_dict.__contains__("time") else None,
                "blob": None
            })
        files_data = files_data[:10]

        # 定义 Arrow 表的 schema
        schema = pa.schema([
            ("name", pa.string()),
            ("path", pa.string()),
            ("suffix", pa.string()),
            ("type", pa.string()),
            ("size", pa.int64()),
            ("time", pa.string()),
            ("blob", pa.binary())
        ])
        table = pa.Table.from_pydict({key: [file[key] for file in files_data] for key in schema.names}, schema=schema)
        return table

    def parse_dir(self, file_path: str, dataset_name: str) -> pa.Table:
        # Ensure the cache directory exists
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/dir/")
        os.makedirs(os.path.dirname(DEFAULT_ARROW_CACHE_PATH), exist_ok=True)

        arrow_file_name = str(uuid.uuid4()) + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        data_source_service = None;
        if FairdConfigManager.get_config().access_mode == "interface":
            data_source_service = metacat_service.MetaCatService()
        elif FairdConfigManager.get_config().access_mode == "mongodb":
            data_source_service = metacat_mongo_service.MetaCatMongoService()
        elif FairdConfigManager.get_config().access_mode == "neo4j":
            data_source_service = metacat_neo4j_service.MetaCatNeo4jService()
        else:
            logger.error("Failed to load data source service.")

        #  分页
        total_length = data_source_service.get_dataframes_length(dataset_name)
        if total_length == 0:
            logger.info("total length is 0")
            return None
        limit = 10000
        total_pages = math.ceil(total_length / limit)
        all_files = []
        for page in range(1, total_pages + 1):
            dataframes = data_source_service.list_dataframes("", dataset_name, page=page, limit=limit)
            if dataframes:
                    all_files.extend(dataframes)

        files_data = []
        #all_files = data_source_service.list_dataframes("", dataset_name)
        for file_dict in all_files:
            if file_dict["type"] == "dir":
                continue
            files_data.append({
                "name": file_dict["name"],
                "path": file_dict["path"],
                "suffix": file_dict["suffix"],
                "type": file_dict["type"],
                "size": file_dict["size"],
                "time": file_dict["time"] if file_dict.__contains__("time") else None,
                "blob": None
            })

        # 定义 Arrow 表的 schema
        schema = pa.schema([
            ("name", pa.string()),
            ("path", pa.string()),
            ("suffix", pa.string()),
            ("type", pa.string()),
            ("size", pa.int64()),
            ("time", pa.string()),
            ("blob", pa.binary())
        ])
        table = pa.Table.from_pydict({key: [file[key] for file in files_data] for key in schema.names}, schema=schema)

        # Save the table as a .arrow file
        with ipc.new_file(arrow_file_path, table.schema) as writer:
            writer.write_table(table)

        # Load the .arrow file into a pyarrow Table using zero-copy
        with pa.memory_map(arrow_file_path, "r") as source:
            return ipc.open_file(source).read_all()

    def sample(self, file_path:str) -> pa.Table:
        return None

    def parse(self, file_path: str) -> pa.Table:
        return None

    def write(self, table: pa.Table, output_path: str):
        raise NotImplementedError("DirParser.write() 尚未实现：当前不支持写回 Dir 文件")

    def count(self, file_path: str) -> int:
        pass