import os
from urllib.parse import urlparse
import math
import pyarrow.flight

from sdk.dataframe import DataFrame
from services.connection.faird_connection import FairdConnection
from utils.format_utils import format_arrow_table
from services.datasource.services import *
from services.types.thread_safe_dict import ThreadSafeDict
from services.connection.connection_service import connect_server_with_oauth, connect_server_with_controld
from parser import *
from compute.interactive.interactive import *
from core.config import FairdConfigManager
from utils.logger_utils import get_logger, get_access_logger
logger = get_logger(__name__)
access_logger = get_access_logger(__name__)

class FairdServiceProducer(pa.flight.FlightServerBase):
    def __init__(self, location: pa.flight.Location):
        super(FairdServiceProducer, self).__init__(location)

        # 线程安全的字典，用于存储连接信息
        self.datasetMetas = ThreadSafeDict() # dataset_name -> DatasetMetadata
        self.connections = ThreadSafeDict() # connection_id -> Connection
        self.user_compute_resources = ThreadSafeDict()  # username -> UserComputeResource

        # 初始化datasource_service
        self.data_source_service = None;
        if FairdConfigManager.get_config().access_mode == "interface":
            self.data_source_service = metacat_service.MetaCatService()
        elif FairdConfigManager.get_config().access_mode == "mongodb":
            self.data_source_service = metacat_mongo_service.MetaCatMongoService()
        elif FairdConfigManager.get_config().access_mode == "neo4j":
            self.data_source_service = metacat_neo4j_service.MetaCatNeo4jService()
        else:
            logger.error("Failed to load data source service.")

    def list_flights(self, context, criteria):
        # 实现列出可用的 Flight 数据集
        pass

    def get_flight_info(self, context, descriptor):
        dataframe = json.loads(descriptor.command.decode("utf-8")).get("dataframe")
        dataframe_id = json.loads(dataframe).get("id")
        actions = json.loads(dataframe).get("actions")
        connection_id = json.loads(dataframe).get("connection_id")

        # 获取 Arrow Table 的 schema
        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)
        schema = arrow_table.schema

        # 构造 FlightInfo
        ticket = pa.flight.Ticket(json.dumps({"dataframe_id": dataframe_id}).encode("utf-8"))
        endpoints = [pa.flight.FlightEndpoint(ticket, [])]
        flight_info = pa.flight.FlightInfo(
            schema,
            descriptor,
            endpoints,
            total_records=arrow_table.num_rows,
            total_bytes=arrow_table.nbytes
        )
        return flight_info

    def do_get(self, context, ticket):
        ticket_data = json.loads(ticket.ticket.decode('utf-8'))
        dataframe_id = json.loads(ticket_data.get('dataframe')).get('id')
        actions = json.loads(ticket_data.get('dataframe')).get('actions')
        connection_id = json.loads(ticket_data.get('dataframe')).get("connection_id")
        max_chunksize = ticket_data.get('max_chunksize')
        row_index = ticket_data.get('row_index')  # 获取行索引
        column_name = ticket_data.get('column_name')  # 获取列名
        type = ticket_data.get('type')

        # 从conn中获取dataframe.data
        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)

        # todo: 暂时在这里处理collect_blob
        if type is not None and type == "collect_blob":
            batches = arrow_table.to_batches()
            updated_batches = []
            for batch in batches:
                path_column = batch.column(batch.schema.get_field_index("path")).to_pylist()
                blob_column_index = batch.schema.get_field_index("blob")
                blob_data = []
                for path in path_column:
                    try:
                        file_path = FairdConfigManager.get_config().storage_local_path + path
                        logger.info(f"Reading file: {file_path}")
                        with open(file_path, "rb") as f:
                            blob_data.append(f.read())
                    except Exception as e:
                        logger.error(f"Error reading file {path}: {e}")
                        blob_data.append(None)
                blob_array = pa.array(blob_data, type=pa.binary())
                updated_batch = batch.set_column(blob_column_index, "blob", blob_array)
                updated_batches.append(updated_batch)
            return pa.flight.GeneratorStream(arrow_table.schema, iter(updated_batches))

        if row_index is not None:  # 如果请求某行
            row_data = arrow_table.slice(row_index, 1).to_pydict()
            return pa.flight.GeneratorStream(
                pa.schema([(col, arrow_table.schema.field(col).type) for col in row_data.keys()]),
                iter([pa.RecordBatch.from_pydict(row_data)])
            )
        elif column_name is not None:  # 如果请求某列
            column_data = arrow_table[column_name].combine_chunks()
            return pa.flight.GeneratorStream(
                pa.schema([(column_name, column_data.type)]),
                iter([pa.RecordBatch.from_arrays([column_data], [column_name])])
            )
        else: # 如果没有指定行或列，则返回整个表
            if max_chunksize:
                batches = arrow_table.to_batches(max_chunksize)
            else:
                batches = arrow_table.to_batches()
            return pa.flight.GeneratorStream(arrow_table.schema, iter(batches))

    def do_put(self, context, descriptor, reader, writer):
        # 实现数据写入逻辑
        pass

    def do_action(self, context, action):
        action_type = action.type
        if action_type == "connect_server":
            ticket_data = json.loads(action.body.to_pybytes().decode('utf-8'))
            auth_type = ticket_data.get('auth_type')
            if auth_type == "oauth":
                token = connect_server_with_oauth(ticket_data.get('type'), ticket_data.get('username'), ticket_data.get('password'))
                conn = FairdConnection(clientIp=ticket_data.get('clientIp'), username=ticket_data.get('username'), token=token)
                self.connections[conn.connectionID] = conn
                return iter([pa.flight.Result(json.dumps({"token": token, "connectionID": conn.connectionID}).encode("utf-8"))])
            elif auth_type == "controld":
                verified = connect_server_with_controld(ticket_data.get('controld_domain_name'), ticket_data.get('signature'))
                if verified:
                    conn = FairdConnection(clientIp=ticket_data.get('clientIp'))
                    self.connections[conn.connectionID] = conn
                    return iter([pa.flight.Result(json.dumps({"connectionID": conn.connectionID}).encode("utf-8"))])
                else:
                    raise ValueError("Controld verification failed.")
            elif auth_type == "anonymous":
                conn = FairdConnection(clientIp=ticket_data.get('clientIp'))
                self.connections[conn.connectionID] = conn
                return iter([pa.flight.Result(json.dumps({"connectionID": conn.connectionID}).encode("utf-8"))])
            else:
                conn = FairdConnection(clientIp=ticket_data.get('clientIp'))
                self.connections[conn.connectionID] = conn
                return iter([pa.flight.Result(json.dumps({"connectionID": conn.connectionID}).encode("utf-8"))])

        elif action_type == "get_instrument_info":
            instrument_info = FairdConfigManager.get_config().instrument_info
            return iter([pa.flight.Result(instrument_info.encode("utf-8"))])

        elif action_type == "get_network_link_info":
            network_link_info = FairdConfigManager.get_config().network_link_info
            return iter([pa.flight.Result(network_link_info.encode("utf-8"))])

        elif action_type == "list_datasets":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            page = int(ticket_data.get("page"))
            limit = int(ticket_data.get("limit"))
            datasets = self.data_source_service.list_dataset(token=token, page=page, limit=limit)
            return iter([pa.flight.Result(json.dumps(datasets).encode("utf-8"))])

        elif action_type == "get_dataset":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            dataset_name = ticket_data.get("dataset_name")
            meta = (self.datasetMetas.get(dataset_name)
                    or self.data_source_service.get_dataset_meta(token, dataset_name))
            if meta:
                self.datasetMetas[dataset_name] = meta
                return iter([pa.flight.Result(meta.model_dump_json().encode())])
            return None

        elif action_type == "list_dataframes":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            token = ticket_data.get("token")
            dataset_name = ticket_data.get("dataset_name")
            max_chunksize = ticket_data.get("max_chunksize")
            if max_chunksize is None:
                max_chunksize = 50000  # 设置默认值
            total_length = self.data_source_service.get_dataframes_length(dataset_name)
            if total_length == 0:
                logger.info("total length is 0")
                return iter([])
            else:
                limit = max_chunksize
                total_pages = math.ceil(total_length / limit)
                def dataframe_generator():
                    for page in range(1, total_pages + 1):
                        dataframes = self.data_source_service.list_dataframes(token, dataset_name, page=page, limit=limit)
                        if dataframes:
                            yield pa.flight.Result(json.dumps(dataframes).encode("utf-8"))
                return dataframe_generator()

        elif action_type == "list_user_auth_dataframes":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            username = ticket_data.get("username")
            dataframes = self.data_source_service.list_user_auth_dataframes(username)
            return iter([pa.flight.Result(json.dumps(dataframes).encode())])

        elif action_type == "check_permission":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            dataset_name = ticket_data.get("dataset_name")
            username = ticket_data.get("username")
            has_permission = self.data_source_service.check_permission(dataset_name, username)
            return iter([pa.flight.Result(str(has_permission).encode("utf-8"))])

        elif action_type == "sample":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            dataframe_name = ticket_data.get("dataframe_name")
            connection_id = ticket_data.get("connection_id")
            sample_json = self.sample_action(dataframe_name)
            conn = self.connections.get(connection_id)
            if conn:
                access_logger.info(f"Dataframe: {dataframe_name}, Action: sample, Client IP: {conn.clientIp}, Username: {conn.username}")
            return iter([pa.flight.Result(json.dumps(sample_json).encode())])

        elif action_type == "count":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            dataframe_name = ticket_data.get("dataframe_name")
            connection_id = ticket_data.get("connection_id")
            count_json = self.count_action(dataframe_name)
            conn = self.connections.get(connection_id)
            if conn:
                access_logger.info(f"Dataframe: {dataframe_name}, Action: count, Client IP: {conn.clientIp}, Username: {conn.username}")
            return iter([pa.flight.Result(json.dumps(count_json).encode())])

        elif action_type == "open":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            connection_id = ticket_data.get('connection_id')
            dataframe_name = ticket_data.get("dataframe_name")  # uri
            # open with parser
            df = self.open_action(dataframe_name)
            # put dataframe to connection memory
            conn = self.connections.get(connection_id)
            conn.dataframes[dataframe_name] = df
            if conn:
                access_logger.info(f"Dataframe: {dataframe_name}, Action: open, Client IP: {conn.clientIp}, Username: {conn.username}")
            return None

        elif action_type == "get_dataframe_stream":
            ticket_data = json.loads(action.body.to_pybytes().decode("utf-8"))
            dataframe_name = ticket_data.get("dataframe_name")
            max_chunksize = ticket_data.get("max_chunksize")
            connection_id = ticket_data.get('connection_id')
            conn = self.connections.get(connection_id)
            if max_chunksize is None:
                max_chunksize = 1024 * 1024 * 5
            # 获取文件路径
            parsed_url = urlparse(dataframe_name)
            relative_path = '/' + parsed_url.path.split('/', 2)[2]  # 相对路径
            file_path = FairdConfigManager.get_config().storage_local_path + relative_path  # 绝对路径
            # 分块生成器
            def file_chunk_generator(file_path, chunk_size=max_chunksize):
                try:
                    with open(file_path, "rb") as file:
                        while chunk := file.read(chunk_size):
                            access_logger.info(f"Dataframe: {dataframe_name}, Action: get_dataframe_stream, Data Size: {len(chunk)} Bytes, "
                                               f"Client IP: {conn.clientIp}, Username: {conn.username}")
                            yield pa.flight.Result(chunk)
                except FileNotFoundError:
                    raise ValueError(f"文件未找到: {file_path}")
                except Exception as e:
                    raise ValueError(f"读取文件失败: {str(e)}")
            return file_chunk_generator(file_path)
    
        elif action_type == "to_string":
            return self.to_string_action(context, action)

        elif action_type.startswith("compute_"):
            return handle_compute_actions(self.connections, action)

        else:
            return None

    def sample_action(self, dataframe_name):
        parsed_url = urlparse(dataframe_name)
        dataset_name = f"{parsed_url.scheme}://{parsed_url.netloc}/{parsed_url.path.split('/', 2)[1]}"
        relative_path = '/' + parsed_url.path.split('/', 2)[2]  # 相对路径
        file_path = FairdConfigManager.get_config().storage_local_path + relative_path  # 绝对路径
        file_extension = os.path.splitext(file_path)[1].lower()
        # 暂时这样适配文件夹类型
        sample_table = None
        total_count = None
        if file_extension == "":
            sample_table = dir_parser.DirParser().sample_dir(file_path, dataset_name)
        else:
            parser_switch = {
                ".csv": csv_parser.CSVParser,
                ".json": None,
                ".xml": None,
                ".nc": nc_parser.NCParser,
                ".tiff": tif_parser.TIFParser,
                ".tif": tif_parser.TIFParser
            }
            parser_class = parser_switch.get(file_extension)
            if not parser_class:
                raise ValueError(f"Unsupported file extension: {file_extension}")
            parser = parser_class()
            sample_table = parser.sample(file_path)
            if hasattr(parser, "count") and callable(getattr(parser, "count", None)):
                try:
                    total_count = parser.count(file_path)
                except Exception as e:
                    logger.warning(f"count方法调用失败: {e}")
                    total_count = None
            else:
                total_count = None

        # 返回sample_table打印内容
        schema_names = [field.name for field in sample_table.schema]
        schema_types = [str(field.type) for field in sample_table.schema]
        sample_data = {col: sample_table.column(col).slice(0, 10).to_pylist() for col in sample_table.column_names}
        sample_data = replace_nan(sample_data)
        # 处理bytes类型序列化
        # metadata_bytes = sample_table.schema.metadata
        # sample_metadata = decode_bytes_keys(metadata_bytes)
        sample_json = {
            'schema_names': schema_names,
            'schema_types': schema_types,
            # 'schema_metadata': meta_json_data,
            'sample_data': sample_data,
            'total_count': total_count
        }
        return sample_json

    def count_action(self, dataframe_name):
        parsed_url = urlparse(dataframe_name)
        dataset_name = f"{parsed_url.scheme}://{parsed_url.netloc}/{parsed_url.path.split('/', 2)[1]}"
        relative_path = '/' + parsed_url.path.split('/', 2)[2]  # 相对路径
        file_path = FairdConfigManager.get_config().storage_local_path + relative_path  # 绝对路径
        file_extension = os.path.splitext(file_path)[1].lower()
        total_count = None
        if file_extension == "":
            total_count = None
        else:
            parser_switch = {
                ".csv": csv_parser.CSVParser,
                ".json": None,
                ".xml": None,
                ".nc": nc_parser.NCParser,
                ".tiff": tif_parser.TIFParser,
                ".tif": tif_parser.TIFParser
            }
            parser_class = parser_switch.get(file_extension)
            if not parser_class:
                raise ValueError(f"Unsupported file extension: {file_extension}")
            parser = parser_class()
            if hasattr(parser, "count") and callable(getattr(parser, "count", None)):
                try:
                    total_count = parser.count(file_path)
                except Exception as e:
                    logger.warning(f"count方法调用失败: {e}")
                    total_count = None
            else:
                total_count = None
        rtn_json = {
            'total_count': total_count
        }
        return rtn_json

    def open_action(self, dataframe_name):
        parsed_url = urlparse(dataframe_name)
        dataset_name = f"{parsed_url.scheme}://{parsed_url.netloc}/{parsed_url.path.split('/', 2)[1]}"
        relative_path = '/' + parsed_url.path.split('/', 2)[2]  # 相对路径
        file_path = FairdConfigManager.get_config().storage_local_path + relative_path  # 绝对路径
        file_extension = os.path.splitext(file_path)[1].lower()
        # 暂时这样适配文件夹类型
        if file_extension == "":
            arrow_table = dir_parser.DirParser().parse_dir(file_path, dataset_name)
            return DataFrame(id=dataframe_name, data=arrow_table)
        parser_switch = {
            ".csv": csv_parser.CSVParser,
            ".json": None,
            ".xml": None,
            ".nc": nc_parser.NCParser,
            ".tiff": tif_parser.TIFParser,
            ".tif": tif_parser.TIFParser
        }
        parser_class = parser_switch.get(file_extension)
        if not parser_class:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        parser = parser_class()
        arrow_table = parser.parse(file_path)
        return DataFrame(id=dataframe_name, data=arrow_table)

    def to_string_action(self, context, action):
        params = json.loads(action.body.to_pybytes().decode("utf-8"))
        dataframe_id = json.loads(params.get("dataframe")).get("id")
        actions = json.loads(params.get("dataframe")).get("actions")
        connection_id = json.loads(params.get("dataframe")).get("connection_id")
        head_rows = params.get("head_rows", 5)
        tail_rows = params.get("tail_rows", 5)
        first_cols = params.get("first_cols", 3)
        last_cols = params.get("last_cols", 3)
        display_all = params.get("display_all", False)

        conn = self.connections[connection_id]
        arrow_table = conn.dataframes[dataframe_id].data
        arrow_table = handle_prev_actions(arrow_table, actions)

        table_str = format_arrow_table(arrow_table, head_rows, tail_rows, first_cols, last_cols, display_all)
        return iter([pa.flight.Result(table_str.encode("utf-8"))])

# 将字典中的 bytes 类型键转换为字符串类型
def decode_bytes_keys(data):
    if isinstance(data, dict):
        return {k.decode() if isinstance(k, bytes) else k: decode_bytes_keys(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [decode_bytes_keys(item) for item in data]
    elif isinstance(data, bytes):
        return data.decode()
    return data

def replace_nan(data):
    if isinstance(data, dict):
        return {k: replace_nan(v) for k, v in data.items()}
    elif isinstance(data, list):
        return ['NaN' if isinstance(item, float) and math.isnan(item) else replace_nan(item) for item in data]
    return data