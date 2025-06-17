import json
from typing import List, Optional, Dict, Any

import duckdb
import pandas
import pyarrow as pa
import pyarrow.compute as pc

from core.models.dataframe import DataFrame
from sdk.dacp_client import ConnectionManager
from utils.format_utils import format_arrow_table
import os


class DataFrame(DataFrame):

    def __init__(self, id: str, data: Optional[pa.Table] = None, actions: Optional[List[tuple]] = [], connection_id: Optional[str] = None):
        self.id = id
        self.data = data # 初始状态下 data 为空
        self.actions = actions # 用于记录操作的列表，延迟执行
        self.connection_id = connection_id

    def __str__(self) -> str:
        return self.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3)

    def __len__(self) -> int:
        return self.num_rows

    def __getitem__(self, index):
        if isinstance(index, int):  # 行选择
            if self.data is None:
                ticket = {
                    "dataframe": json.dumps(self, default=vars),
                    "row_index": index
                }
                with ConnectionManager.get_connection() as conn:
                    reader = conn.do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
                row_data = reader.read_all().to_pydict()
                return {col: row_data[col][0] for col in row_data}
            return {col: self.data[col][index].as_py() for col in self.data.column_names}
        elif isinstance(index, str):  # 列选择
            if self.data is None:
                ticket = {
                    "dataframe": json.dumps(self, default=vars),
                    "column_name": index
                }
                with ConnectionManager.get_connection() as conn:
                    reader = conn.do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
                column_data = reader.read_all()
                return column_data.column(0).combine_chunks().to_pylist()  # 转换为列表
            return self.data[index].combine_chunks().to_pylist()
        else:
            raise TypeError("Index must be an integer (row) or string (column).")


    @property
    def schema(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            return flight_info.schema
        return self.data.schema

    @property
    def num_rows(self) -> int:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            return flight_info.total_records
        return self.data.num_rows

    @property
    def num_cols(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            return len(flight_info.schema)
        return len(self.data.column_names)

    @property
    def shape(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            num_rows = flight_info.total_records
            num_cols = len(flight_info.schema)
            return (num_rows, num_cols)
        return self.data.shape

    @property
    def column_names(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            return flight_info.schema.names
        return self.data.column_names

    @property
    def total_bytes(self):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            descriptor = pa.flight.FlightDescriptor.for_command(json.dumps(ticket))
            with ConnectionManager.get_connection() as conn:
                flight_info = conn.get_flight_info(descriptor)
            return flight_info.total_bytes
        return self.data.nbytes

    def collect(self) -> DataFrame:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars)
            }
            with ConnectionManager.get_connection() as conn:
                reader = conn.do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self

    def collect_blob(self) -> DataFrame:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "type": "collect_blob"
            }
            with ConnectionManager.get_connection() as conn:
                reader = conn.do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self

    def get_stream(self, max_chunksize: Optional[int] = 1000):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "max_chunksize": max_chunksize
            }
            with ConnectionManager.get_connection() as conn:
                reader = conn.do_get(pa.flight.Ticket(json.dumps(ticket).encode('utf-8')))
            for batch in reader:
                yield batch.data
            self.actions = []
        else:
            for batch in self.data.to_batches(max_chunksize):
                yield batch

    def limit(self, rowNum: int) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("limit", {"rowNum": rowNum}))
        return new_df

    def slice(self, offset: int = 0, length: Optional[int] = None) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("slice", {"offset": offset, "length": length}))
        return new_df

    def select(self, *columns):
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("select", {"columns": columns}))
        return new_df

    def filter(self, expression: str) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("filter", {"expression": expression}))
        return new_df

    def sum(self, column: str):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            with ConnectionManager.get_connection() as conn:
                results = conn.do_action(
                    pa.flight.Action("compute_sum", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.sum(arrow_table[column]).as_py()

    def mean(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            with ConnectionManager.get_connection() as conn:
                results = conn.do_action(
                    pa.flight.Action("compute_mean", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.mean(arrow_table[column]).as_py()

    def min(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            with ConnectionManager.get_connection() as conn:
                results = conn.do_action(
                    pa.flight.Action("compute_min", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.min(arrow_table[column]).as_py()

    def max(self, column):
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "column": column
            }
            with ConnectionManager.get_connection() as conn:
                results = conn.do_action(
                    pa.flight.Action("compute_max", json.dumps(ticket).encode("utf-8")))
            for res in results:
                return json.loads(res.body.to_pybytes().decode("utf-8"))["result"]
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return pc.max(arrow_table[column]).as_py()

    def sort(self, column: str, order: str = "ascending") -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("sort", {"column": column, "order": order}))
        return new_df

    def sql(self, sql_str: str) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("sql", {"sql_str": sql_str}))
        return new_df

    def map(self, column: str, func: Any, new_column_name: Optional[str] = None) -> DataFrame:
        new_df = DataFrame(self.id, self.data, self.actions[:], self.connection_id)
        new_df.actions.append(("map", {"column": column, "func": func, "new_column_name": new_column_name}))
        return new_df

    def to_pandas(self, **kwargs) -> pandas.DataFrame:
        if self.data is None:
            with ConnectionManager.get_connection() as conn:
                reader = conn.do_get(pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self.data.to_pandas(**kwargs)

    def to_pydict(self) -> Dict[str, List[Any]]:
        if self.data is None:
            with ConnectionManager.get_connection() as conn:
                reader = conn.do_get(
                    pa.flight.Ticket(json.dumps(self, default=vars).encode('utf-8')))
            self.data = reader.read_all()
            self.actions = []
        return self.data.to_pydict()

    def to_string(self, head_rows: int = 5, tail_rows: int = 5, first_cols: int = 3, last_cols: int = 3, display_all: bool = False) -> str:
        if self.data is None:
            ticket = {
                "dataframe": json.dumps(self, default=vars),
                "head_rows": head_rows,
                "tail_rows": tail_rows,
                "first_cols": first_cols,
                "last_cols": last_cols
            }
            with ConnectionManager.get_connection() as conn:
                results = conn.do_action(pa.flight.Action("to_string", json.dumps(ticket).encode("utf-8")))
                for res in results:
                    return res.body.to_pybytes().decode('utf-8')
        else:
            arrow_table = self.handle_prev_actions(self.data, self.actions)
            return format_arrow_table(arrow_table, head_rows, tail_rows, first_cols, last_cols, display_all)

    # todo: 补充
    def handle_prev_actions(self, arrow_table, prev_actions):
        for action in prev_actions:
            action_type, params = action
            if action_type == "limit":
                row_num = params.get("rowNum")
                arrow_table = arrow_table.slice(0, row_num)
            elif action_type == "slice":
                offset = params.get("offset", 0)
                length = params.get("length")
                arrow_table = arrow_table.slice(offset, length)
            elif action_type == "select":
                columns = params.get("columns")
                arrow_table = arrow_table.select(columns)
            elif action_type == "filter":
                column_names = arrow_table.column_names
                locals_dict = {col: arrow_table[col].to_pandas() for col in column_names}
                mask = eval(params.get("expression"), {"__builtins__": None}, locals_dict)
                arrow_table = arrow_table.filter(pa.array(mask))
            elif action_type == "map":
                column = params.get("column")
                func = params.get("func")
                new_column_name = params.get("new_column_name", f"{column}_mapped")
                column_data = arrow_table[column].to_pylist()
                mapped_data = [func(value) for value in column_data]
                arrow_table = arrow_table.append_column(new_column_name, pa.array(mapped_data))
            elif action_type == "sort":
                column = params.get("column")
                order = params.get("order", "ascending")
                if order == "ascending":
                    arrow_table = arrow_table.sort_by(column)
                else:
                    arrow_table = arrow_table.sort_by([(column, "descending")])
            elif action_type == "sql":
                dataframe = arrow_table
                arrow_table = duckdb.sql(params.get("sql_str")).arrow()
            else:
                raise ValueError(f"Unsupported action type: {action_type}")
        return arrow_table

    def write(self, output_path: str, file_path: Optional[str] = None, format: str = None):
        """
        将 DataFrame 写入文件，支持多种格式（netcdf, csv 等）

        Args:
            output_path (str): 输出文件路径。
            file_path (Optional[str]): 原始文件路径（可选），用于 NetCDF 元信息恢复。
            format (str, optional): 文件格式，如 'netcdf', 'csv'。默认自动根据扩展名推断。
        """
        if self.data is None:
            self.collect()

        if format is None:
            ext = os.path.splitext(output_path)[-1].lower()
            if ext in ('.nc', '.netcdf'):
                format = 'netcdf'
            elif ext in ('.csv',):
                format = 'csv'
            elif ext in ('.arrow', '.ipc'):
                format = 'arrow'
            else:
                raise ValueError(f"无法识别文件格式，请指定 format 参数，例如 'netcdf', 'csv'")

        if format == 'netcdf':
            from parser.nc_parser_2 import NCParser2
            parser = NCParser2()

            # 使用传入的 file_path 或 self.id 作为原始文件路径
            original_file_path = file_path or self.id

            logger.info(f"🔍 检查是否已存在缓存元信息...")
            DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/nc/")
            base_name = os.path.basename(original_file_path).rsplit(".", 1)[0]
            meta_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, base_name + ".arrow.metadata.json")

            if not os.path.exists(meta_file_path):
                logger.info(f"⚠️ 缓存不存在，正在通过 parse({original_file_path}) 强制生成完整缓存（包括 metadata）...")
                # 强制解析一次，绕过缓存
                logger.info(f"file_path: {original_file_path}, 类型: {type(original_file_path)}")
                parser.parse(original_file_path, force=True)

            # 继续写回 NetCDF 文件
            parser.write(self.data, output_path, original_file_path=original_file_path)
        elif format == 'csv':
            import pyarrow.csv as csv
            csv.write_csv(self.data, output_path)
        elif format == 'arrow':
            with pa.OSFile(output_path, 'wb') as sink:
                with pa.ipc.new_file(sink, self.data.schema) as writer:
                    writer.write_table(self.data)
        else:
            supported = ['netcdf', 'csv', 'arrow']
            raise NotImplementedError(f"不支持的输出格式: {format}。当前支持: {', '.join(supported)}")

    # def write(self, output_path: str, file_path: Optional[str] = None, format: str = None):
    #     """
    #     将 DataFrame 写入文件，支持多种格式（netcdf, csv 等）。
    #     如果提供 file_path，则复制该文件到 output_path（用于测试/调试）；
    #     否则根据 format 写入当前数据内容。
    #
    #     Args:
    #         output_path (str): 输出文件路径。
    #         file_path (Optional[str]): 要复制的源文件路径（可选）。
    #         format (str, optional): 文件格式，如 'netcdf', 'csv'。默认自动根据扩展名推断。
    #     """
    #     if file_path is not None:
    #         import shutil
    #         logger.info(f"正在写入文件 {file_path} → {output_path}")
    #         try:
    #             logger.info("Source file path:", file_path)
    #             logger.info("Target file path:", output_path)
    #             shutil.copy(file_path, output_path)
    #             logger.info("文件写入成功")
    #         except Exception as e:
    #             raise RuntimeError(f"文件写入失败: {e}")
    #         return
    #
