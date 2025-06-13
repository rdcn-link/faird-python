import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.ipc as ipc
import os

from parser.abstract_parser import BaseParser
from utils.logger_utils import get_logger
logger = get_logger(__name__)


class CSVParser(BaseParser):
    """
    CSV file parser implementing the BaseParser interface.
    """

    def sample(self, file_path:str) -> pa.Table:
        try:
            logger.info(f"即将读取 CSV 文件: {file_path}")
            table = csv.read_csv(
                file_path,
                read_options=csv.ReadOptions(block_size=1024 * 1024),  # 设置块大小,
                convert_options=csv.ConvertOptions(),
                parse_options=csv.ParseOptions()
            ).slice(0, 10) # 使用 slice 限制行数
            logger.info(f"成功读取 CSV 文件: {file_path}")
            return table
        except Exception as e:
            logger.error(f"读取 CSV 文件时出错: {e}")
            raise

    def parse(self, file_path: str) -> pa.Table:
        """
        Save the CSV file as a .arrow file and load it into memory as a pyarrow Table using zero-copy.

        Args:
            file_path (str): Path to the input CSV file.
        Returns:
            pa.Table: A pyarrow Table object.
        """

        # Ensure the cache directory exists
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/csv/")
        os.makedirs(os.path.dirname(DEFAULT_ARROW_CACHE_PATH), exist_ok=True)

        # Extract the file name and append the .arrow suffix
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        # Read the CSV file into a pyarrow Table
        try:
            logger.info(f"即将读取 CSV 文件: {file_path}")
            table = csv.read_csv(file_path)
            logger.info(f"成功读取 CSV 文件: {file_path}")
        except Exception as e:
            logger.info(f"读取 CSV 文件时出错: {e}")
            raise

        try:
            with ipc.new_file(arrow_file_path, table.schema) as writer:
                writer.write_table(table)
        except Exception as e:
            logger.info(f"保存 .arrow 文件时出错: {e}")
            raise

        try:
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()
        except Exception as e:
            logger.info(f"加载 .arrow 文件时出错: {e}")
            raise

    def write(self, table: pa.Table, output_path: str):
        """
        占位 write 方法，用于满足 BaseParser 接口要求。
        当前尚未实现写入功能。

        Args:
            table (pa.Table): 要写入的数据（当前不处理）
            output_path (str): 目标输出路径（当前不处理）
        Raises:
            NotImplementedError: 始终抛出未实现异常
        """
        raise NotImplementedError("CSVParser.write() 尚未实现：当前不支持写回 CSV 文件")

    def count(self, file_path: str) -> int:
        try:
            table = csv.read_csv(file_path)
            return table.num_rows
        except Exception as e:
            logger.error(f"统计 CSV 文件行数时出错: {e}")
            raise