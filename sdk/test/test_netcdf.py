import xarray as xr
import  pyarrow.compute as pc
from sdk.dacp_client import DacpClient, Principal
from pathlib import Path
from utils.logger_utils import get_logger
logger = get_logger(__name__)

#SERVER_URL = "dacp://localhost:3101"
SERVER_URL = "dacp://60.245.194.25:50201"
USERNAME = "user1@cnic.cn"
TENANT = "conet"
CLIENT_ID = "faird-user1"


def test_netcdf_file(dataframe_id, output_path):
    """
    测试 NetCDF 文件的加载和写回功能。
    现在通过 df.write(...) 接口完成，不再依赖 NCParser 实例。
    """

    #input_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/test_data.nc"
    #output_path = "/Users/zhouziang/Documents/project/faird_new_2/faird/output_test.nc"

    #dataframe_id = "/Users/zhouziang/Documents/test-data/nc/test_data.nc"
    conn = DacpClient.connect(SERVER_URL, Principal.oauth(TENANT))



    logger.info("正在加载 DataFrame...")
    dataframe_name = "dacp://60.245.194.25:50201/home/lcf/faird/test-data/test_data.nc"

    df = conn.open(dataframe_id)
    if df is None:
        logger.info("加载失败：faird.open 返回 None。请检查 parser 或文件路径。")
        return
    logger.info("DataFrame 加载成功")
    logger.info(type(df))
    # logger.info("time 列类型:", type(df["time"]))
    # logger.info("time 列长度:", len(df["time"]))
    # logger.info("time 列内容:", df["time"][:10])  # 只打印前10个值，避免太多输出
    # logger.info(f"Schema: {df.schema}")
    # logger.info(f"Columns: {df.column_names}")
    # logger.info(f"Number of rows: {df.num_rows}")
    # logger.info(f"Memory usage: {df.nbytes} bytes")

    # logger.info(f"Filter temperature < 0.08: {df.filter(pc.less(df["temperature"], 0.08))}")

    # 🔍 1. 查看前几行数据（自动触发 data 加载）
    # logger.info("\n查看前几行数据预览:")
    # logger.info(df.to_string(head_rows=5, tail_rows=0))

    #output_path = "/Users/zhouziang/Documents/test-data/nc/test_data_output.nc"
    logger.info(f"正在使用 df.write(...) 转换文件到: {output_path}")

    try:
        # df.write(output_path=output_path)
        logger.info(f"output_path 类型: {type(output_path)}")
        logger.info(f"dataframe_id 类型: {type(dataframe_id)}")

        df.write(output_path,Path(dataframe_id))

        logger.info(f"成功从df转换为文件: {output_path}")
    except Exception as e:
        logger.info(f"转换文件失败: {e}")








if __name__ == "__main__":
    original_file = "/Users/zhouziang/Documents/test-data/nc/test_data.nc"
    output_file = "/Users/zhouziang/Documents/test-data/nc/test_data_output_2.nc"
    # original_file = "/home/lcf/faird/test-data/test_data.nc"
    # output_file = "/Users/zhouziang/Documents/test-data/test_data.nc"

    test_netcdf_file(original_file, output_file)
    #result = compare_netcdf_files(original_file, output_file)