import base64
import hashlib
import json

from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame
import os
import uuid
import logging
logger = logging.getLogger(__name__)


def test_sdk():

    #url = "dacp://60.245.194.25:50201"
    url = "dacp://localhost:3101"
    username = "faird-user1"
    password = "user1@cnic.cn"
    #conn = DacpClient.connect(url, Principal.oauth("conet", username=username, password=password))
    #conn = DacpClient.connect(url, Principal.ANONYMOUS)

    #conn = DacpClient.connect(url, Principal.controld(domain_name="controld_domain_name", signature="signature"))


    conn = DacpClient.connect(url)

    ## !! for local test
    # dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/SOCATv2021_qrtrdeg_gridded_coast_monthly.nc"
    # dataframe_name = r"dacp://0.0.0.0:3101/中尺度涡旋数据集/D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_qrtrdeg_gridded_coast_monthly.nc"
    # sample = conn.sample(dataframe_name)
    # print(sample)

    datasets = conn.list_datasets()
    has_permission = conn.check_permission(datasets[0], "faird-user1")
    metadata = conn.get_dataset(datasets[12])


    dataframes = conn.list_dataframes(datasets[12])
    # 改流式
    for chunk in conn.list_dataframes_stream(datasets[12]):
        print(f"Chunk size: {len(chunk)}")

    dataframe_name = dataframes[3]['dataframeName']
    total_size = 0
    for chunk in conn.get_dataframe_stream(dataframe_name, max_chunksize=1024*1024*5):
        total_size += len(chunk)
    print(f"total size: {total_size} Bytes")

    # dir sample
    dir_dataframe_name = dataframes[0]['dataframeName']
    sample = conn.sample(dir_dataframe_name)



    # df = conn.open(dir_dataframe_name)
    # df = conn.open("/Users/yaxuan/Desktop/测试用/2019年中国榆林市沟道信息.csv")
    #
    # # csv parser
    #csv_dataframe_name = dataframes[3]['dataframeName']
    #sample = conn.sample(csv_dataframe_name)
    # df = conn.open(csv_dataframe_name)


    # nc parser
    #nc_dataframe_name = dataframes[4]['dataframeName']
    #sample = conn.sample(nc_dataframe_name)
    #df = conn.open(nc_dataframe_name)
    df = DataFrame()
    print(f"表结构: {df.schema} \n")
    print(f"表大小: {df.shape} \n")
    print(f"行数: {df.num_rows} \n")  # 或者len(dataframe)
    print(f"列数: {df.num_cols} \n")
    print(f"列名: {df.column_names} \n")
    print(f"数据大小: {df.total_bytes} \n")

    data_str = df.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False)
    print(f"打印dataframe：\n {data_str}\n")  # 或者直接用print()(df)

    # 默认1000行
    for chunk in df.get_stream():
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")

    # 设置每次读取100行
    for chunk in df.get_stream(max_chunksize=100):
        print(chunk)
        print(f"Chunk size: {chunk.num_rows}")
    #
    # print()(f"=== 01.limit, select 在本地计算: === \n {df.collect().limit(3).select("start_p")} \n")
    # print()(f"=== 02.limit, select 在远程计算，仅将处理结果加载到本地: === \n {df.limit(3).select("start_p").collect()} \n")
    # print()(f"=== 03.limit 在远程计算，select 在本地计算: === \n {df.limit(3).collect().select("start_p")} \n")

    print(f"打印指定列的值: \n {df["start_p"]} \n")
    print(f"筛选某几列: \n {df.select("start_p", "start_l", "end_l")} \n")

    print(f"打印第0行的值: \n {df[0]} \n")
    print(f"打印第0行、指定列的值: \n {df[0]["start_l"]} \n")
    print(f"筛选前10行: \n {df.limit(10)} \n")
    print(f"筛选第2-4行: \n {df.slice(2, 4)} \n")

    # 示例 1: 筛选某列值大于 10 的行
    expression = "OBJECTID <= 30"

    # 示例 2: 筛选某列值等于特定字符串的行
    expression = "name == 'example'"

    # 示例 3: 筛选多列满足条件的行
    expression = "(OBJECTID > 10) & (OBJECTID < 50)"

    # 示例 4: 筛选某列值在特定列表中的行
    expression = "OBJECTID.isin([11, 12, 13])"

    # 示例 5: 筛选某列值不为空的行
    expression = "name.notnull()"

    # 示例 6: 复杂条件组合
    expression = "((OBJECTID < 10) | (name == 'example')) & (start_p != 0)"

    print(f"条件筛选后的结果: \n {df.filter(expression)} \n")

    ## 8.1 sum
    print(f"统计某一数值列的和: {df.sum("start_l")}")
    ## 8.2 mean
    print(f"统计某一数值列的平均值: {df.mean("start_l")}")
    ## 8.3 min
    print(f"统计某一数值列的最小值: {df.min("start_l")}")
    ## 8.4 max
    print(f"统计某一数值列的最大值: {df.max("start_l")}")

    print(f"按照某个列的值升序或者降序: \n {df.sort('start_l', order='descending')}")

    ## 10.1 sql
    sql_str = ("select OBJECTID, start_l, end_l "
               "from dataframe "
               "where OBJECTID <= 30 "
               "order by OBJECTID desc ")

    print(f"sql执行结果: {df.sql(sql_str)}")


def calculate_file_hash(file_path, hash_algorithm="md5"):
    """计算文件的哈希值"""
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def verify_base64(base64_str, original_file_path, hash_algorithm="md5"):
    """验证分批返回的 base64 字符串是否与源文件一致"""
    # 解码 base64 字符串
    decoded_data = base64.b64decode(base64_str)
    # 计算解码数据的哈希值
    decoded_hash = hashlib.new(hash_algorithm, decoded_data).hexdigest()
    # 计算源文件的哈希值
    original_hash = calculate_file_hash(original_file_path, hash_algorithm)
    # 比较哈希值
    return decoded_hash == original_hash

def test_download_file_stream():
    """
    TODO 测试通过Flight流式下载服务器文件并保存到本地
    """
    url = "dacp://localhost:3101"
    conn = DacpClient.connect(url)
    # 你可以根据实际情况选择dataframe_name和file_type
    dataframe_name = "faird://dataset_name/dataframe_name"
    file_type = "tif"
    local_save_path = "downloaded_from_server.tif"

    print(f"开始流式下载 {dataframe_name} 文件类型 {file_type} 到本地 {local_save_path}")
    with open(local_save_path, "wb") as f:
        for chunk in conn.download_file_stream(dataframe_name, file_type=file_type):
            f.write(chunk)
    print(f"下载完成，已保存到 {local_save_path}")

if __name__ == "__main__":
    test_sdk()