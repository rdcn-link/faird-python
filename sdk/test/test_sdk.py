import base64
import hashlib
import json

from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame
import os
import uuid
from utils.logger_utils import get_logger
logger = get_logger(__name__)


def test_sdk():
    url = "dacp://localhost:3101"
    signature = "aYii16AHpVH0YNhXtu6Q/r9I3bUmccH7hEVuaqmzUWvzvwqPuYeH8VtBz4XsDSmCii2GTi+4ZOfYEe/QfIaNccGMjwUM5we6H4HfkXYTaKnBllgRnh9/RtzgGB2oEHXMHkX3Sep+r0HFgqp7xC3r+a1hQuGrewt8/97WVVKVfFuVarWncDmrUe4GKCgJz8zcINEpBi4NKu2/qLGs3hwh9iymfj1QZAheXXQP+xw3BYVkNT6rq3HYA0Ux0QIslsWv13+ud4fFEbFftVODIoPp72JB7qiq4Kq8xZcifCiVxC69tPLcYv4p99WOLZ9KPe7ysUEMqTEMA4tfa8LsBqvlqA=="
    conn = DacpClient.connect(url, Principal.controld(domain_name="hello from java", signature=signature))


    # url = "dacp://60.245.194.25:50201"
    url = "dacp://localhost:3101"
    username = "faird-user1"
    password = "user1@cnic.cn"
    conn = DacpClient.connect(url, Principal.oauth("conet", username=username, password=password))

    #datasets = conn.list_datasets()
    #dataframes = conn.list_dataframes(datasets[1]) # 这个数据集下有3个文件（dataframe）
    #dataframe_name = dataframes[0]['dataframeName'] # 这个是第一个dataframe，dir类型，有3行，每一行是一个文件，最后一列是blob
    dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat"
    df = conn.open(dataframe_name)
    print(df) # 此时blob=None

    blob_reader = df[2]["blob"]  # 获取 blob 列的流式数据

    # 流式读取 blob 数据
    for chunk in blob_reader:
        print(f"Chunk size: {len(chunk)} bytes")

    # dataframe = dataframes[0]
    # dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/SOCATv2021_tracks_gridded_decadal.csv"
    # df = conn.open(dataframe_name)
    # print(df)
    # a = df[0]['start_p']  # 打印第一行的'start_p'列的值)
    # tt = df[0]['blob']
    # # for chunk in blob_reader:
    # #     print(f"Chunk size: {len(chunk)} bytes")
    #     # 处理chunk数据，例如保存到文件或其他操作
    #     # with open("output_file", "wb") as f:
    #     #     f.write(chunk)
    #
    #
    #
    # # sample = conn.sample(dir_dataframe_name)
    #
    #
    # #conn = DacpClient.connect(url, Principal.ANONYMOUS)
    #

    #
    #
    # #conn = DacpClient.connect(url)
    #
    # ## !! for local test
    # #dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/SOCATv2021_qrtrdeg_gridded_coast_monthly.nc"
    # # dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/SOCATv2021_tracks_gridded_decadal.csv"
    # # # dataframe_name = "dacp://0.0.0.0:3101/中尺度涡旋数据集/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atld das/SOCATv2021_Gridded_Dat/sample.tiff"
    # # # # dataframe_name = r"dacp://0.0.0.0:3101/中尺度涡旋数据集/D:\test\faird\SOCATv2021_Gridded_Dat\SOCATv2021_qrtrdeg_gridded_coast_monthly.nc"
    # # count = conn.count(dataframe_name)
    # # sample = conn.sample(dataframe_name)
    # # total_size = 0
    # # for chunk in conn.get_dataframe_stream(dataframe_name, max_chunksize=1024 * 500):
    # #     total_size += len(chunk)
    # # df = conn.open(dataframe_name)
    # # # print(sample)
    #
    # datasets = conn.list_datasets()
    # # has_permission = conn.check_permission(datasets[5], "faird-user1")
    # # metadata = conn.get_dataset(datasets[56])

    conn = DacpClient.connect("dacp://60.245.194.25:50201")
    datasets = conn.list_datasets()
    dataframes = conn.list_dataframes(datasets[56]) # 这个数据集下有3个文件（dataframe）
    dataframe_name = dataframes[0]['dataframeName'] # 这个是第一个dataframe，dir类型，有3行，每一行是一个文件，最后一列是blob
    df = conn.open(dataframe_name)
    print(df) # 此时blob=None

    # # 改流式
    # for chunk in conn.list_dataframes_stream(datasets[12]):
    #     print(f"Chunk size: {len(chunk)}")

    # dataframe_name = dataframes[1]['dataframeName']
    #
    # sample = conn.sample(dataframe_name)
    # count = conn.count(dataframe_name)
    #
    # total_size = 0
    # for chunk in conn.get_dataframe_stream(dataframe_name, max_chunksize=1024*1024*5):
    #     total_size += len(chunk)
    # print(f"total size: {total_size} Bytes")

    dataframe_name = 'dacp://60.245.194.25:50201/2m陆表气温数据集/historical/SD016-GHCN_CAMS/Derived/air.mon.1981-2010.ltm.nc'
    output_file_path = "/Users/yaxuan/Desktop/output.nc"
    with open(output_file_path, 'wb') as f:
        for chunk in conn.get_dataframe_stream(dataframe_name, max_chunksize=1024*1024*5):
            f.write(chunk)
    print(f"数据已写入到 {output_file_path}")

    sample = conn.sample(dataframe_name)
    count = conn.count(dataframe_name)

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
    nc_dataframe_name = dataframes[4]['dataframeName']
    sample = conn.sample(nc_dataframe_name)
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