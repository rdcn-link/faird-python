import pandas as pd
import time
import psutil
import os
from utils.logger_utils import get_logger
logger = get_logger(__name__)

# 工具函数：监控资源占用
def measure(func, *args, **kwargs):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss
    cpu_percent_before = psutil.cpu_percent(interval=None)

    start = time.time()
    result = func(*args, **kwargs)
    duration = time.time() - start

    mem_after = process.memory_info().rss
    cpu_percent_after = psutil.cpu_percent(interval=1.0)

    logger.info(f"操作: {func.__name__}")
    logger.info(f"用时: {duration:.4f}s")
    logger.info(f"内存占用: {(mem_after - mem_before) / 1024 / 1024:.2f} MB")
    logger.info(f"CPU 使用率: {cpu_percent_after}%\n")

    return result

# 加载 CSV 文件
def load_data(file_path):
    return pd.read_csv(file_path)

# 示例操作
def filter_data(df):
    return df[df['value'] > 100]

def select_columns(df):
    return df[['lat', 'lon']]

def sort_data(df):
    return df.sort_values(by='time')

def groupby_agg(df):
    return df.groupby('city')['temp'].mean().reset_index()

def join_data(df1, df2):
    return df1.merge(df2, on='id', how='inner')

def export_data(df):
    return df.to_csv("output.csv", index=False)

def correctness_check(df, expected_columns):
    return set(expected_columns).issubset(df.columns)

# 主流程
def main():
    file_path = "/data/faird/test-data/2019年中国榆林市沟道信息.csv"
    df = measure(load_data, file_path)

    df_filtered = measure(filter_data, df)
    df_selected = measure(select_columns, df)
    df_sorted = measure(sort_data, df)
    df_grouped = measure(groupby_agg, df)

    # 模拟连接数据
    df2 = df.copy()
    df_joined = measure(join_data, df, df2)

    measure(export_data, df)

    # 正确性验证
    expected = ['lat', 'lon']
    is_correct = correctness_check(df_selected, expected)
    logger.info(f"正确性验证: {'通过 ✅' if is_correct else '失败 ❌'}")

if __name__ == "__main__":
    main()
