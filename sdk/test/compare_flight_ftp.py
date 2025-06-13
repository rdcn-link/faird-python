import os
import time
import psutil
import threading
import ftplib
import pandas as pd
from sdk.dacp_client import DacpClient, Principal

from utils.logger_utils import get_logger
logger = get_logger(__name__)

# ===== 基础配置 =====
SERVER_URL = "dacp://localhost:3101"
USERNAME = "user1@cnic.cn"
TENANT = "conet"
CLIENT_ID = "faird-user1"
CSV_FILE_PATH = "/data/faird/test-data/2019年中国榆林市沟道信息.csv"
LOCAL_CSV_PATH = "temp_downloaded.csv"

FTP_HOST = "example.com"
FTP_USER = "ftp_user"
FTP_PASS = "ftp_password"
FTP_REMOTE_PATH = "/remote/path/2019年中国榆林市沟道信息.csv"

CONCURRENCY = 5  # 并发线程数
RUN_TIMES = 3  # 每项测试运行次数

process = psutil.Process(os.getpid())

# ===== 工具函数 =====
def get_mem_usage():
    return process.memory_info().rss / (1024 ** 2)

def flight_test():
    logger.info("开始 Arrow Flight 测试...")
    mem_before = get_mem_usage()

    start_connect = time.time()
    conn = DacpClient.connect(SERVER_URL, Principal.oauth(TENANT, CLIENT_ID, USERNAME))
    connect_time = time.time() - start_connect

    start_open = time.time()
    df = conn.open(CSV_FILE_PATH)
    total_time = time.time() - start_connect

    mem_after = get_mem_usage()

    result = {
        "type": "flight",
        "connect_time": connect_time,
        "total_time": total_time,
        "rows": df.num_rows,
        "cols": len(df.schema),
        "memory_change_mb": mem_after - mem_before
    }
    logger.info(f"Flight 测试完成: {result}")
    return result

def ftp_test():
    logger.info("开始 FTP 测试...")

    mem_before = get_mem_usage()

    start_connect = time.time()
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login(user=FTP_USER, passwd=FTP_PASS)
    connect_time = time.time() - start_connect

    start_download = time.time()
    with open(LOCAL_CSV_PATH, 'wb') as f:
        ftp.retrbinary(f"RETR {FTP_REMOTE_PATH}", f.write)
    download_time = time.time() - start_download

    start_parse = time.time()
    df = pd.read_csv(LOCAL_CSV_PATH)
    parse_time = time.time() - start_parse

    total_time = time.time() - start_connect
    mem_after = get_mem_usage()

    result = {
        "type": "ftp",
        "connect_time": connect_time,
        "download_time": download_time,
        "parse_time": parse_time,
        "total_time": total_time,
        "rows": len(df),
        "cols": len(df.columns),
        "memory_change_mb": mem_after - mem_before
    }
    logger.info(f"FTP 测试完成: {result}")
    return result

# ===== 多线程并发测试 =====
def run_benchmark(test_func, name, repeat_times=RUN_TIMES):
    results = []
    for i in range(repeat_times):
        logger.info(f"{name} 第 {i+1}/{repeat_times} 次测试")
        result = test_func()
        results.append(result)
    return results

def concurrent_test(test_func, name, concurrency=CONCURRENCY, repeat_times=1):
    all_results = []

    def worker():
        for _ in range(repeat_times):
            result = test_func()
            all_results.append(result)

    threads = []
    for _ in range(concurrency):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logger.info(f"{name} 并发测试完成，共 {len(all_results)} 次结果")
    return all_results

# ===== 主程序入口 =====
if __name__ == "__main__":
    logger.info("====== 网络连接效率对比测试 ======")

    # 单次测试
    flight_results = run_benchmark(flight_test, "Arrow Flight")
    ftp_results = run_benchmark(ftp_test, "FTP")

    # 并发测试（可选）
    # logger.info("启动并发测试（Flight）...")
    # flight_concurrent_results = concurrent_test(flight_test, "Arrow Flight", concurrency=CONCURRENCY)
    #
    # logger.info("启动并发测试（FTP）...")
    # ftp_concurrent_results = concurrent_test(ftp_test, "FTP", concurrency=CONCURRENCY)

    # 输出统计摘要
    def summarize(results):
        connect_avg = sum(r["connect_time"] for r in results) / len(results)
        total_avg = sum(r["total_time"] for r in results) / len(results)
        memory_avg = sum(r["memory_change_mb"] for r in results) / len(results)
        rows_avg = sum(r["rows"] for r in results) / len(results)
        cols_avg = sum(r["cols"] for r in results) / len(results)
        return {
            "avg_connect_time": connect_avg,
            "avg_total_time": total_avg,
            "avg_memory_change_mb": memory_avg,
            "avg_rows": round(rows_avg),
            "avg_cols": round(cols_avg)
        }

    logger.info("\n===== 测试结果汇总 =====")
    logger.info("Arrow Flight:")
    logger.info(summarize(flight_results))

    logger.info("\nFTP:")
    logger.info(summarize(ftp_results))

    logger.info("\n✅ 所有测试完成")
