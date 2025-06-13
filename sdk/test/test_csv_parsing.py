import os
import time
import threading
import psutil
from sdk.dacp_client import DacpClient, Principal
from utils.logger_utils import get_logger
logger = get_logger(__name__)

# ===== 基础配置 =====
SERVER_URL = "dacp://localhost:3101"
USERNAME = "user1@cnic.cn"
TENANT = "conet"
CLIENT_ID = "faird-user1"



CSV_FILE_PATH = "/data/faird/test-data/2019年中国榆林市沟道信息.csv"
# CSV_FILE_PATH = "/data/faird/test-data/sample.tiff"
CONCURRENCY = 10  # 并发线程数

process = psutil.Process(os.getpid())

# ===== 单个文件解析测试 =====
def parse_csv_file(file_path):
    try:
        logger.info(f"\n开始解析文件：{file_path}")
        conn = DacpClient.connect(SERVER_URL, Principal.oauth(TENANT, CLIENT_ID, USERNAME))

        mem_before = process.memory_info().rss / (1024 ** 2)

        start = time.time()
        df = conn.open(file_path)
        parse_time = time.time() - start

        mem_after = process.memory_info().rss / (1024 ** 2)

        logger.info(f"[✓] 文件：{file_path}")
        logger.info(f"    - 解析耗时: {parse_time:.3f} 秒")
        logger.info(f"    - 行数: {df.num_rows}, 列数: {len(df.schema)}")
        logger.info(f"    - 内存占用变化: {mem_after - mem_before:.2f} MB")

    except Exception as e:
        logger.info(f"[✗] 解析失败：{file_path}, 错误信息：{str(e)}")

# ===== 并发测试 =====
def parse_csv_concurrently(file_path, concurrency):
    logger.info(f"\n启动并发解析测试（线程数: {concurrency}）")
    threads = []

    for i in range(concurrency):
        t = threading.Thread(target=parse_csv_file, args=(file_path,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    logger.info(f"并发测试完成（共 {concurrency} 个任务）")

# ===== 主函数入口 =====
if __name__ == "__main__":
    logger.info("====== CSV 文件解析性能测试 ======")
    logger.info(f"目标文件：{CSV_FILE_PATH}")

    # 单次解析
    ##parse_csv_file(CSV_FILE_PATH)

    # 并发解析
    #parse_csv_concurrently(CSV_FILE_PATH, CONCURRENCY)
