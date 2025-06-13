import os, sys
import time
import psutil
import threading
import ftplib
import pandas as pd
import numpy as np

sys.path.append("/data/faird")
from sdk.dacp_client import DacpClient, Principal

from utils.logger_utils import get_logger
logger = get_logger(__name__)

# ===== 基础配置 =====
SERVER_URL = "dacp://10.0.89.38:3101"
USERNAME = "user1@cnic.cn"
PASSWORD = "user1@cnic.cn"
TENANT = "conet"

FTP_HOST = "10.0.89.38"
FTP_USER = "ftpuser"
FTP_PASS = "ftpuser"

CONCURRENCY = 5  # 并发线程数
RUN_TIMES = 3  # 每项测试运行次数

process = psutil.Process(os.getpid())

# ===== 测试数据配置 =====
TEST_DATASETS = [
    {
        "name": "small_csv",
        "local_path": "dacp://10.0.89.38:3101/GFS/small_100rows.csv",
        "local_real_path": "/data/faird/test-data/small_100rows.csv",
        "ftp_path": "remote/path/small_100rows.csv",
        "description": "小文件 - 100行数据",
        "expected_rows": 100
    },
    {
        "name": "medium_csv",
        "local_path": "dacp://60.245.194.25:50201/GFS/medium_10k_rows.csv",
        "local_real_path": "/data/faird/test-data/medium_10k_rows.csv",
        "ftp_path": "remote/path/medium_10k_rows.csv",
        "description": "中等文件 - 10K行数据",
        "expected_rows": 10000
    },
    {
        "name": "large_csv",
        "local_path": "dacp://60.245.194.25:50201/GFS/large_100k_rows.csv",
        "local_real_path": "/data/faird/test-data/large_100k_rows.csv",
        "ftp_path": "remote/path/large_100k_rows.csv",
        "description": "大文件 - 100K行数据",
        "expected_rows": 100000
    },
    {
        "name": "xlarge_csv",
        "local_path": "dacp://60.245.194.25:50201/GFS/xlarge_1m_rows.csv",
        "local_real_path": "/data/faird/test-data/xlarge_1m_rows.csv",
        "ftp_path": "remote/path/xlarge_1m_rows.csv",
        "description": "超大文件 - 1M行数据",
        "expected_rows": 1000000
    },
    {
        "name": "xxlarge_csv",
        "local_path": "dacp://60.245.194.25:50201/GFS/xxlarge_10m_rows.csv",
        "local_real_path": "/data/faird/test-data/xxlarge_10m_rows.csv",
        "ftp_path": "remote/path/xxlarge_10m_rows.csv",
        "description": "超大文件 - 10M行数据",
        "expected_rows": 10000000
    }
]


# ===== 数据生成函数 =====
def generate_test_csv_files():
    """生成不同大小的测试CSV文件"""
    logger.info("开始生成测试CSV文件...")

    # 基础列结构
    base_columns = [
        '沟道编号', '沟道名称', '所属区县', '长度_km', '深度_m',
        '宽度_m', '坡度_percent', '土壤类型', '植被覆盖率', '年降雨量_mm',
        '经度', '纬度', '海拔_m', '建设年份', '维护状态'
    ]

    for dataset in TEST_DATASETS:
        file_path = dataset["local_path"]
        rows = dataset["expected_rows"]

        # 创建目录
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path):
            logger.info(f"文件已存在，跳过生成: {file_path}")
            continue

        logger.info(f"生成 {dataset['description']} -> {file_path}")

        # 生成随机数据
        data = {
            '沟道编号': [f"GD{i:06d}" for i in range(1, rows + 1)],
            '沟道名称': [f"沟道_{i}" for i in range(1, rows + 1)],
            '所属区县': np.random.choice(['榆阳区', '横山区', '靖边县', '定边县'], rows),
            '长度_km': np.round(np.random.uniform(0.5, 50.0, rows), 2),
            '深度_m': np.round(np.random.uniform(1.0, 15.0, rows), 2),
            '宽度_m': np.round(np.random.uniform(2.0, 25.0, rows), 2),
            '坡度_percent': np.round(np.random.uniform(1.0, 30.0, rows), 1),
            '土壤类型': np.random.choice(['黄土', '沙土', '黏土', '混合土'], rows),
            '植被覆盖率': np.round(np.random.uniform(10.0, 90.0, rows), 1),
            '年降雨量_mm': np.random.randint(200, 800, rows),
            '经度': np.round(np.random.uniform(107.0, 111.0, rows), 6),
            '纬度': np.round(np.random.uniform(37.0, 40.0, rows), 6),
            '海拔_m': np.random.randint(800, 1800, rows),
            '建设年份': np.random.randint(1990, 2023, rows),
            '维护状态': np.random.choice(['良好', '一般', '需维修', '已损坏'], rows)
        }

        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, encoding='utf-8')

        file_size_mb = os.path.getsize(file_path) / (1024 ** 2)
        logger.info(f"生成完成: {rows}行, {file_size_mb:.2f}MB")


# ===== 工具函数 =====
def get_mem_usage():
    return process.memory_info().rss / (1024 ** 2)


def get_file_size_mb(filepath):
    """获取文件大小(MB)"""
    if os.path.exists(filepath):
        return os.path.getsize(filepath) / (1024 ** 2)
    return 0


def flight_test(dataset):
    """Arrow Flight测试"""
    logger.info(f"开始 Arrow Flight 测试: {dataset['description']}")
    mem_before = get_mem_usage()

    try:
        start_connect = time.time()
        conn = DacpClient.connect(SERVER_URL)
        connect_time = time.time() - start_connect

        start_open = time.time()
        df = conn.open(dataset['local_path'])
        df.collect()
        read_time = time.time() - start_open
        total_time = time.time() - start_connect

        mem_after = get_mem_usage()
        file_size_mb = get_file_size_mb(dataset['local_real_path'])

        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "flight",
            "connect_time": connect_time,
            "read_time": read_time,
            "total_time": total_time,
            "rows": df.num_rows,
            "cols": len(df.schema),
            "file_size_mb": file_size_mb,
            "throughput_mbps": file_size_mb / total_time if total_time > 0 else 0,
            "rows_per_sec": df.num_rows / total_time if total_time > 0 else 0,
            "memory_change_mb": mem_after - mem_before,
            "success": True
        }

        #conn.close()

    except Exception as e:
        logger.error(f"Flight测试失败: {e}")
        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "flight",
            "error": str(e),
            "success": False
        }

    logger.info(f"Flight 测试完成: {dataset['name']}")
    return result


def ftp_test(dataset):
    """FTP测试"""
    logger.info(f"开始 FTP 测试: {dataset['description']}")
    mem_before = get_mem_usage()
    local_temp_file = f"temp_{dataset['name']}.csv"

    try:
        start_connect = time.time()
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        connect_time = time.time() - start_connect

        start_download = time.time()
        with open(local_temp_file, 'wb') as f:
            ftp.retrbinary(f"RETR {dataset['ftp_path']}", f.write)
        download_time = time.time() - start_download

        start_parse = time.time()
        df = pd.read_csv(local_temp_file)
        parse_time = time.time() - start_parse

        total_time = time.time() - start_connect
        mem_after = get_mem_usage()
        file_size_mb = get_file_size_mb(local_temp_file)

        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "ftp",
            "connect_time": connect_time,
            "download_time": download_time,
            "parse_time": parse_time,
            "total_time": total_time,
            "rows": len(df),
            "cols": len(df.columns),
            "file_size_mb": file_size_mb,
            "throughput_mbps": file_size_mb / total_time if total_time > 0 else 0,
            "rows_per_sec": len(df) / total_time if total_time > 0 else 0,
            "memory_change_mb": mem_after - mem_before,
            "success": True
        }

        ftp.quit()

        # 清理临时文件
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)

    except Exception as e:
        logger.error(f"FTP测试失败: {e}")
        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "ftp",
            "error": str(e),
            "success": False
        }

        # 清理临时文件
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)

    logger.info(f"FTP 测试完成: {dataset['name']}")
    return result


# ===== 批量测试函数 =====
def run_single_protocol_tests(test_func, protocol_name, datasets, repeat_times=RUN_TIMES):
    """运行单个协议的所有数据集测试"""
    all_results = []

    for dataset in datasets:
        logger.info(f"\n--- {protocol_name} 测试数据集: {dataset['description']} ---")
        dataset_results = []

        for i in range(repeat_times):
            logger.info(f"{protocol_name} {dataset['name']} 第 {i + 1}/{repeat_times} 次测试")
            result = test_func(dataset)
            if result.get('success', False):
                dataset_results.append(result)
            time.sleep(1)  # 避免连接过于频繁

        all_results.extend(dataset_results)

        # 输出当前数据集的统计
        if dataset_results:
            avg_connect = sum(r["connect_time"] for r in dataset_results) / len(dataset_results)
            avg_total = sum(r["total_time"] for r in dataset_results) / len(dataset_results)
            avg_throughput = sum(r["throughput_mbps"] for r in dataset_results) / len(dataset_results)
            memory_change_mb = sum(r["memory_change_mb"] for r in dataset_results) / len(dataset_results)
            logger.info(
                f"{dataset['name']} 平均连接时间: {avg_connect:.3f}s, 总时间: {avg_total:.3f}s, 吞吐量: {avg_throughput:.2f}MB/s, 占用内存变化: {memory_change_mb:.2f}MB")

    return all_results


def run_concurrent_tests(test_func, protocol_name, dataset, concurrency=CONCURRENCY, repeat_times=1):
    """并发测试"""
    all_results = []

    def worker():
        for _ in range(repeat_times):
            result = test_func(dataset)
            if result.get('success', False):
                all_results.append(result)

    logger.info(f"\n--- {protocol_name} 并发测试 ({concurrency}线程): {dataset['description']} ---")

    threads = []
    start_time = time.time()

    for _ in range(concurrency):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    concurrent_total_time = time.time() - start_time

    logger.info(f"{protocol_name} 并发测试完成: {len(all_results)}个成功结果, 总耗时: {concurrent_total_time:.2f}s")
    return all_results, concurrent_total_time


# ===== 结果分析函数 =====
def analyze_results(results, protocol_name):
    """分析测试结果"""
    if not results:
        logger.warning(f"{protocol_name} 没有有效结果")
        return {}

    # 按数据集分组统计
    dataset_stats = {}
    for result in results:
        dataset_name = result['dataset_name']
        if dataset_name not in dataset_stats:
            dataset_stats[dataset_name] = []
        dataset_stats[dataset_name].append(result)

    analysis = {
        "protocol": protocol_name,
        "total_tests": len(results),
        "datasets": {}
    }

    for dataset_name, dataset_results in dataset_stats.items():
        if not dataset_results:
            continue

        stats = {
            "count": len(dataset_results),
            "avg_connect_time": sum(r["connect_time"] for r in dataset_results) / len(dataset_results),
            "avg_total_time": sum(r["total_time"] for r in dataset_results) / len(dataset_results),
            "avg_throughput_mbps": sum(r["throughput_mbps"] for r in dataset_results) / len(dataset_results),
            "avg_rows_per_sec": sum(r["rows_per_sec"] for r in dataset_results) / len(dataset_results),
            "avg_memory_change_mb": sum(r["memory_change_mb"] for r in dataset_results) / len(dataset_results),
            "avg_file_size_mb": sum(r["file_size_mb"] for r in dataset_results) / len(dataset_results),
            "min_total_time": min(r["total_time"] for r in dataset_results),
            "max_total_time": max(r["total_time"] for r in dataset_results)
        }

        analysis["datasets"][dataset_name] = stats

    return analysis


def print_comparison_report(flight_analysis, ftp_analysis):
    """打印对比报告"""
    logger.info("\n" + "=" * 80)
    logger.info("                        FAIRD vs FTP 性能对比报告")
    logger.info("=" * 80)

    print(f"\n{'数据集':<15} {'协议':<8} {'连接时间(s)':<12} {'总时间(s)':<10} {'吞吐量(MB/s)':<12} {'行/秒':<10}")
    print("-" * 80)

    for dataset_name in flight_analysis.get("datasets", {}):
        flight_stats = flight_analysis["datasets"].get(dataset_name, {})
        ftp_stats = ftp_analysis["datasets"].get(dataset_name, {})

        if flight_stats:
            print(f"{dataset_name:<15} {'FAIRD':<8} {flight_stats['avg_connect_time']:<12.3f} "
                  f"{flight_stats['avg_total_time']:<10.3f} {flight_stats['avg_throughput_mbps']:<12.2f} "
                  f"{flight_stats['avg_rows_per_sec']:<10.0f}")

        if ftp_stats:
            print(f"{'':<15} {'FTP':<8} {ftp_stats['avg_connect_time']:<12.3f} "
                  f"{ftp_stats['avg_total_time']:<10.3f} {ftp_stats['avg_throughput_mbps']:<12.2f} "
                  f"{ftp_stats['avg_rows_per_sec']:<10.0f}")

        # 计算性能比较
        if flight_stats and ftp_stats:
            connect_ratio = ftp_stats['avg_connect_time'] / flight_stats['avg_connect_time']
            total_ratio = ftp_stats['avg_total_time'] / flight_stats['avg_total_time']
            throughput_ratio = flight_stats['avg_throughput_mbps'] / ftp_stats['avg_throughput_mbps']

            print(f"{'比较':<15} {'FAIRD':<8} {f'快{connect_ratio:.1f}x':<12} "
                  f"{f'快{total_ratio:.1f}x':<10} {f'快{throughput_ratio:.1f}x':<12} {'':<10}")

        print("-" * 80)


# ===== 主程序 =====
if __name__ == "__main__":
    logger.info("====== FAIRD vs FTP 性能对比测试 ======")

    # 1. 生成测试数据
    # generate_test_csv_files()

    # 2. 单线程性能测试
    logger.info("\n🚀 开始单线程性能测试...")

    flight_results = run_single_protocol_tests(flight_test, "FAIRD", TEST_DATASETS, RUN_TIMES)
    ftp_results = run_single_protocol_tests(ftp_test, "FTP", TEST_DATASETS, RUN_TIMES)

    # 4. 结果分析
    logger.info("\n📊 分析测试结果...")

    flight_analysis = analyze_results(flight_results, "FAIRD")
    ftp_analysis = analyze_results(ftp_results, "FTP")

    # 5. 输出报告
    print_comparison_report(flight_analysis, ftp_analysis)
    sys.exit(0)



    # 3. 并发性能测试（选择中等大小数据集）
    logger.info("\n🚀 开始并发性能测试...")

    medium_dataset = next((d for d in TEST_DATASETS if d['name'] == 'medium_csv'), TEST_DATASETS[1])

    flight_concurrent_results, flight_concurrent_time = run_concurrent_tests(
        flight_test, "FAIRD", medium_dataset, CONCURRENCY, 1)

    ftp_concurrent_results, ftp_concurrent_time = run_concurrent_tests(
        ftp_test, "FTP", medium_dataset, CONCURRENCY, 1)

    # 6. 并发测试结果
    logger.info(f"\n🔄 并发测试结果 ({CONCURRENCY}线程):")
    logger.info(f"FAIRD: {len(flight_concurrent_results)}个成功, 总耗时: {flight_concurrent_time:.2f}s")
    logger.info(f"FTP:   {len(ftp_concurrent_results)}个成功, 总耗时: {ftp_concurrent_time:.2f}s")

    if flight_concurrent_results and ftp_concurrent_results:
        flight_avg_concurrent = sum(r["total_time"] for r in flight_concurrent_results) / len(flight_concurrent_results)
        ftp_avg_concurrent = sum(r["total_time"] for r in ftp_concurrent_results) / len(ftp_concurrent_results)
        logger.info(f"并发平均响应时间 - FAIRD: {flight_avg_concurrent:.3f}s, FTP: {ftp_avg_concurrent:.3f}s")

    logger.info("\n✅ 所有测试完成！")