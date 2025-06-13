import os
import time
import psutil
import unittest
from sdk.dacp_client import DacpClient, Principal  # 假设这是用于获取 DataFrame 的客户端
from utils.logger_utils import get_logger
logger = get_logger(__name__)

class DataFramePerformanceTest(unittest.TestCase):
    SERVER_URL = "dacp://localhost:3101"
    USERNAME = "user1@cnic.cn"
    TENANT = "conet"
    CLIENT_ID = "faird-user1"
    CSV_FILE_PATH = "/data/faird/test-data/2019年中国榆林市沟道信息.csv"

    @classmethod
    def setUpClass(cls):
        cls.conn = DacpClient.connect(cls.SERVER_URL, Principal.oauth(cls.TENANT, cls.CLIENT_ID, cls.USERNAME))
        cls.df = cls.conn.open(cls.CSV_FILE_PATH)
        cls.process = psutil.Process(os.getpid())

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def setUp(self):
        self.mem_before = self.process.memory_info().rss / (1024 ** 2)

    def tearDown(self):
        mem_after = self.process.memory_info().rss / (1024 ** 2)
        logger.info(f"内存占用变化: {mem_after - self.mem_before:.2f} MB")

    def measure_performance(self, operation, description):
        start_time = time.time()
        result = operation()
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"{description} 耗时: {duration:.3f} 秒")
        return result, duration

    ### 筛选操作测试
    def test_filter(self):
        operation = lambda: self.df.filter("value > 100")
        self.measure_performance(operation, "筛选操作")

    ### 投影操作测试
    def test_select(self):
        operation = lambda: self.df.select("lat", "lon")
        self.measure_performance(operation, "投影操作")

    ### 排序操作测试
    def test_order_by(self):
        operation = lambda: self.df.order_by("time")
        self.measure_performance(operation, "排序操作")

    ### 分组聚合操作测试
    def test_group_by_agg(self):
        operation = lambda: self.df.group_by("city").agg({"temp": "avg"})
        self.measure_performance(operation, "分组聚合操作")

    ### 连接操作测试
    def test_join(self):
        # 假设 df1 和 df2 是两个需要连接的 DataFrame
        df1 = self.df  # 示例中使用同一个 DataFrame
        df2 = self.df  # 实际应用中应为不同的 DataFrame
        operation = lambda: df1.join(df2, on="id")
        self.measure_performance(operation, "连接操作")

    ### 导出数据操作测试
    def test_collect(self):
        operation = lambda: self.df.collect()
        self.measure_performance(operation, "导出数据操作（collect）")

    def test_to_pandas(self):
        operation = lambda: self.df.to_pandas()
        self.measure_performance(operation, "导出数据操作（to_pandas）")

    ### 流式读取处理操作测试
    def test_get_stream(self):
        operation = lambda: self.df.get_stream()
        self.measure_performance(operation, "流式读取处理操作")

    ### CPU 使用率测试（示例）
    def test_cpu_usage(self):
        def cpu_intensive_operation():
            # 示例：一个 CPU 密集型操作
            sorted_df = self.df.order_by("time")
            return sorted_df

        start_time = time.time()
        result = cpu_intensive_operation()
        end_time = time.time()
        duration = end_time - start_time

        cpu_percent = self.process.cpu_percent(interval=duration)
        logger.info(f"CPU 密集型操作耗时: {duration:.3f} 秒, CPU 使用率: {cpu_percent}%")

    ### 可扩展性测试（示例）
    def test_scalability(self):
        small_df = self.df.limit(10000)
        large_df = self.df.limit(1000000)

        def operation(df):
            return df.order_by("time")

        small_result, small_duration = self.measure_performance(lambda: operation(small_df), "小数据量排序")
        large_result, large_duration = self.measure_performance(lambda: operation(large_df), "大数据量排序")

        logger.info(f"小数据量 vs 大数据量排序性能比: {small_duration / large_duration:.2f}")

    ### 正确性验证测试（示例）
    def test_correctness(self):
        filtered_df = self.df.filter("value > 100")
        selected_df = filtered_df.select("lat", "lon")
        aggregated_df = self.df.group_by("city").agg({"temp": "avg"})

        # 验证筛选结果
        self.assertTrue(all(row["value"] > 100 for row in filtered_df.collect()), "筛选结果不正确")

        # 验证投影结果
        self.assertEqual(set(selected_df.columns), {"lat", "lon"}, "投影结果列不正确")

        # 验证聚合结果
        for row in aggregated_df.collect():
            city_data = self.df.filter(f"city == '{row['city']}'")
            expected_avg = sum(row["temp"] for row in city_data.collect()) / city_data.count()
            self.assertAlmostEqual(row["avg(temp)"], expected_avg, places=2, msg="聚合结果不准确")


if __name__ == "__main__":
    unittest.main()
