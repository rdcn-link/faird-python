import numpy as np
import rasterio
from rasterio.transform import from_origin
from sdk.dacp_client import DacpClient, Principal
from utils.logger_utils import get_logger
logger = get_logger(__name__)

SERVER_URL = "dacp://localhost:3101"
USERNAME = "user1@cnic.cn"
TENANT = "conet"
CLIENT_ID = "faird-user1"

INPUT_TIFF_PATH = "/Users/zhouziang/Documents/test-data/tif/sample.tiff"
OUTPUT_TIFF_PATH = "/Users/zhouziang/Documents/test-data/tif/test_data_output.tif"





def test_tiff_file():


    # 1. 连接服务并加载数据
    conn = DacpClient.connect(SERVER_URL, Principal.oauth(TENANT))
    logger.info("正在加载 DataFrame...")
    df = conn.open(INPUT_TIFF_PATH)
    if df is None:
        logger.info("加载失败：faird.open 返回 None。请检查 parser 或文件路径。")
        return
    logger.info("DataFrame 加载成功")

    # 2. 写出 TIFF 文件
    logger.info(f"正在使用 df.write(...) 转换文件到: {OUTPUT_TIFF_PATH}")
    try:
        df.write(OUTPUT_TIFF_PATH, INPUT_TIFF_PATH)
        logger.info(f"成功从df转换为文件: {OUTPUT_TIFF_PATH}")
    except Exception as e:
        logger.info(f"转换文件失败: {e}")
        return

    # # 3. 验证转换前后一致性
    # if compare_tiff_files(INPUT_TIFF_PATH, OUTPUT_TIFF_PATH):
    #     logger.info("TIFF 文件转换前后内容一致")
    # else:
    #     logger.info("TIFF 文件存在差异，请检查转换过程")


def compare_tiff_files(original_path, output_path):
    """
    比较两个 TIFF 文件是否一致：
    - 元信息（尺寸、投影、变换矩阵）
    - 像素数据
    """
    with rasterio.open(original_path) as src1, rasterio.open(output_path) as src2:

        # 比较元数据
        if src1.meta != src2.meta:
            logger.info("元数据不一致")
            return False

        # 比较数据
        data1 = src1.read()
        data2 = src2.read()

        if not np.array_equal(data1, data2):
            logger.info("像素数据不一致")
            return False

    logger.info("所有检查项通过，TIFF 文件完整一致")
    return True


if __name__ == "__main__":
    test_tiff_file()
