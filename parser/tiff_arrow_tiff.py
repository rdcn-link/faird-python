import tifffile
import pyarrow as pa
import pyarrow.ipc as ipc
from utils.logger_utils import get_logger
logger = get_logger(__name__)

def tiff_to_arrow(tiff_file, arrow_file):
    """
    将 TIFF 图像转换为 Arrow 格式文件

    Args:
        tiff_file (str): 输入 TIFF 文件路径
        arrow_file (str): 输出 Arrow 文件路径
    """
    # 读取 TIFF 图像
    with tifffile.TiffFile(tiff_file) as tif:
        array = tif.asarray()
        shape = array.shape
        dtype = str(array.dtype)

    # 展平并保存为 Arrow 表
    flat_array = array.flatten()

    # 让每个数据点都关联形状信息
    shape_array = [shape] * len(flat_array)  # 重复形状以匹配数据的长度

    # 创建表
    table = pa.table({
        "data": pa.array(flat_array),
        "shape": pa.array(shape_array),
        "dtype": pa.array([dtype] * len(flat_array))  # 重复dtype以匹配数据的长度
    })

    # 写入 Arrow 文件
    with ipc.new_file(arrow_file, table.schema) as writer:
        writer.write_table(table)
    logger.info(f"成功将 {tiff_file} 转换为 {arrow_file}")




import tifffile
import pyarrow.ipc as ipc
import numpy as np

def arrow_to_tiff(arrow_file, tiff_file):
    """
    从 Arrow 格式文件还原 TIFF 图像

    Args:
        arrow_file (str): 输入 Arrow 文件路径
        tiff_file (str): 输出 TIFF 文件路径
    """
    # 读取 Arrow 表
    with ipc.open_file(arrow_file) as reader:
        table = reader.read_all()

    # 提取并还原图像数据
    flat_array = table.column("data").to_numpy()
    shape = tuple(table.column("shape")[0].as_py())
    dtype = table.column("dtype")[0].as_py()

    # 还原成原始图像形状
    array = np.array(flat_array, dtype=dtype).reshape(shape)

    # 保存为 TIFF 文件
    tifffile.imwrite(tiff_file, array)
    logger.info(f"成功将 {arrow_file} 转换为 {tiff_file}")


if __name__ == "__main__":
    tiff_to_arrow("sample.tiff", "output.arrow")

    logger.info("----------------------------------------")

    arrow_to_tiff("output.arrow", "restored.tiff")
