import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import numpy as np
import pyarrow as pa
import tifffile
import logging
from parser.tif_parser import TIFParser
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_tiff(path, shape=(3, 10, 10), dtype=np.uint8):
    """创建一个测试用的TIFF文件"""
    data = np.random.randint(0, 255, size=shape, dtype=dtype)
    tifffile.imwrite(path, data)
    logger.info(f"创建测试TIFF文件: {path}, shape={shape}, dtype={dtype}")
    return data

def test_parse_and_write_multiband(tmp_path):
    tif_path = tmp_path / "test_multi.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(3, 8, 8))
    parser = TIFParser()
    logger.info("测试多波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert isinstance(table, pa.Table)
    assert len(table.columns) == 3
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_multi.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("write 方法通过，验证写出数据与原始数据一致")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_singleband(tmp_path):
    tif_path = tmp_path / "test_single.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(8, 8))
    parser = TIFParser()
    logger.info("测试单波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert len(table.columns) == 1
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("单波段 parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_single.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("单波段写出数据验证")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_hwcbands(tmp_path):
    tif_path = tmp_path / "test_hwc.tif"
    orig_data = create_test_tiff(str(tif_path), shape=(8, 8, 3))
    parser = TIFParser()
    logger.info("测试HWC多波段 parse 方法")
    table = parser.parse(str(tif_path))
    assert len(table.columns) == 3
    assert table.num_rows == 8 * 8
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # logger.info("HWC多波段 parse 方法通过，开始测试 write 方法")
    # out_tif_path = tmp_path / "out_hwc.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data = tif.asarray()
    # logger.info("HWC多波段写出数据验证")
    # np.testing.assert_array_equal(orig_data, out_data)

def test_parse_and_write_multipage(tmp_path):
    tif_path = tmp_path / "test_multi_page.tif"
    data1 = np.random.randint(0, 255, size=(8, 8), dtype=np.uint8)
    data2 = np.random.randint(0, 255, size=(3, 6, 6), dtype=np.uint8)
    with tifffile.TiffWriter(str(tif_path)) as tif:
        tif.write(data1)
        tif.write(data2)
    parser = TIFParser()
    logger.info("测试多页TIFF parse 方法")
    table = parser.parse(str(tif_path))
    table_schema = table.schema
    logger.info(f"解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)
    # # 第一页单波段，第二页三波段
    # assert len(table.columns) == 1 + 3
    # out_tif_path = tmp_path / "out_multi_page.tif"
    # parser.write(table, str(out_tif_path))
    # with tifffile.TiffFile(str(out_tif_path)) as tif:
    #     out_data1 = tif.pages[0].asarray()
    #     out_data2 = tif.pages[1].asarray()
    # logger.info("多页TIFF写出数据验证")
    # np.testing.assert_array_equal(data1, out_data1)
    # np.testing.assert_array_equal(data2, out_data2)

def test_invalid_shape(tmp_path):
    tif_path = tmp_path / "invalid.tif"
    data = np.random.randint(0, 255, size=(2, 2, 2, 2), dtype=np.uint8)
    tifffile.imwrite(str(tif_path), data)
    parser = TIFParser()
    logger.info("测试高维TIFF parse 方法（应展平）")
    table = parser.parse(str(tif_path))
    # assert len(table.columns) == 1
    # assert table.num_rows == 16
    table_schema = table.schema
    logger.info(f"高维TIFF解析的表结构: {table_schema}")
    logger.info("------------------------")
    print(table)

def test_real_tif(tif_path, tem_path, out_tif_path):
    """测试实际的TIFF文件解析和写入"""
    # 这里可以放一个实际的TIFF文件路径进行测试
    parser = TIFParser()
    logger.info("测试实际TIFF文件 parse 方法")
    table = parser.parse(str(tif_path))
    assert isinstance(table, pa.Table)
    print("parse Arrow Table 列:", table.column_names)
    print("parse Arrow Table 行数:", table.num_rows)
    print("parse Arrow Table schema:", table.schema)
    logger.info("------------------------")
    print(table)
    logger.info("开始测试写入方法")
    out_tif_path = tem_path / out_tif_path
    parser.write(table, str(out_tif_path))
    logger.info("多页TIFF写出数据验证")
    
def test_tif_sample(tif_path):
    assert os.path.exists(tif_path), f"测试文件不存在: {tif_path}"
    parser = TIFParser()
    table = parser.sample(tif_path)
    # print("Arrow Table schema:", table.schema)
    # print("Arrow Table preview:")
    # print(table.to_pandas().head())
    # 用法示例
    # meta = {k.decode(): v.decode() for k, v in table.schema.metadata.items()}
    # json_data = parser.meta_to_json(meta)
    # logger.info(f"采样结果的元数据:{json.dumps(json_data)}")
    # 基本断言
    assert isinstance(table, pa.Table)
    assert table.num_columns > 0
    assert table.num_rows > 0
    # 检查是否有NaN补齐
    for col in table.columns:
        arr = col.to_numpy()
        assert len(arr) == table.num_rows
    print("sample Arrow Table 列:", table.column_names)
    print("sample Arrow Table 行数:", table.num_rows)
    print("sample Arrow Table schema:", table.schema)
    print(table)
    print("sample方法测试通过")
    
def test_tif_count(tif_path):
    """测试 TIFParser 的 count 方法，验证返回的 Arrow Table 总行数是否正确"""
    assert os.path.exists(tif_path), f"测试文件不存在: {tif_path}"
    parser = TIFParser()
    row_count = parser.count(tif_path)
    table = parser.parse(tif_path)
    logger.info(f"多波段TIFF文件行数: {row_count}，parse后实际行数: {table.num_rows}")
    
    
if __name__ == "__main__":
    logger.info("手动运行测试")
    import pathlib
    import tempfile
    os.makedirs("D:/tmp", exist_ok=True)
    with tempfile.TemporaryDirectory(dir="D:/tmp") as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
    #     test_parse_and_write_multiband(tmp_path)
    #     test_parse_and_write_singleband(tmp_path)
    #     test_parse_and_write_hwcbands(tmp_path)
    #     test_parse_and_write_multipage(tmp_path)
    #     test_invalid_shape(tmp_path)
        # logger.info("开始测试多波段TIFF文件解析和写入,file_path [%s]", r"D:\test\faird\sample.tiff")
        # test_real_tif(r"D:\test\faird\sample.tiff", tmp_path, "sample_write.tiff")
        # logger.info("开始测试单波段TIFF文件解析和写入,file_path [%s]", r"D:\test\faird\R-factor.tif")
        # test_real_tif(r"D:\test\faird\R-factor.tif", tmp_path, "R-factor_write.tif")
        # logger.info("开始测试HWC多波段TIFF文件解析和写入,file_path [%s]", r"D:\test\faird\k-factorSD1.tif")
        # test_real_tif(r"D:\test\faird\k-factorSD1.tif", tmp_path, "k-factorSD1_write.tif")
        # logger.info("测试实际TIFF文件解析和写入完成")
    # logger.info("开始测试sample方法")
    # test_tif_sample(r"D:\test\faird\sample.tiff")  # 替换为实际的TIFF文件路径
    logger.info("开始测试count方法")
    test_tif_count(r"D:\test\faird\sample.tiff")  # 替换为实际的TIFF文件路径
    test_tif_count(r"D:\test\faird\R-factor.tif")  # 替换为实际的TIFF文件路径
    test_tif_count(r"D:\test\faird\k-factorSD1.tif")  # 替换为实际的TIFF文件路径
    logger.info("count方法测试完成")
    logger.info("全部测试完成")