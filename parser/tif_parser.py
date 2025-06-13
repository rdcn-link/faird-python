import os
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import tifffile
from parser.abstract_parser import BaseParser
from utils.logger_utils import get_logger
logger = get_logger(__name__)

class TIFParser(BaseParser):
    def parse(self, file_path: str) -> pa.Table:
        """
        读取 TIFF 文件为 Arrow Table，保留原始 dtype、shape、波段信息。
        支持多页、多波段。先写入.arrow缓存，再读取返回。
        """
        try:
            DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/tif/")
            # DEFAULT_ARROW_CACHE_PATH = os.path.join("D:/faird_cache/dataframe/tif/")
            os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
            arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
            arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)
            if os.path.exists(arrow_file_path):
                logger.info(f"检测到缓存文件，直接从 {arrow_file_path} 读取 Arrow Table。")
                try:
                    with pa.memory_map(arrow_file_path, "r") as source:
                        return ipc.open_file(source).read_all()
                except Exception as e:
                    logger.warning(f"读取缓存 .arrow 文件失败，将重新解析TIFF: {e}")

            logger.info(f"开始解析 TIFF 文件: {file_path}")
            try:
                images = tifffile.imread(file_path)
            except Exception as e:
                logger.error(f"TIFF 文件读取失败: {e}")
                raise
            logger.info(f"TIFF 文件 shape: {images.shape}, dtype: {images.dtype}")
            # 支持多页
            if images.ndim == 2:
                images = [images]
            elif images.ndim == 3:
                # (pages, H, W) 或 (H, W, bands)
                if images.shape[0] in [1, 3, 4] and images.shape[0] < images.shape[1] and images.shape[0] < images.shape[2]:
                    images = [images]
                elif images.shape[2] in [1, 3, 4] and images.shape[2] < images.shape[0] and images.shape[2] < images.shape[1]:
                    images = [images]
                else:
                    # 多页
                    images = [img for img in images]
            elif images.ndim == 4:
                # (pages, bands, H, W) 或 (pages, H, W, bands)
                images = [img for img in images]
            else:
                images = [images]

            pa_arrays_raw = []
            band_names = []
            orig_lengths = []
            pa_types = []
            shapes = []
            dtypes = []

            for idx, img in enumerate(images):
                try:
                    dtype = img.dtype
                    shape = img.shape
                    if img.ndim == 2:
                        arr = img.flatten()
                        pa_arrays_raw.append(pa.array(arr, type=pa.from_numpy_dtype(dtype)))
                        pa_types.append(pa.from_numpy_dtype(dtype))
                        band_names.append(f'page{idx+1}_band1')
                        orig_lengths.append(arr.size)
                        shapes.append(shape)
                        dtypes.append(str(dtype))
                    elif img.ndim == 3:
                        # (B, H, W)
                        if img.shape[0] in [1, 3, 4] and img.shape[0] < img.shape[1] and img.shape[0] < img.shape[2]:
                            for b in range(img.shape[0]):
                                arr = img[b, :, :].flatten()
                                pa_arrays_raw.append(pa.array(arr, type=pa.from_numpy_dtype(dtype)))
                                pa_types.append(pa.from_numpy_dtype(dtype))
                                band_names.append(f'page{idx+1}_band{b+1}')
                                orig_lengths.append(arr.size)
                                shapes.append((1, img.shape[1], img.shape[2]))
                                dtypes.append(str(dtype))
                        # (H, W, B)
                        elif img.shape[2] in [1, 3, 4] and img.shape[2] < img.shape[0] and img.shape[2] < img.shape[1]:
                            for b in range(img.shape[2]):
                                arr = img[:, :, b].flatten()
                                pa_arrays_raw.append(pa.array(arr, type=pa.from_numpy_dtype(dtype)))
                                pa_types.append(pa.from_numpy_dtype(dtype))
                                band_names.append(f'page{idx+1}_band{b+1}')
                                orig_lengths.append(arr.size)
                                shapes.append((img.shape[0], img.shape[1], 1))
                                dtypes.append(str(dtype))
                        else:
                            arr = img.flatten()
                            pa_arrays_raw.append(pa.array(arr, type=pa.from_numpy_dtype(dtype)))
                            pa_types.append(pa.from_numpy_dtype(dtype))
                            band_names.append(f'page{idx+1}_flatten')
                            orig_lengths.append(arr.size)
                            shapes.append(shape)
                            dtypes.append(str(dtype))
                    else:
                        arr = img.flatten()
                        pa_arrays_raw.append(pa.array(arr, type=pa.from_numpy_dtype(dtype)))
                        pa_types.append(pa.from_numpy_dtype(dtype))
                        band_names.append(f'page{idx+1}_flatten')
                        orig_lengths.append(arr.size)
                        shapes.append(shape)
                        dtypes.append(str(dtype))
                except Exception as e:
                    logger.error(f"处理第{idx+1}页/波段时异常: {e}")
                    raise

            try:
                max_len = max(len(arr) for arr in pa_arrays_raw)
            except Exception as e:
                logger.error(f"计算最大长度异常: {e}")
                raise

            pa_arrays = []
            for arr, typ in zip(pa_arrays_raw, pa_types):
                try:
                    if len(arr) < max_len:
                        if pa.types.is_floating(typ):
                            padded = np.full(max_len, np.nan, dtype=typ.to_pandas_dtype())
                        else:
                            padded = np.zeros(max_len, dtype=typ.to_pandas_dtype())
                        padded[:len(arr)] = arr.to_numpy()
                        pa_arrays.append(pa.array(padded, type=typ))
                    else:
                        pa_arrays.append(arr)
                except Exception as e:
                    logger.error(f"补齐列时异常: {e}")
                    raise

            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "orig_lengths": str(orig_lengths),
                "band_names": str(band_names)
            }
            try:
                schema = pa.schema([pa.field(n, t) for n, t in zip(band_names, pa_types)]).with_metadata(
                    {k: str(v).encode() for k, v in meta.items()}
                )
                table = pa.table(pa_arrays, schema=schema)
                logger.info(f"TIFF 解析完成，列数: {len(table.column_names)}，每列长度: {max_len}，写入缓存 {arrow_file_path}")
                with ipc.new_file(arrow_file_path, schema) as writer:
                    writer.write_table(table)
                # 再从.arrow读取返回
                with pa.memory_map(arrow_file_path, "r") as source:
                    return ipc.open_file(source).read_all()
            except Exception as e:
                logger.error(f"写入或读取 Arrow 缓存异常: {e}")
                raise
        except Exception as e:
            logger.error(f"TIFF 解析失败: {e}")
            raise

    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写入 TIFF 文件。
        支持多页、多波段、多shape的还原（需依赖metadata）。
        写回时自动去除NaN补齐部分，只用有效数据还原 shape。
        """
        try:
            logger.info(f"开始写入 TIFF 文件: {output_path}")
            meta = table.schema.metadata or {}
            # 还原shape、dtype、原始长度
            try:
                shapes = eval(meta.get(b'shapes', b'[]').decode() if isinstance(meta.get(b'shapes', b''), bytes) else meta.get('shapes', '[]'))
                dtypes = eval(meta.get(b'dtypes', b'[]').decode() if isinstance(meta.get(b'dtypes', b''), bytes) else meta.get('dtypes', '[]'))
                orig_lengths = eval(meta.get(b'orig_lengths', b'[]').decode() if isinstance(meta.get(b'orig_lengths', b''), bytes) else meta.get('orig_lengths', '[]'))
                band_names = eval(meta.get(b'band_names', b'[]').decode() if isinstance(meta.get(b'band_names', b''), bytes) else meta.get('band_names', '[]'))
            except Exception as e:
                logger.error(f"元数据解析异常: {e}")
                raise
            try:
                arrays = [col.to_numpy() for col in table.columns]
            except Exception as e:
                logger.error(f"Arrow Table 转 numpy 异常: {e}")
                raise
            images = []
            arr_idx = 0
            i = 0
            try:
                while i < len(shapes):
                    shape = shapes[i]
                    dtype = np.dtype(dtypes[i])
                    if len(shape) == 2:
                        valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                        img = valid.reshape(shape).astype(dtype)
                        images.append(img)
                        arr_idx += 1
                        i += 1
                    elif len(shape) == 3:
                        # 判断是 (B, H, W) 还是 (H, W, B)
                        if shape[0] in [1, 3, 4] and shape[0] < shape[1] and shape[0] < shape[2]:
                            bands = shape[0]
                            band_imgs = []
                            for b in range(bands):
                                valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                                band_imgs.append(valid.reshape((shape[1], shape[2])).astype(dtype))
                                arr_idx += 1
                                i += 1
                            img = np.stack(band_imgs, axis=0)
                            images.append(img)
                        elif shape[2] in [1, 3, 4] and shape[2] < shape[0] and shape[2] < shape[1]:
                            bands = shape[2]
                            band_imgs = []
                            for b in range(bands):
                                valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                                band_imgs.append(valid.reshape((shape[0], shape[1])).astype(dtype))
                                arr_idx += 1
                                i += 1
                            img = np.stack(band_imgs, axis=-1)
                            images.append(img)
                        else:
                            valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                            img = valid.reshape(shape).astype(dtype)
                            images.append(img)
                            arr_idx += 1
                            i += 1
                    else:
                        valid = arrays[arr_idx][:orig_lengths[arr_idx]]
                        img = valid.reshape(shape).astype(dtype)
                        images.append(img)
                        arr_idx += 1
                        i += 1
            except Exception as e:
                logger.error(f"TIFF 还原 numpy 数据异常: {e}")
                raise
            try:
                tifffile.imwrite(output_path, images if len(images) > 1 else images[0])
                logger.info(f"写入 TIFF 文件到 {output_path}，共 {len(images)} 页")
            except Exception as e:
                logger.error(f"写入 TIFF 文件异常: {e}")
                raise
        except Exception as e:
            logger.error(f"写入 TIFF 文件失败: {e}")
            raise

    def sample(self, file_path):
        """
        从 TIFF 文件中采样数据，返回 Arrow Table。
        读取第一页（或第一波段）前max_rows个像素，自动补齐为相同长度，并添加schema的metadata。
        max_rows: 生成的Arrow Table的行数，默认20（写在方法内部）
        """
        try:
            max_rows = 20
            logger.info(f"开始采样 TIFF 文件: {file_path}，采样行数: {max_rows}")
            try:
                with tifffile.TiffFile(file_path) as tif:
                    if len(tif.pages) == 0:
                        logger.error("TIFF 文件无有效页")
                        raise ValueError("TIFF 文件无有效页")
                    img = tif.pages[0].asarray()
            except Exception as e:
                logger.error(f"TIFF 采样读取第一页异常: {e}")
                raise
            logger.info(f"第一页 shape: {img.shape}, dtype: {img.dtype}")
            arrays = []
            names = []
            shapes = []
            dtypes = []
            orig_lengths = []
            try:
                if img.ndim == 2:
                    arr = img.flatten().astype(np.float64)
                    arrays.append(arr[:max_rows])
                    names.append('page1_band1')
                    shapes.append(img.shape)
                    dtypes.append(str(img.dtype))
                    orig_lengths.append(min(max_rows, arr.size))
                    logger.info("采样二维影像，波段数: 1")
                elif img.ndim == 3:
                    # (B, H, W)
                    if img.shape[0] in [1, 3, 4] and img.shape[0] < img.shape[1] and img.shape[0] < img.shape[2]:
                        logger.info(f"采样三维影像，按(B, H, W)模式，波段数: {img.shape[0]}")
                        for b in range(img.shape[0]):
                            arr = img[b, :, :].flatten().astype(np.float64)
                            arrays.append(arr[:max_rows])
                            names.append(f'page1_band{b+1}')
                            shapes.append(img[b, :, :].shape)
                            dtypes.append(str(img.dtype))
                            orig_lengths.append(min(max_rows, arr.size))
                    # (H, W, B)
                    elif img.shape[2] in [1, 3, 4] and img.shape[2] < img.shape[0] and img.shape[2] < img.shape[1]:
                        logger.info(f"采样三维影像，按(H, W, B)模式，波段数: {img.shape[2]}")
                        for b in range(img.shape[2]):
                            arr = img[:, :, b].flatten().astype(np.float64)
                            arrays.append(arr[:max_rows])
                            names.append(f'page1_band{b+1}')
                            shapes.append(img[:, :, b].shape)
                            dtypes.append(str(img.dtype))
                            orig_lengths.append(min(max_rows, arr.size))
                    else:
                        logger.info("采样三维影像，未知排列，直接flatten")
                        arr = img.flatten().astype(np.float64)
                        arrays.append(arr[:max_rows])
                        names.append('page1_flatten')
                        shapes.append(img.shape)
                        dtypes.append(str(img.dtype))
                        orig_lengths.append(min(max_rows, arr.size))
                else:
                    logger.info("采样高维影像，直接flatten")
                    arr = img.flatten().astype(np.float64)
                    arrays.append(arr[:max_rows])
                    names.append('page1_flatten')
                    shapes.append(img.shape)
                    dtypes.append(str(img.dtype))
                    orig_lengths.append(min(max_rows, arr.size))
            except Exception as e:
                logger.error(f"采样数据处理异常: {e}")
                raise
            # 补齐
            pa_arrays = []
            try:
                for arr in arrays:
                    if len(arr) < max_rows:
                        padded = np.full(max_rows, np.nan, dtype=np.float64)
                        padded[:len(arr)] = arr
                        pa_arrays.append(pa.array(padded))
                    else:
                        pa_arrays.append(pa.array(arr))
            except Exception as e:
                logger.error(f"采样数据补齐异常: {e}")
                raise
            # 构造schema并添加metadata
            try:
                schema = pa.schema([pa.field(n, pa.float64()) for n in names])
                meta = {
                    "shapes": str(shapes),
                    "dtypes": str(dtypes),
                    "orig_lengths": str(orig_lengths),
                    "file_type": "TIFF",
                    "sample": "True"
                }
                schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})
                table = pa.table(pa_arrays, schema=schema)
                logger.info(f"采样完成，生成 Arrow Table，列: {names}，每列采样长度: {max_rows}，orig_lengths: {orig_lengths}")
                return table
            except Exception as e:
                logger.error(f"采样 Arrow Table 构造异常: {e}")
                raise
        except Exception as e:
            logger.error(f"采样 TIFF 文件失败: {e}")
            raise

    def count(self, file_path: str) -> int:
        """
        返回解析后 Arrow Table 的总行数（即所有页/波段像素数的最大值）。
        """
        try:
            images = tifffile.imread(file_path)
            # 统一成列表
            if images.ndim == 2:
                images = [images]
            elif images.ndim == 3:
                if images.shape[0] in [1, 3, 4] and images.shape[0] < images.shape[1] and images.shape[0] < images.shape[2]:
                    images = [images]
                elif images.shape[2] in [1, 3, 4] and images.shape[2] < images.shape[0] and images.shape[2] < images.shape[1]:
                    images = [images]
                else:
                    images = [img for img in images]
            elif images.ndim == 4:
                images = [img for img in images]
            else:
                images = [images]
            # 统计每个波段/页的像素数
            max_len = max(img.size for img in images)
            return int(max_len)
        except Exception as e:
            logger.error(f"统计 TIFF 文件 Arrow Table 行数失败: {e}")
            raise
        
    # def meta_to_json(self, meta: dict):
    #     """
    #     只返回 shape 和 dtype，变量为列，属性为行，适合前端表格展示。波段很少的很难看，只有一列，暂时不提供吧
    #     """
    #     import ast
    #     def safe_eval(val, default):
    #         try:
    #             return ast.literal_eval(val)
    #         except Exception:
    #             return default

    #     shapes = safe_eval(meta.get('shapes', '[]'), [])
    #     dtypes = safe_eval(meta.get('dtypes', '[]'), [])
    #     band_names = safe_eval(meta.get('band_names', '[]'), [])
    #     # 如果没有band_names就用 band1, band2...
    #     if not band_names:
    #         band_names = [f"band{i+1}" for i in range(len(shapes))]

    #     data = {}
    #     for i, v in enumerate(band_names):
    #         data[v] = {
    #             "shape": shapes[i] if i < len(shapes) else "",
    #             "dtype": dtypes[i] if i < len(dtypes) else ""
    #         }

    #     row_order = ["shape", "dtype"]

    #     result = {
    #         "columns": list(data.keys()),
    #         "rows": [
    #             {
    #                 "attribute": row,
    #                 **{col: data[col].get(row, "") for col in data}
    #             }
    #             for row in row_order
    #         ]
    #     }
    #     return result