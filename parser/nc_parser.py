import os
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import xarray as xr
import dask
import netCDF4
import ast
import cftime
from parser.abstract_parser import BaseParser
from utils.logger_utils import get_logger
logger = get_logger(__name__)

def get_auto_chunk_size(var_shape, dtype=np.float64, target_mem_mb=10):
    dtype_size = np.dtype(dtype).itemsize
    if len(var_shape) == 0:
        return 1
    other_dim = int(np.prod(var_shape[1:])) if len(var_shape) > 1 else 1
    max_chunk = max(10, int((target_mem_mb * 1024 * 1024) // (other_dim * dtype_size)))
    return max_chunk

def is_large_variable(shape, dtype=np.float64, threshold_mb=50):
    dtype_size = np.dtype(dtype).itemsize
    size_mb = np.prod(shape) * dtype_size / 1024 / 1024
    return size_mb > threshold_mb

class NCParser(BaseParser):
    def parse(self, file_path: str) -> pa.Table:
        """
        用 xarray+dask 流式分块读取超大 NetCDF 文件，避免 OOM。
        并提取 dtype、压缩参数等元信息。
        兼容 _FillValue 和 missing_value 两种缺测值属性。
        保留原始缺测值（如 -9.96921e+36），不自动转为 np.nan。
        """
        DEFAULT_ARROW_CACHE_PATH = os.path.expanduser("~/.cache/faird/dataframe/csv/")
        # DEFAULT_ARROW_CACHE_PATH = os.path.join("D:/faird_cache/dataframe/nc/")
        os.makedirs(DEFAULT_ARROW_CACHE_PATH, exist_ok=True)
        arrow_file_name = os.path.basename(file_path).rsplit(".", 1)[0] + ".arrow"
        arrow_file_path = os.path.join(DEFAULT_ARROW_CACHE_PATH, arrow_file_name)

        file_size = os.path.getsize(file_path)
        logger.info(f"NetCDF 文件大小: {file_size} bytes")

        try:
            if os.path.exists(arrow_file_path):
                logger.info(f"检测到缓存文件，直接从 {arrow_file_path} 读取 Arrow Table。")
                with pa.memory_map(arrow_file_path, "r") as source:
                    return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取缓存 .arrow 文件失败: {e}")

        try:
            logger.info(f"开始用 xarray+dask 读取 NetCDF 文件: {file_path}")
            ds = xr.open_dataset(file_path, chunks={}, decode_cf=False)
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            shapes = [tuple(ds[v].shape) for v in var_names]
            dtypes = [str(ds[v].dtype) for v in var_names]
            var_attrs = {v: dict(ds[v].attrs) for v in var_names}
            def get_fill_value(attrs):
                for k in attrs:
                    if k.lower() in ['_fillvalue', 'missing_value']:
                        return attrs[k]
                return None
            fill_values = {v: get_fill_value(var_attrs[v]) for v in var_names}
            global_attrs = dict(ds.attrs)
            main_axes = [ds[v].dims[0] if ds[v].ndim > 0 else None for v in var_names]
            main_lens = [ds[v].shape[0] if ds[v].ndim > 0 else 1 for v in var_names]
            var_dims = {v: ds[v].dims for v in var_names}
            # 提取压缩参数
            var_compress = {}
            with netCDF4.Dataset(file_path) as nc:
                file_format = getattr(nc, 'file_format', 'unknown')
                for v in var_names:
                    var = nc.variables[v]
                    compress_info = {}
                    for attr in ['zlib', 'complevel', 'shuffle', 'chunksizes']:
                        if hasattr(var, attr):
                            compress_info[attr] = getattr(var, attr)
                    var_compress[v] = compress_info

            logger.info(f"变量列表: {var_names}")
            logger.info(f"变量 shapes: {shapes}")
            logger.info(f"变量 dtypes: {dtypes}")

            schema = pa.schema([pa.field(v, pa.from_numpy_dtype(ds[v].dtype)) for v in var_names])
            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str(global_attrs),
                "orig_lengths": str(main_lens),
                "var_dims": str(var_dims),
                "file_type": file_format,
                "var_compress": str(var_compress)
            }
            schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})

            large_vars = []
            small_vars = []
            for i, shape in enumerate(shapes):
                if is_large_variable(shape, dtype=np.float64, threshold_mb=50):
                    large_vars.append((i, var_names[i]))
                else:
                    small_vars.append((i, var_names[i]))

            max_chunks = []
            for i, shape in enumerate(shapes):
                chunk = get_auto_chunk_size(shape, dtype=np.float64, target_mem_mb=10)
                max_chunks.append(chunk)
            total_chunks = max([int(np.ceil(main_lens[i] / max_chunks[i])) for i in range(len(var_names))])

            logger.info(f"总分块数: {total_chunks}")

            with ipc.new_file(arrow_file_path, schema) as writer:
                for chunk_idx in range(total_chunks):
                    chunk_arrays = [None] * len(var_names)
                    chunk_lens = [0] * len(var_names)
                    logger.info(f"处理第 {chunk_idx+1}/{total_chunks} 块")
                    # 大变量串行
                    for i, v in large_vars:
                        safe_chunk_size = max_chunks[i]
                        main_dim = main_axes[i]
                        main_len = main_lens[i]
                        start = chunk_idx * safe_chunk_size
                        end = min(start + safe_chunk_size, main_len)
                        logger.debug(f"大变量 {v} 分块: start={start}, end={end}, shape={ds[v].shape}")
                        if start >= end:
                            arr_flat = np.array([], dtype=ds[v].dtype)
                        else:
                            darr = ds[v].isel({main_dim: slice(start, end)}).data
                            arr = darr.compute() if hasattr(darr, "compute") else np.array(darr)
                            arr_flat = np.array(arr).flatten()
                        chunk_arrays[i] = arr_flat
                        chunk_lens[i] = len(arr_flat)
                    # 小变量并行
                    batch_idxs = [i for i, _ in small_vars]
                    dask_chunks = []
                    safe_chunk_sizes = []
                    for i, v in small_vars:
                        safe_chunk_size = max_chunks[i]
                        main_dim = main_axes[i]
                        main_len = main_lens[i]
                        start = chunk_idx * safe_chunk_size
                        end = min(start + safe_chunk_size, main_len)
                        logger.debug(f"小变量 {v} 分块: start={start}, end={end}, shape={ds[v].shape}")
                        if start >= end:
                            dask_chunks.append(np.array([], dtype=ds[v].dtype))
                        else:
                            dask_chunks.append(ds[v].isel({main_dim: slice(start, end)}).data)
                        safe_chunk_sizes.append(safe_chunk_size)
                    computed_chunks = []
                    if dask_chunks:
                        try:
                            computed_chunks = dask.compute(*dask_chunks, scheduler="threads")
                        except Exception as e:
                            logger.warning(f"小变量并行失败，自动降级为串行: {e}")
                            computed_chunks = []
                            for chunk in dask_chunks:
                                computed_chunks.append(chunk.compute() if hasattr(chunk, "compute") else chunk)
                        for idx, arr in zip(batch_idxs, computed_chunks):
                            arr_flat = np.array(arr).flatten()
                            chunk_arrays[idx] = arr_flat
                            chunk_lens[idx] = len(arr_flat)
                    # 统一补齐到本次最大长度
                    max_len_this_chunk = max(chunk_lens) if chunk_lens else 0
                    for i, arr_flat in enumerate(chunk_arrays):
                        dtype = ds[var_names[i]].dtype
                        # cftime.datetime 类型兼容：转为自 1970-01-01 的天数
                        if arr_flat is not None and arr_flat.size > 0 and isinstance(arr_flat[0], cftime.datetime):
                            arr_flat = np.array([(x - cftime.DatetimeGregorian(1970, 1, 1)).days for x in arr_flat], dtype=np.float64)
                            chunk_arrays[i] = pa.array(arr_flat)
                            continue
                        if arr_flat is None:
                            padded = np.full(max_len_this_chunk, np.nan, dtype=dtype)
                            chunk_arrays[i] = pa.array(padded)
                        elif len(arr_flat) < max_len_this_chunk:
                            padded = np.full(max_len_this_chunk, np.nan, dtype=dtype)
                            padded[:len(arr_flat)] = arr_flat.astype(dtype)
                            chunk_arrays[i] = pa.array(padded)
                        else:
                            chunk_arrays[i] = pa.array(arr_flat.astype(dtype))
                    table = pa.table(chunk_arrays, names=var_names)
                    writer.write_table(table)
            ds.close()
            logger.info(f"Arrow Table 已写入缓存文件: {arrow_file_path}")
        except Exception as e:
            logger.error(f"解析 NetCDF 文件失败: {e}")
            raise

        try:
            logger.info(f"从 .arrow 文件 {arrow_file_path} 读取 Arrow Table。")
            with pa.memory_map(arrow_file_path, "r") as source:
                return ipc.open_file(source).read_all()
        except Exception as e:
            logger.error(f"读取 .arrow 文件失败: {e}")
            raise

    def write(self, table: pa.Table, output_path: str):
        """
        将 Arrow Table 写回 NetCDF 文件。大文件非常慢。
        支持变量属性、全局属性、缺测值、原始dtype和shape的还原。
        分批写入，避免内存溢出。自动处理 datetime64[ns] 类型为 float64。
        支持还原压缩参数，保证文件大小与原始一致。
        """
        import gc
        try:
            meta = table.schema.metadata or {}

            def _meta_eval(val, default):
                if isinstance(val, bytes):
                    return eval(val.decode())
                elif isinstance(val, str):
                    return eval(val)
                else:
                    return default

            def get_meta(meta, key, default):
                if key in meta:
                    return meta[key]
                if isinstance(key, str) and key.encode() in meta:
                    return meta[key.encode()]
                if isinstance(key, bytes) and key.decode() in meta:
                    return meta[key.decode()]
                return default

            shapes = _meta_eval(get_meta(meta, 'shapes', '[]'), [])
            dtypes = _meta_eval(get_meta(meta, 'dtypes', '[]'), [])
            var_names = _meta_eval(get_meta(meta, 'var_names', '[]'), [])
            var_attrs = _meta_eval(get_meta(meta, 'var_attrs', '{}'), {})
            fill_values = _meta_eval(get_meta(meta, 'fill_values', '{}'), {})
            global_attrs = _meta_eval(get_meta(meta, 'global_attrs', '{}'), {})
            orig_lengths = _meta_eval(get_meta(meta, 'orig_lengths', '[]'), [])
            var_dims = _meta_eval(get_meta(meta, 'var_dims', '{}'), {})
            var_compress = _meta_eval(get_meta(meta, 'var_compress', '{}'), {})

            logger.info(f"写入 NetCDF 文件: {output_path}")
            logger.info(f"变量名: {var_names}")

            if not (len(var_names) == len(shapes) == len(dtypes) == len(orig_lengths)):
                raise ValueError(
                    f"元数据长度不一致: var_names({len(var_names)}), shapes({len(shapes)}), dtypes({len(dtypes)}), orig_lengths({len(orig_lengths)})"
                )

            # 动态估算 batch_size
            def estimate_row_bytes(shapes, dtypes):
                total = 0
                for shape, dtype in zip(shapes, dtypes):
                    n = int(np.prod(shape[1:])) if len(shape) > 1 else 1
                    total += np.dtype(dtype).itemsize * n
                return total

            try:
                import psutil
                avail_mem = psutil.virtual_memory().available
                target_mem = avail_mem // 4
            except Exception:
                target_mem = 512 * 1024 * 1024  # 默认512MB

            row_bytes = estimate_row_bytes(shapes, dtypes)
            batch_size = max(1000, min(100000, target_mem // max(row_bytes, 1)))
            logger.info(f"动态设置 batch_size={batch_size}，单行约{row_bytes}字节，可用内存目标{target_mem}字节")

            with netCDF4.Dataset(output_path, 'w') as ds:
                # 1. 创建所有维度
                for i, name in enumerate(var_names):
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shapes[i]))])
                    shape = shapes[i]
                    for dim_name, dim_len in zip(dims, shape):
                        if dim_name not in ds.dimensions:
                            ds.createDimension(dim_name, dim_len)
                # 2. 创建所有变量
                nc_vars = []
                dtype_map = []
                for i, name in enumerate(var_names):
                    shape = shapes[i]
                    dtype = dtypes[i]
                    fill_value = fill_values.get(name, None)
                    dims = var_dims.get(name, [f"{name}_dim{j}" for j in range(len(shape))])
                    np_dtype = np.dtype(dtype)
                    attrs = var_attrs.get(name, {})
                    # 处理 datetime64[ns] 类型
                    if np.issubdtype(np_dtype, np.datetime64):
                        logger.warning(f"{name}: datetime64[ns] 不被 netCDF4 支持，自动转为 float64（单位：天）")
                        np_dtype = np.float64
                        attrs['units'] = attrs.get('units', 'days since 1970-01-01')
                    dtype_map.append(np_dtype)
                    # 还原压缩参数
                    compress_info = var_compress.get(name, {})
                    create_kwargs = {}
                    for k in ['zlib', 'complevel', 'shuffle', 'chunksizes']:
                        if k in compress_info and compress_info[k] is not None:
                            create_kwargs[k] = compress_info[k]
                    if fill_value is not None:
                        var = ds.createVariable(name, np_dtype, dims, fill_value=fill_value, **create_kwargs)
                    else:
                        var = ds.createVariable(name, np_dtype, dims, **create_kwargs)
                    nc_vars.append(var)
                    var_attrs[name] = attrs  # 更新属性，后续写属性用
                # 3. 分批写入数据
                write_offsets = [0 for _ in var_names]
                batch_count = 0 # 记录批次数
                logger.info(f"开始分批写入数据")
                for batch_idx, batch in enumerate(table.to_batches(batch_size)):
                    logger.info(f"处理第 {batch_idx + 1} 批数据")
                    batch_count += 1
                    for i, arr in enumerate(batch.columns):
                        arr_np = arr.to_numpy(zero_copy_only=False)
                        orig_length = orig_lengths[i]
                        remain = orig_length - write_offsets[i]
                        shape_i = shapes[i]
                        np_dtype = dtype_map[i]
                        other_dim = int(np.prod(shape_i[1:])) if len(shape_i) > 1 else 1
                        max_write_rows = len(arr_np) // other_dim
                        write_len = min(remain, max_write_rows)
                        if write_len <= 0:
                            continue
                        arr_write = arr_np[:write_len * other_dim]
                        # 类型转换和特殊处理
                        if np.issubdtype(np_dtype, np.datetime64):
                            arr_write = arr_write.astype('datetime64[D]').astype('float64')
                        # 兼容 parse/sample 阶段已转 float 的 cftime.datetime
                        # 如果 units 属性是 days since 1970-01-01，直接写 float64
                        if np.issubdtype(np_dtype, np.integer) and fill_values.get(var_names[i], None) is not None:
                            arr_write = np.where(np.isnan(arr_write), fill_values[var_names[i]], arr_write)
                            arr_write = arr_write.astype(np_dtype)
                        else:
                            arr_write = arr_write.astype(np_dtype)
                        arr_write = arr_write.reshape((write_len,) + tuple(shape_i[1:]))
                        nc_vars[i][write_offsets[i]:write_offsets[i]+write_len, ...] = arr_write
                        write_offsets[i] += write_len
                        del arr_write  # 只在定义后删除
                    del batch, arr_np
                    gc.collect()
                logger.info(f"共写入 {batch_count} 批数据")
                # 4. 写变量属性
                for i, name in enumerate(var_names):
                    attrs = var_attrs.get(name, {})
                    for k, v in attrs.items():
                        if k == "_FillValue":
                            continue
                        try:
                            nc_vars[i].setncattr(k, v)
                        except Exception:
                            logger.warning(f"变量 {name} 属性 {k}={v} 写入失败")
                # 5. 写全局属性
                for k, v in global_attrs.items():
                    try:
                        ds.setncattr(k, v)
                    except Exception:
                        logger.warning(f"全局属性 {k}={v} 写入失败")
            logger.info(f"写入 NetCDF 文件到 {output_path}")
        except Exception as e:
            logger.error(f"写入 NetCDF 文件失败: {e}")
            raise

    def sample(self, file_path: str) -> pa.Table:
        """
        从 NetCDF 文件中采样数据，返回 Arrow Table。
        默认每个变量只读取前10个主轴切片（如 time 维度的前10个）。
        更快的 NetCDF 采样方法：边采样边补齐，避免多余拷贝和类型推断。
        并为 schema 添加 metadata。
        兼容 _FillValue 和 missing_value 两种缺测值属性。
        保留原始缺测值（如 -9.96921e+36），不自动转为 np.nan。

        """
        try:
            # if file_path.startswith("/"):
            #     file_path = file_path[1:]
            ds = xr.open_dataset(file_path, decode_cf=False)
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            arrays = []
            field_types = []
            var_attrs = {v: dict(ds[v].attrs) for v in var_names}
            def get_fill_value(attrs):
                for k in attrs:
                    if k.lower() in ['_fillvalue', 'missing_value']:
                        return attrs[k]
                return None
            fill_values = {v: get_fill_value(var_attrs[v]) for v in var_names}
            max_len = 20
            for idx, v in enumerate(var_names):
                var = ds[v]
                if var.shape[0] > 10:
                    arr = var.isel({var.dims[0]: slice(0, 10)}).values
                else:
                    arr = var.values
                arr_flat = arr.flatten() if isinstance(arr, np.ndarray) else np.array(arr).flatten()
                # 类型推断和补齐
                if arr_flat.size > 0 and isinstance(arr_flat[0], cftime.datetime):
                    arr_flat = np.array([(x - cftime.DatetimeGregorian(1970, 1, 1)).days for x in arr_flat], dtype=np.float64)
                    typ = pa.float64()
                elif arr_flat.size > 0 and (isinstance(arr_flat[0], (bytes, np.bytes_, str))):
                    arr_flat = np.array([x.decode() if isinstance(x, (bytes, np.bytes_)) else str(x) for x in arr_flat], dtype=object)
                    typ = pa.string()
                else:
                    typ = pa.float64()
                field_types.append(typ)
                if typ == pa.string():
                    if len(arr_flat) >= max_len:
                        arrays.append(pa.array(arr_flat[:max_len], type=pa.string()))
                    else:
                        padded = np.full(max_len, "", dtype=object)
                        padded[:len(arr_flat)] = arr_flat
                        arrays.append(pa.array(padded, type=pa.string()))
                else:
                    fill_value = fill_values.get(v, np.nan)
                    if fill_value is None:
                        fill_value = np.nan
                    if len(arr_flat) >= max_len:
                        arrays.append(pa.array(arr_flat[:max_len].astype(np.float64), type=pa.float64()))
                    else:
                        padded = np.full(max_len, fill_value, dtype=np.float64)
                        padded[:len(arr_flat)] = arr_flat.astype(np.float64)
                        arrays.append(pa.array(padded, type=pa.float64()))
            schema = pa.schema([pa.field(v, t) for v, t in zip(var_names, field_types)])
            shapes = [tuple(ds[v].shape) for v in var_names]
            dtypes = [str(ds[v].dtype) for v in var_names]
            global_attrs = dict(ds.attrs)
            orig_lengths = [int(np.prod(ds[v].shape)) for v in var_names]
            var_dims = {v: ds[v].dims for v in var_names}
            meta = {
                "shapes": str(shapes),
                "dtypes": str(dtypes),
                "var_names": str(var_names),
                "var_attrs": str(var_attrs),
                "fill_values": str(fill_values),
                "global_attrs": str(global_attrs),
                "orig_lengths": str(orig_lengths),
                "var_dims": str(var_dims),
                "sample": "True"
            }
            schema = schema.with_metadata({k: str(v).encode() for k, v in meta.items()})
            table = pa.table(arrays, schema=schema)
            ds.close()
            return table
        except Exception as e:
            logger.error(f"采样 NetCDF 文件失败: {e}")
            raise

    def meta_to_json(self, meta: dict):
        """
        将 sample 方法生成的 meta 字典转为适合前端展示的 JSON 格式（变量为列，属性为行）。
        用法示例
        meta = {k.decode(): v.decode() for k, v in table.schema.metadata.items()}
        json_data = meta_to_json(meta)
        print(json_data)
        """
        def safe_eval(val, default):
            try:
                return ast.literal_eval(val)
            except Exception:
                return default

        shapes = safe_eval(meta.get('shapes', '[]'), [])
        dtypes = safe_eval(meta.get('dtypes', '[]'), [])
        var_names = safe_eval(meta.get('var_names', '[]'), [])
        var_attrs = safe_eval(meta.get('var_attrs', '{}'), {})
        fill_values = safe_eval(meta.get('fill_values', '{}'), {})
        var_dims = safe_eval(meta.get('var_dims', '{}'), {})
        orig_lengths = safe_eval(meta.get('orig_lengths', '[]'), [])
        global_attrs = safe_eval(meta.get('global_attrs', '{}'), {})

        # 组织每个变量的属性
        data = {}
        for i, v in enumerate(var_names):
            data[v] = {
                "shape": shapes[i] if i < len(shapes) else "",
                "dtype": dtypes[i] if i < len(dtypes) else "",
                "var_attrs": var_attrs.get(v, {}),
                "fill_value": fill_values.get(v, ""),
                "var_dims": var_dims.get(v, ""),
                "orig_length": orig_lengths[i] if i < len(orig_lengths) else ""
            }
        # 增加全局属性一列
        data["global_attrs"] = {
            "shape": "",
            "dtype": "",
            "var_attrs": "",
            "fill_value": "",
            "var_dims": "",
            "orig_length": "",
            "global_attrs": global_attrs
        }

        # 行顺序
        row_order = ["shape", "dtype", "var_attrs", "fill_value", "var_dims", "orig_length", "global_attrs"]

        # 转为前端友好的json
        result = {
            "columns": list(data.keys()),
            "rows": [
                {
                    "attribute": row,
                    **{col: data[col].get(row, "") for col in data}
                }
                for row in row_order
            ]
        }
        return result
    
    def count(self, file_path: str) -> int:
        """
        返回解析后 Arrow Table 的总行数（即主变量 shape 的元素数最大值）。
        新增：记录 Arrow Table 总行数，parse方法生成的Arrow Table总行数 = shape中最高维变量的所有元素数量（即所有主变量shape的乘积）！
        原理：parse方法通常会将所有主变量（shape大于1的变量）拉平成一维，每一行对应原始nc文件中所有主变量的一个元素（即所有维度的组合），而不是只按第一个维度分块。
        arrow_table_rows = max(np.prod(shape) for shape in shapes)
        """
        try:
            ds = xr.open_dataset(file_path, decode_cf=False)
            var_names = [v for v in ds.variables if ds[v].ndim > 0]
            shapes = [tuple(ds[v].shape) for v in var_names]
            if not shapes:
                return 0
            # Arrow Table 行数 = 所有主变量 shape 的元素总数的最大值
            total_rows = max(np.prod(shape) for shape in shapes)
            ds.close()
            return int(total_rows)
        except Exception as e:
            logger.error(f"统计 NetCDF 文件 Arrow Table 行数失败: {e}")
            raise