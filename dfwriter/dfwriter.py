# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# import json
# import h5py
# from io import BytesIO
#
#
# class DfWriter:
#     def __init__(self):
#         self.output_target = None
#         self.format = "arrow"  # 默认格式
#         self.handlers = {
#             "csv": self._write_csv,
#             "json": self._write_json,
#             "hdf5": self._write_hdf5,
#             "arrow": self._write_arrow,
#         }
#
#     def output(self, target):
#         """
#         设置输出目标，可以是文件路径、文件流或字节流
#         """
#         self.output_target = target
#         return self
#
#     def format(self, fmt):
#         """
#         设置输出格式
#         """
#         self.format = fmt.lower()
#         return self
#
#     def write(self, df):
#         """
#         写入 DataFrame
#         """
#         if self.format not in self.handlers:
#             raise ValueError(f"Unsupported format: {self.format}")
#         handler = self.handlers[self.format]
#         handler(df)
#
#     def _write_csv(self, df):
#         if isinstance(self.output_target, str):
#             df.to_csv(self.output_target, index=False)
#         elif isinstance(self.output_target, BytesIO):
#             self.output_target.write(df.to_csv(index=False).encode("utf-8"))
#         else:
#             raise ValueError("Unsupported output target for CSV")
#
#     def _write_json(self, df):
#         if isinstance(self.output_target, str):
#             df.to_json(self.output_target, orient="records", lines=True)
#         elif isinstance(self.output_target, BytesIO):
#             self.output_target.write(df.to_json(orient="records", lines=True).encode("utf-8"))
#         else:
#             raise ValueError("Unsupported output target for JSON")
#
#     def _write_hdf5(self, df):
#         if isinstance(self.output_target, str):
#             with h5py.File(self.output_target, "w") as f:
#                 for col in df.columns:
#                     f.create_dataset(col, data=df[col].values)
#         elif isinstance(self.output_target, BytesIO):
#             with h5py.File(self.output_target, "w") as f:
#                 for col in df.columns:
#                     f.create_dataset(col, data=df[col].values)
#         else:
#             raise ValueError("Unsupported output target for HDF5")
#
#     def _write_arrow(self, df):
#         table = pa.Table.from_pandas(df)
#         if isinstance(self.output_target, str):
#             pq.write_table(table, self.output_target)
#         elif isinstance(self.output_target, BytesIO):
#             pq.write_table(table, self.output_target)
#         else:
#             raise ValueError("Unsupported output target for Arrow")
#
#
# # 示例用法
# if __name__ == "__main__":
#     # 创建示例 DataFrame
#     data = {"name": ["Alice", "Bob"], "age": [25, 30]}
#     df = pd.DataFrame(data)
#
#     # 输出到 CSV 文件
#     writer = DfWriter()
#     writer.output("output.csv").format("csv").write(df)
#
#     # 输出到 JSON 文件
#     writer.output("output.json").format("json").write(df)
#
#     # 输出到 HDF5 文件
#     writer.output("output.h5").format("hdf5").write(df)
#
#     # 输出到字节流（ARROW 格式）
#     buffer = BytesIO()
#     writer.output(buffer).format("arrow").write(df)
#     print("Arrow data written to buffer:", buffer.getvalue())