# sdk使用方式

## 1. 安装
版本管理：https://pypi.org/project/faird/#history
```bash
pip install faird
```

## 2. 使用
### 2.1. 引入依赖
```python
from sdk import DataFrame, DacpClient, Principal
```
### 2.2. 连接faird服务
```python
url = "dacp://60.245.194.25:50201"
username = "faird-user1"
password = "user1@cnic.cn"

# 方式一：用户名/密码
conn = DacpClient.connect(url, Principal.oauth("conet", username=username, password=password))

# 方式二：匿名
conn = DacpClient.connect(url)
conn = DacpClient.connect(url, Principal.ANONYMOUS)

# 方式三：controld连接
conn = DacpClient.connect(url, Principal.oauth("controld", controld_domain_name="controld_domain_name", signature="signature"))
```

### 2.3. 获取数据集列表
```python
datasets = conn.list_datasets()
```

### 2.4. 获取数据集元信息
```python
metadata = conn.get_dataset(datasets[0])
```

### 2.5. 获取数据集下的数据帧列表
```python
# 方式一：全量返回
dataframes = conn.list_dataframes(datasets[1])

# 方式二：流式返回
for chunk in conn.list_dataframes_stream(datasets[1]):
  print(f"Chunk size: {len(chunk)}")
```

### 2.6. 获取数据帧的数据流
- 方式一：get_dataframe_stream
```python
total_size = 0
for chunk in conn.get_dataframe_stream(dataframe_name, max_chunksize=1024*1024*5):
    total_size += len(chunk)
print(f"total size: {total_size} Bytes")
```
- 方式二：collect_blob
```python
dataframe_name = dataframes[0]['dataframeName'] # 假设这个为dir类型
df = conn.open(dataframe_name) # 此时blob列的值都为None

# 加载全部文件的blob数据
df.collect_blob()
```
- 方式三：下标
```python
# 获取第一行 blob 列的流式数据
blob_reader = df[0]["blob"]  

# 流式读取 blob 数据
for blob in blob_reader:
    print(f"blob size: {len(blob)} bytes")
```

### 2.7. 获取数据帧的数据样例
```python
sample = conn.sample(dataframe_name)
```

### 2.8. 打开dataframe
```python
df = conn.open(dataframe_name)
```

## 3. DataFrame API
### 3.1. 数据结构信息
查看 dataframe 表结构、行数、数据大小等基本信息，不需要实际加载数据。
```python
logger.info(f"表结构: {df.schema} \n")
logger.info(f"表大小: {df.shape} \n")
logger.info(f"行数: {df.num_rows} \n")  # 或者len(dataframe)
logger.info(f"列数: {df.num_cols} \n")
logger.info(f"列名: {df.column_names} \n")
logger.info(f"数据大小: {df.total_bytes} \n")
```

### 3.2 数据预览与打印
预览（部分或全部）数据、打印数据。 
> 注：这一步并不会将全部数据加载到本地，仅返回需要展示的数据）
- __*to_string(head_rows: int = 5, tail_rows: int = 5, first_cols: int = 3, last_cols: int = 3, display_all: bool = False) -> str*__: 
  - `head_rows` `tail_rows` `first_cols` `last_cols`: 选择查看的行数和列数，默认前5行和后5行、前3列和后3列。
  - `display_all`: 是否查看所有行和列，默认False
- __*logger.info(df)*__: 默认打印前5行和后5行、前3列和后3列。

```python
data_str = df.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False)
logger.info(f"打印dataframe：\n {data_str}\n") # 或者直接用logger.info(df)
```

### 3.3 数据流式读取
- __*get_stream()*__：流式读取数据，每次返回一个数据分块。
  - `max_chunksize`：每次读取的行数，默认 max_chunksize = 1000。
```python
# 默认1000行
for chunk in df.get_stream(): 
    logger.info(chunk)
    logger.info(f"Chunk size: {chunk.num_rows}")

# 设置每次读取100行
for chunk in df.get_stream(max_chunksize=100):
    logger.info(chunk)
    logger.info(f"Chunk size: {chunk.num_rows}")
```

### 3.4 加载数据到本地
- __*collect() -> DataFrame*__: 将全部数据加载到本地，后续的所有计算将在本地进行。
> 注：连续操作时，collect()调用位置将影响计算方式。
```python
logger.info(f"=== 01.limit, select 在本地计算: === \n {df.collect().limit(3).select("col1")} \n")
logger.info(f"=== 02.limit, select 在远程计算，仅将处理结果加载到本地: === \n {df.limit(3).select("col1").collect()} \n")
logger.info(f"=== 03.limit 在远程计算，select 在本地计算: === \n {df.limit(3).collect().select("col1")} \n")
```

### 3.5 数据选择与过滤
#### 3.5.1 列过滤
- __*df['{column_name}'] -> list*__ : 选择某一列，返回该列类型的值列表。
- __*select(\*columns: str) -> DataFrame*__：选择指定的一列或多列，返回一个新的 DataFrame 对象。

```python
logger.info(f"打印指定列的值: \n {df["col1"]} \n")
logger.info(f"筛选某几列: \n {df.select("col1", "col2", "col3")} \n")
```

#### 3.5.2 行过滤
- __*df[{row_index}] -> dict*__: 选择某一行，返回dict类型，key为列名，value为该行对应列的值。
- __*df[{row_index}]['{column_name}']*__: 选择某一行的某一列，返回该列类型的值。
- __*limit(rowNum: int) -> DataFrame*__: 限制返回的行数，返回一个新的 DataFrame 对象。
- __*slice(offset: int = 0, length: Optional[int] = None) -> DataFrame*__: 进行行切片操作，返回一个新的 DataFrame 对象。

```python
logger.info(f"打印第0行的值: \n {df[0]} \n")
logger.info(f"打印第0行、指定列的值: \n {df[0]["col1"]} \n")
logger.info(f"筛选前10行: \n {df.limit(10)} \n")
logger.info(f"筛选第2-4行: \n {df.slice(2, 4)} \n")
```
#### 3.5.3 条件筛选
- __*filter(expression: str) -> DataFrame*__: 选择符合条件的行，返回一个新的 DataFrame 对象。

```python
# 示例 1: 筛选某列值小于等于 30 的行
expression = "col1 <= 30"

# 示例 2: 筛选某列值等于特定字符串的行
expression = "col2 == 'example'"

# 示例 3: 筛选多列满足条件的行
expression = "(col1 > 10) & (col3 < 50)"

# 示例 4: 筛选某列值在特定列表中的行
expression = "col4.isin([1, 2, 3])"

# 示例 5: 筛选某列值不为空的行
expression = "col5.notnull()"

# 示例 6: 复杂条件组合
expression = "((col1 < 10) | (col2 == 'example')) & (col3 != 0)"

logger.info(f"条件筛选后的结果: \n {df.filter(expression)} \n")
```

### 3.6 数据聚合与统计
- **聚合操作**: `sum`, `mean`, `min`, `max`, `count`, `std`, `var`
- **分组聚合**: `groupby`, `aggregate`
- **唯一值**: `unique`, `nunique`

### 3.7 数据排序

### 3.8 数据变换与计算
- **列运算**: `apply`, `map`, `transform`
- **新增列**: `assign`, `append_column`
- **删除列**: `drop`
- **数据类型转换**: `astype`

### 3.9 数据合并与连接
- **表连接**: `merge`, `join`
- **表拼接**: `concat`, `append`

### 3.10 数据格式转换与导出
- **转换**: `to_nc`, `to_pandas`
- **导出**: `write`

### 3.11 高级操作
- **SQL 查询**: `sql`
- **重塑数据**: `pivot`, `melt`
- **缺失值处理**: `fillna`, `dropna`
