# How to use the Local SDK

## 0. Configure environment variables
### 0.1 Configure environment variables
```bash
export FAIRD_HOME=/path/to/your/faird
```
### 0.1 faird.conf
The `faird.conf` file is located in the `$FAIRD_HOME` directory, and its contents are as follows:
```text
[storage]
storage.type=local
storage.local.path=/path/to/storage

[ftp]
storage.ftp.url=ftp://example.com
storage.ftp.username=ftp_user
storage.ftp.password=ftp_password
```

## 1. Install
```bash
pip install faird-local
```

## 2. Best Practice
### 2.1 Importing dependencies
```python
from local_sdk import faird
```

### 2.2 Get data directory
```python
dataset_ids = faird.list_datasets()
dataframe_ids = faird.list_dataframes("{dataset_id}")
```

### 2.3 Open DataFrame
```python
df = faird.open("{dataframe_id}")
```

### 2.4 Operate DataFrame
```python
"""
1. basic attributes
"""
schema = df.schema
column_names = df.column_names
num_rows = df.num_rows
num_columns = df.num_columns
shape = df.shape
nbytes = df.nbytes

"""
2. get all data, streaming data
"""
## 2.1 get all data
logger.info(df)
df.to_string(head_rows=5, tail_rows=5, first_cols=3, last_cols=3, display_all=False) 

## 2.2 streaming
stream_data = df.get_stream(max_chunksize=100) # max_chunksize指定chunk行数，默认1000
for chunk in stream_data:
    logger.info(chunk)
    logger.info(f"Chunk size: {chunk.num_rows}")


"""
3. row & column operations
"""
## 3.1 use index and column name to get row and column
row_0 = df[0]
column_OBJECTID = df["OBJECTID"]
cell = df[0]["OBJECTID"]

## 3.2 limit, slice, select
limit_3 = df.limit(3)
slice_2_5 = df.slice(2, 5)
select_columns = df.select("OBJECTID", "start_p", "end_p")

"""
4. filter, map
"""
## 4.1 filter
mask = pc.less(df["OBJECTID"], 30)
filtered_data = df.filter(mask)

## 4.2 map
mapped_df = df.map("OBJECTID", lambda x: x + 10, new_column_name="OBJECTID_PLUS_10")

## 4.3 sum
sum = df.sum('OBJECTID')

"""
5. from_pandas, to_pandas
"""
## 5.1 to_pandas
pdf = df.to_pandas()

## 5.2 to_pandas
pandas_data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
df_from_pandas = DataFrame.from_pandas(pandas_data)
logger.info(df_from_pandas)


```
