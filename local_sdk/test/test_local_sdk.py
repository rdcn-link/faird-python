import faird
import pyarrow.compute as pc
import pandas as pd

from dataframe import DataFrame
from utils.logger_utils import get_logger
logger = get_logger(__name__)


def test_local_sdk():

    """
    1. list datasets, list dataframes
    """
    dataset_ids = faird.list_datasets()
    dataframe_ids = faird.list_dataframes(dataset_ids[6])

    """
    2. open dataframe
    """
    df = faird.open(dataframe_ids[1])

    """
    3. basic attributes
    """
    schema = df.schema
    column_names = df.column_names
    num_rows = df.num_rows
    num_columns = df.num_columns
    shape = df.shape
    nbytes = df.nbytes

    """
    4. collect data, stream data
    """
    ## 4.1 collect all data
    all_data = df.collect()
    logger.info(f"data size: {all_data.num_rows}")
    ## 4.2 stream data
    stream_data = df.get_stream(max_chunksize=100)
    for chunk in stream_data:
        logger.info(chunk)
        logger.info(f"Chunk size: {chunk.num_rows}")

    """
    5. row & column operations
    """
    ## 5.1 use index and column name to get row and column
    row_0 = df[0]
    column_OBJECTID = df["OBJECTID"]
    cell = df[0]["OBJECTID"]

    ## 5.2 limit, slice, select
    limit_3 = df.limit(3)
    slice_2_5 = df.slice(2, 5)
    select_columns = df.select("OBJECTID", "start_p", "end_p")

    """
    6. filter, map
    """
    ## 6.1 filter
    mask = pc.less(df["OBJECTID"], 30)
    filtered_data = df.filter(mask)

    ## 6.2 map
    mapped_df = df.map("OBJECTID", lambda x: x + 10, new_column_name="OBJECTID_PLUS_10")

    ## 6.3 sum
    sum = df.sum('OBJECTID')

    """
    7. from_pandas(), to_pandas()
    """
    pdf = df.to_pandas()

    pandas_data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
    df_from_pandas = DataFrame.from_pandas(pandas_data)
    logger.info(df_from_pandas)


if __name__ == "__main__":
    test_local_sdk()