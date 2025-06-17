import pyarrow as pa
from sdk.dacp_client import DacpClient, Principal
from sdk.dataframe import DataFrame
import pyarrow.flight

def create_dir_dataframe():
    files_data = [
        {
            "name": "sample.tiff",
            "path": "/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/sample.tiff",
            "suffix": "tiff",
            "type": "no",
            "size": 592386,
            "time": "2025-06-01T00:10:16.688000000",
            "blob": None
         },
        {
            "name": "SOCATv2021_tracks_gridded_decadal.csv",
            "path": "/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat/SOCATv2021_tracks_gridded_decadal.csv",
            "suffix": "csv",
            "type": "no",
            "size": 958045,
            "time": "2025-06-01T00:10:16.688000000",
            "blob": None
        }
    ]
    # 定义 Arrow 表的 schema
    schema = pa.schema([
        ("name", pa.string()),
        ("path", pa.string()),
        ("suffix", pa.string()),
        ("type", pa.string()),
        ("size", pa.int64()),
        ("time", pa.string()),
        ("blob", pa.binary())
    ])
    return pa.Table.from_pydict({key: [file[key] for file in files_data] for key in schema.names}, schema=schema)

if __name__ == "__main__":
    df = DataFrame(id="/sharedata/dataset/historical/SD039-SurfOcean_CO2_Atlas/SOCATv2021_Gridded_Dat", connection_id=0)
    df.data = create_dir_dataframe()
    print(df)

    batches = df.data.to_batches()
    updated_batches = []
    for batch in batches:
        path_column = batch.column(batch.schema.get_field_index("path")).to_pylist()
        blob_column_index = batch.schema.get_field_index("blob")
        blob_data = []
        for path in path_column:
            try:
                file_path = "/Users/yaxuan/Desktop" + path
                with open(file_path, "rb") as f:
                    blob_data.append(f.read())
            except Exception as e:
                blob_data.append(None)
        blob_array = pa.array(blob_data, type=pa.binary())
        updated_batch = batch.set_column(blob_column_index, "blob", blob_array)
        updated_batches.append(updated_batch)
    print(111)