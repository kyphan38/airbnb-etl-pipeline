import os
import pandas as pd
from typing import Any
from datetime import datetime
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from dagster import IOManager, OutputContext, InputContext

# Just like Minio because I completely init and create spark to process in gold_layer.py 

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key_id"),
        secret_key=config.get("minio_secret_access_key"),
        secure=False
    )
    try:
        yield client
    except Exception:
        raise

class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        layer, schema, table = context.asset_key.path
        key = f"{layer}/{schema}/{table}"
        tmp_dir_path = f"/tmp/{layer}/{schema}/"

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: Any):
        key_name, tmp_file_path = self._get_path(context) # tmp_file_path: ... .parquet
        table = pa.Table.from_pandas(obj) # Create pa table from pandas
        pq.write_table(table, tmp_file_path) # Write pa table ith parquet file into tmp_file_path

        # Upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fput_object(bucket_name=bucket_name, object_name=key_name, file_path=tmp_file_path) # Upload into bucket Minio
                row_count = len(obj)
                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})

            print("Write successfully!")
            
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception:
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket") # Get bucket

        # Get from MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                client.fget_object(bucket_name, key_name, tmp_file_path)
               
            df = pd.read_parquet(tmp_file_path) # Read parquet file into dataframe from minio

            # Clean up tmp file
            os.remove(tmp_file_path)

            return df

        except Exception:
            raise