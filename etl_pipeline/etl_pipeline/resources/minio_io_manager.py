from contextlib import contextmanager
from typing import Any
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq  
from contextlib import contextmanager
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

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


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        layer, schema, table = context.asset_key.path # Get layer, schema, table
        key = f"{layer}/{schema}/{table}" # example: bronze/US/Midwest
        tmp_dir_path = f"/tmp/{layer}/{schema}/" # example:/tmp/bronze/US

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        return f"{key}.pq", tmp_file_path # example: bronze/US/Midwest.pq, /tmp/bronze/US/Midwest.parquet

    def handle_output(self, context: OutputContext, obj: pd.DataFrame): # context: OutputContext manage
        key_name, tmp_file_path = self._get_path(context)
        table = pa.Table.from_pandas(obj) # Create pyarrow table from dataframe (obj)
        pq.write_table(table, tmp_file_path) # Write table just created into tmp_file_path

        # Upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} already exists")

                client.fput_object(bucket_name=bucket_name, object_name=key_name, file_path=tmp_file_path)
                row_count = len(obj)
                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})

            print("Write successfully!")
            
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception:
            raise
    

    def load_input(self, context: InputContext) -> Any: # context: InputContext manage
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        
        # Get from MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                client.fget_object(bucket_name, key_name, tmp_file_path)
               
            df = pd.read_parquet(tmp_file_path)

            # Clean up tmp file
            os.remove(tmp_file_path)

            return df

        except Exception:
            raise