import os
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account
from dagster import IOManager, OutputContext, InputContext
from contextlib import contextmanager

class BigqueryIOManager(IOManager):
    def __init__(self, config): # Get information of BigQuery 
        self._config = config
        credentials = service_account.Credentials.from_service_account_file(config["credentials"])
        self.client = bigquery.Client(credentials=credentials, project=config["project_id"])

    def _get_path(self, context): 
        layer, schema, table = context.asset_key.path 
        key = f"{layer}/{schema}/{table}"
        tmp_dir_path = f"/tmp/{layer}/{schema}/"

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table_id = f'{self._config["project_id"]}.{self._config["dataset_id"]}.{context.asset_key.path[-1]}'

        for column in obj.select_dtypes(include=["object"]).columns: # Convert deicmal in object into float
            try:
                obj[column] = pd.to_numeric(obj[column])
            except ValueError:
                pass

        self.client.create_table(bigquery.Table(table_id, schema=[]), exists_ok=True) # Create table if not exists

        # Write data into BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # WRITE_TRUNCATE means if table existed, remove the whole table and load the new table
        job = self.client.load_table_from_dataframe(obj, table_id, job_config=job_config)
        job.result()

        if job.errors:
            print("Errors:", job.errors)
        else:
            print("Write successfully!")


    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context) 
        bucket_name = self._config.get("bucket") 

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