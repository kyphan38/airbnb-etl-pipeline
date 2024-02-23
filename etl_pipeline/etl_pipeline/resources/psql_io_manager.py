from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
import pyarrow.parquet as pq

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        layer, schema, table = context.asset_key.path
        key = f"{layer}/{schema}/{table}"
        tmp_dir_path = f"/tmp/{layer}/{schema}/"

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table = context.asset_key.path[-1]
        schema = context.asset_key.path[-2]
        with connect_psql(self._config) as engine:
            obj.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
            
        print("Write successfully!")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp_file_path = self._get_path(context) # tmp_file_path: ... .parquet
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