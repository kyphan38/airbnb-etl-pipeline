from dagster import Definitions
from .assets.bronze_layer import (
    CA_Atlantic_dataset,
    CA_British_Columbia_dataset,
    CA_Ontario_dataset,
    CA_Prairies_dataset,
    CA_Quebec_dataset,
)
from .assets.bronze_layer import (
    US_Midwest_dataset,
    US_Northeast_dataset,
    US_Southeast_dataset,
    US_West_dataset,
)
from .assets.silver_layer import (
    airbnb_dataset,
    host_table,
    location_table,
    fact_table,
    listing_table,
    review_table,
)
from .assets.gold_layer import rental_dataset
from .assets.warehouse_layer import rental_info_postgres # rental_info_cloud

from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.spark_io_manager import SparkIOManager
#from .resources.bigquery_io_manager import BigqueryIOManager

import os
from dotenv import load_dotenv
load_dotenv() # Correct when .env same this file

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "minio_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
}

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

SPARK_CONFIG = {
    "endpoint_url": os.getenv("SPARK_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "minio_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
}

# BIGQUERY_CONFIG = {
#     "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
#     "dataset_id": os.getenv("BIGQUERY_DATASET_ID"),
#     "credentials": "<Path of credentials here>",
# }

defs = Definitions(
    assets=[
        CA_Atlantic_dataset, CA_British_Columbia_dataset, CA_Ontario_dataset, CA_Prairies_dataset, CA_Quebec_dataset,
        US_Midwest_dataset, US_Northeast_dataset, US_Southeast_dataset, US_West_dataset,
        airbnb_dataset, host_table, location_table, fact_table, listing_table, review_table,
        rental_dataset,
        rental_info_postgres,
        # rental_info_cloud
    ],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "spark_io_manager": SparkIOManager(SPARK_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        # "bigquery_io_manager": BigqueryIOManager(BIGQUERY_CONFIG)
    }
)


