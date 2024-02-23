import os
from datetime import datetime
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from dagster import asset, AssetIn, Output

# Set your envrionment if you have problem with version in Spark
os.environ["PYSPARK_PYTHON"] = "/home/kyphan/anaconda3/bin/python3.10"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/kyphan/anaconda3/bin/python3.10"

def get_spark_session():
    spark = SparkSession.builder \
    .appName("airbnb") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "8") \
    .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

    return spark


@asset(
    io_manager_key="spark_io_manager",
    ins={
        "host_table": AssetIn(key_prefix=["silver", "schema"]),
        "location_table": AssetIn(key_prefix=["silver", "schema"]),
        "fact_table": AssetIn(key_prefix=["silver", "schema"]),
        "listing_table": AssetIn(key_prefix=["silver", "schema"]),
        "review_table": AssetIn(key_prefix=["silver", "schema"]),
    },
    key_prefix=["gold", "report"],
    group_name="gold_layer",
    compute_kind="PySpark",
    description="Extract useful information for map visualization"
)
def rental_dataset(
    context,
    host_table: pd.DataFrame,
    location_table: pd.DataFrame,
    fact_table: pd.DataFrame,
    listing_table: pd.DataFrame,
    review_table: pd.DataFrame
) -> Output[pd.DataFrame]:

    spark = get_spark_session()

    # Trick to handle pandas vs spark version
    host_table.iteritems = host_table.items
    location_table.iteritems = location_table.items
    fact_table.iteritems = fact_table.items
    listing_table.iteritems = listing_table.items
    review_table.iteritems = review_table.items

    # Create Pyspark DataFrame for Pandas DataFrame
    host_table_spark = spark.createDataFrame(host_table)
    location_table_spark = spark.createDataFrame(location_table)
    fact_table_spark = spark.createDataFrame(fact_table)
    listing_table_spark = spark.createDataFrame(listing_table)
    review_table_spark = spark.createDataFrame(review_table)

    # Create view to use SQL query
    host_table_spark.createOrReplaceTempView("host_table")
    location_table_spark.createOrReplaceTempView("location_table")
    fact_table_spark.createOrReplaceTempView("fact_table")
    listing_table_spark.createOrReplaceTempView("listing_table")

    # Extract name, host_name, latitude, longtitude, room_type, price, city, country to visualize
    # sql_stm= """
    #     SELECT
    #         lt.name,
    #         ht.host_name,
    #         loct.latitude,
    #         loct.longitude,
    #         lt.room_type,
    #         CASE 
    #             WHEN loct.country = "CA" THEN ft.price*0.75 
    #             ELSE ft.price
    #         END AS price,
    #         loct.city,
    #         loct.country
    #     FROM
    #         fact_table AS ft
    #     JOIN
    #         listing_table AS lt ON ft.id = lt.id
    #     JOIN
    #         host_table AS ht ON lt.host_id = ht.host_id
    #     JOIN
    #         location_table AS loct ON lt.location_id = loct.location_id
    # """

    sql_stm = """
        WITH city_data AS (
        SELECT
            lt.name,
            ht.host_name,
            loct.latitude,
            loct.longitude,
            lt.room_type,
            CASE 
                WHEN loct.country = "CA" THEN ft.price*0.75 
                ELSE ft.price
            END AS price,
            loct.city,
            loct.country,
            ROW_NUMBER() OVER(PARTITION BY loct.city ORDER BY RANDOM()) AS rn
        FROM
            fact_table AS ft
        JOIN
            listing_table AS lt ON ft.id = lt.id
        JOIN
            host_table AS ht ON lt.host_id = ht.host_id
        JOIN
            location_table AS loct ON lt.location_id = loct.location_id
    )
    SELECT
        name,
        host_name,
        latitude,
        longitude,
        room_type,
        price,
        city,
        country
    FROM
        city_data
    WHERE
        rn <= 2500
    """

    df = spark.sql(sql_stm).toPandas() # Convert back into Pandas DataFrame

    return Output(
        df,
        metadata={
            "table": "combined_sql_data",
            "records count": len(df),
        }
    )
