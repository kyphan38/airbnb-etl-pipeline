from dagster import asset, AssetIn, Output
import pandas as pd
import numpy as np

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "CA_Atlantic": AssetIn(key_prefix=["bronze", "CA"]),
        "CA_British_Columbia": AssetIn(key_prefix=["bronze", "CA"]),
        "CA_Ontario": AssetIn(key_prefix=["bronze", "CA"]),
        "CA_Prairies": AssetIn(key_prefix=["bronze", "CA"]),
        "CA_Quebec": AssetIn(key_prefix=["bronze", "CA"]),
        "US_Midwest": AssetIn(key_prefix=["bronze", "US"]),
        "US_Northeast": AssetIn(key_prefix=["bronze", "US"]),
        "US_Southeast": AssetIn(key_prefix=["bronze", "US"]),
        "US_West": AssetIn(key_prefix=["bronze", "US"]),
    },
    key_prefix=["silver", "data"],
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Merge all files into one file"
)
def airbnb_dataset(
    context,
    CA_Atlantic: pd.DataFrame,
    CA_British_Columbia: pd.DataFrame,
    CA_Ontario: pd.DataFrame,
    CA_Prairies: pd.DataFrame,
    CA_Quebec: pd.DataFrame,
    US_Midwest: pd.DataFrame,
    US_Northeast: pd.DataFrame,
    US_Southeast: pd.DataFrame,
    US_West,
) -> Output[pd.DataFrame]:

    CA_data = pd.concat([CA_Atlantic, CA_British_Columbia, CA_Ontario, CA_Prairies, CA_Quebec])
    CA_data["country"] = "CA"

    US_data = pd.concat([US_Midwest, US_Northeast, US_Southeast, US_West])
    US_data["country"] = "US"

    df = pd.concat([CA_data, US_data])
    df["neighbourhood_group"] = df["neighbourhood_group"].fillna("Unknown")
    df = df.sort_values("last_review", ascending=False)
    df = df.drop_duplicates(subset=df.columns.difference(["id", "price"]), keep="first")

    return Output(
        df,
        metadata={
            "table": "airbnb_dataset",
            "records count": len(df),
        }
    )

# Split schema into 5 separate assets
@asset(
    io_manager_key="minio_io_manager",
    ins={"airbnb_dataset": AssetIn(airbnb_dataset.key)}, # .key means showing the dependencies not data
    key_prefix=["silver", "schema"], 
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Contain host information"
)
def host_table(context, airbnb_dataset): # This is actually the data
    host_table = airbnb_dataset[["host_id", "host_name"]].copy().drop_duplicates()
    return Output(
        host_table,
        metadata={
            "table": "host_table",
            "records count": len(host_table),
        }
    )


@asset(
    io_manager_key="minio_io_manager",
    ins={"airbnb_dataset": AssetIn(airbnb_dataset.key)}, 
    key_prefix=["silver", "schema"], 
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Contain location information"
)
def location_table(context, airbnb_dataset):
    location_table = airbnb_dataset[["neighbourhood_group", "neighbourhood", "city", "country", "latitude", "longitude"]].copy().drop_duplicates()
    location_table.insert(0, "location_id", range(1, 1 + len(location_table)))

    return Output(
        location_table,
        metadata={
            "table": "location_table",
            "records count": len(location_table),
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    ins={"airbnb_dataset": AssetIn(airbnb_dataset.key)}, 
    key_prefix=["silver", "schema"], 
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Contain numeric features"
)
def fact_table(context, airbnb_dataset):
    fact_table = airbnb_dataset[["id", "price", "minimum_nights", "availability_365", "calculated_host_listings_count", "number_of_reviews", "reviews_per_month", "number_of_reviews_ltm"]].copy().drop_duplicates()

    return Output(
        fact_table,
        metadata={
            "table": "fact_table",
            "records count": len(fact_table),
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    ins={"airbnb_dataset": AssetIn(airbnb_dataset.key)}, 
    key_prefix=["silver", "schema"], 
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Contain review information"
)
def review_table(context, airbnb_dataset):
    review_table = airbnb_dataset[["id", "last_review"]].copy().drop_duplicates()

    return Output(
        review_table,
        metadata={
            "table": "review_table",
            "records count": len(review_table),
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "airbnb_dataset": AssetIn(airbnb_dataset.key),
        "location_table": AssetIn(location_table.key)
    }, 
    key_prefix=["silver", "schema"], 
    group_name="silver_layer",
    compute_kind="Pandas",
    description="Merge from airbnb_dataset and location_table"
)
def listing_table(context, airbnb_dataset, location_table):
    airbnb_dataset = pd.merge(airbnb_dataset, location_table, on=["neighbourhood_group", "neighbourhood", "city", "latitude", "longitude"])
    listing_table = airbnb_dataset[["id", "name", "room_type", "host_id", "location_id", "license"]].copy().drop_duplicates()

    return Output(
        listing_table,
        metadata={
            "table": "listing_table",
            "records count": len(listing_table),
        }
    )

