from dagster import asset, AssetIn, Output
import pandas as pd

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "rental_dataset": AssetIn(key_prefix=["gold", "report"]),
    },
    key_prefix=["rental_info", "gold"],
    group_name="warehouse_layer",
    compute_kind="PostgreSQL"
)
def rental_info_postgres(
    context,
    rental_dataset: pd.DataFrame
)-> Output[pd.DataFrame]:

    return Output(
        rental_dataset,
        metadata={
            "table": "rental_info",
            "records count": len(rental_dataset)
        }
    )

# @asset(
#     io_manager_key="bigquery_io_manager",
#     ins={
#         "rental_dataset": AssetIn(key_prefix=["gold", "report"]),
#     },
#     key_prefix=["cloud", "bigquery"],
#     group_name="warehouse_layer",
#     compute_kind="BigQuery"
# )
# def rental_info_cloud(
#     context,
#     rental_dataset: pd.DataFrame
# )-> Output[pd.DataFrame]:

#     return Output(
#         rental_dataset,
#         metadata={
#             "table": "rental_info",
#             "records count": len(rental_dataset)
#         }
#     )

