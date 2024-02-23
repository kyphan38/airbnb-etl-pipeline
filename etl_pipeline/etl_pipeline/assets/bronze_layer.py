import pandas as pd
from dagster import asset, Output

compute_kind = "Pandas"
layer = "bronze_layer"
data_dir = "../../../data"

def create_dataset(asset_name, description, io_manager_key, country, info):
    dir_ = f"{data_dir}/{country}/{asset_name}.csv"
    
    @asset(
        name=asset_name,
        description=description,
        io_manager_key=io_manager_key,
        key_prefix=["bronze", country],
        compute_kind=compute_kind,
        group_name=layer,
    )
    def dataset(context):
        df = pd.read_csv(dir_)
        return Output(
            df,
            metadata={
                "directory": dir_,
                "country": country,
                "info": info
            }
        )

    return dataset

# Atlantic
CA_Atlantic_dataset = create_dataset(
    asset_name="CA_Atlantic",
    description="Canada's region",
    io_manager_key="minio_io_manager",
    country="CA",
    info="Airbnb Atlantic Canada Dataset"
)

# British Columbia
CA_British_Columbia_dataset = create_dataset(
    asset_name="CA_British_Columbia",
    description="Canada's region",
    io_manager_key="minio_io_manager",
    country="CA",
    info="Airbnb British Columbia Canada Dataset"
)

# Ontario
CA_Ontario_dataset = create_dataset(
    asset_name="CA_Ontario",
    description="Canada's region",
    io_manager_key="minio_io_manager",
    country="CA",
    info="Airbnb Ontario Canada Dataset"
)

# Prairies
CA_Prairies_dataset = create_dataset(
    asset_name="CA_Prairies",
    description="Canada's region",
    io_manager_key="minio_io_manager",
    country="CA",
    info="Airbnb Prairies Canada Dataset"
)

# Quebec
CA_Quebec_dataset = create_dataset(
    asset_name="CA_Quebec",
    description="Canada's region",
    io_manager_key="minio_io_manager",
    country="CA",
    info="Airbnb Quebec Canada Dataset"
)

# Midwest
US_Midwest_dataset = create_dataset(
    asset_name="US_Midwest",
    description="The United States' region",
    io_manager_key="minio_io_manager",
    country="US",
    info="Airbnb Midwestern United States Dataset"
)

# Northeast
US_Northeast_dataset = create_dataset(
    asset_name="US_Northeast",
    description="The United States' region",
    io_manager_key="minio_io_manager",
    country="US",
    info="Airbnb Northeastern United States Dataset"
)

# Southeast
US_Southeast_dataset = create_dataset(
    asset_name="US_Southeast",
    description="The United States' region",
    io_manager_key="minio_io_manager",
    country="US",
    info="Airbnb Southeastern United States Dataset"
)

# West
US_West_dataset = create_dataset(
    asset_name="US_West",
    description="The United States' region",
    io_manager_key="minio_io_manager",
    country="US",
    info="Airbnb Western United States Dataset"
)
