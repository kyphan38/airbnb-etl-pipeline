import pandas as pd
df = pd.read_csv("../schema/airbnb.csv")

df["neighbourhood_group"] = df["neighbourhood_group"].fillna("Unknown")

# Note: id means the id of original dataframe
# fact_table (id) => listing_table and review_table store id of the original dataframe

# Create host_table (host_id, host_name)
host_table = df[["host_id", "host_name"]].copy()
host_table = host_table.drop_duplicates()

# Craete location table (location_id, neighbourhood_group, neighbourhood, city, latitude, longitude)
location_table = df[["neighbourhood_group", "neighbourhood", "city", "latitude", "longitude"]].copy()
location_table = location_table.drop_duplicates() # Drop duplicated locations
location_table.insert(0, "location_id", range(1, 1 + len(location_table))) # Index location_id

# Add location_id to original df
df = pd.merge(df, location_table, on=["neighbourhood_group", "neighbourhood", "city", "latitude", "longitude"])

# Create fact table (id, price, minimum_nights, availability_365, calculated_host_listings_count, number_of_reviews, reviews_per_month, number_of_reviews_ltm) - contains numerical features
fact_table = df[["id", "price", "minimum_nights", "availability_365", "calculated_host_listings_count", "number_of_reviews", "reviews_per_month", "number_of_reviews_ltm"]].copy()

# Create listing table (id, name, room_type, host_id, location_id, liscense)
listing_table = df[["id", "name", "room_type", "host_id", "location_id", "license"]].copy()

# Create review_table (id, last_review)
review_table = df[["id", "last_review"]].copy()

fact_table.to_csv("../schema/fact_table.csv", index=False)
host_table.to_csv("../schema/host_table.csv", index=False)
location_table.to_csv("../schema/location_table.csv", index=False)
listing_table.to_csv("../schema/listing_table.csv", index=False)
review_table.to_csv("../schema/review_table.csv", index=False)