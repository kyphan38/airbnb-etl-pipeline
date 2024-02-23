import pandas as pd

def merge_files(files, country):
    merged_df = pd.concat([pd.read_csv(f) for f in files]) # Merge all files in each country
    merged_df["country"] = country # Add a country column that corresponds to its country 
    
    return merged_df

if __name__ == "__main__":

    CA_files = ["../CA/CA_Atlantic.csv",
                "../CA/CA_British_Columbia.csv",
                "../CA/CA_Ontario.csv",
                "../CA/CA_Prairies.csv",
                "../CA/CA_Quebec.csv"]

    US_files = ["../US/US_Midwest.csv",
                "../US/US_Northeast.csv",
                "../US/US_Southeast.csv",
                "../US/US_West.csv"]

    ca_data = merge_files(CA_files, "CA")
    us_data = merge_files(US_files, "US")
    merged_data = pd.concat([ca_data, us_data]) # Merge files of two countries

    output_dir_ = "../schema/airbnb.csv"
    merged_data.to_csv(output_dir_, index=False)
    
    print("All files have been successfully merged into the single file 'airbnb.csv'.")