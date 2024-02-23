import pandas as pd
import os

def merge_files_by_region(dirs, country):
    dfs = {}

    for dir_ in dirs: 
        region = os.path.basename(dir_)  # Get the basename ../CA/Atlantic => Atlantic
        region_dfs = []
        for file in os.listdir(dir_):  # Each file in a region
            if file.endswith(".csv"):
                city = file[:-4].replace("_", " ")  # New_Brunswick.csv => New Brunswick
                df = pd.read_csv(os.path.join(dir_, file))
                df = df.assign(city=city)
                region_dfs.append(df)  # Append each file in the region folder

        dfs[f'{country}_{region.replace(" ", "_")}'] = pd.concat(region_dfs, ignore_index=True)  # Example: US_British_Columbia

    output_dir_ = os.path.join(os.path.curdir, "..", country)  # . then .. then country
    os.makedirs(output_dir_, exist_ok=True)  # Create if not exists

    for name, df in dfs.items():
        df.to_csv(os.path.join(output_dir_, f"{name}.csv"), index=False)  # .../country/ + name.csv

if __name__ == "__main__":
    
    # Define data storage path
    dir_CA = ["../CA/Atlantic", "../CA/British Columbia", "../CA/Ontario", "../CA/Prairies", "../CA/Quebec"]
    dir_US = ["../US/Midwest", "../US/Northeast", "../US/Southeast", "../US/West"]

    merge_files_by_region(dir_CA, "CA")
    merge_files_by_region(dir_US, "US")

    print("All files have been successfully merged into the each region file.")
