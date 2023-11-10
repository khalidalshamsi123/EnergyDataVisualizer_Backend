import glob

# specify the directory
directory = "/data"

# get a list of all CSV files in the directory
csv_files = glob.glob(directory + "/*.csv")

import csv
import polars as pl
# from utils.redis_pool import get_redis

# r = get_redis()

# Efficiently reads csv file in chunks. Creating a dataframe per 100 rows.
# This can be taken and stored into redis 'chunk' by 'chunk'.
reader = pl.read_csv_batched("Thermal_characteristics_afterEE.csv", batch_size=100)
batches = reader.next_batches(15)
for df in batches:
    print(df);