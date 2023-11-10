import glob

# specify the directory
directory = "data"

# get a list of all CSV files in the directory
csv_files = glob.glob(directory + "/*.csv")

import polars as pl

from utils.redis_pool import get_redis

r = get_redis()

# Calculate the number of rows in the csv file.
# This is used to calculate the number of chunks.

for file in csv_files:
    batch_count = 1
    reader = pl.read_csv_batched(file, batch_size=100)
    # Process 15 rows at a time.
    while True:
        batches = reader.next_batches(15)
        if batches is None:
            break
        concat_batch = pl.concat(batches)
        ipc_batch = concat_batch.write_ipc_stream()
        r.set(f"caches:dataframes:{file}:{batch_count}:original", ipc_batch)
        batch_count = batch_count + 1