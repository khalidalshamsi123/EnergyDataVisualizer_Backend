import glob
import asyncio

# specify the directory
directory = "data"

# get a list of all CSV files in the directory
csv_files = glob.glob(directory + "/*.csv")

import polars as pl

from utils.redis_pool import get_redis

r = get_redis()

# Iterates over all CSV files, reading them into a DataFrame
# and storing the IPC stream to Redis with a unique key.
async def initialise_csv():
    for file in csv_files:
        # Store file name without .csv at end and without the directory 'data\' at the beginning.
        file_name = file[len(directory)+1:-4]

        # More inefficient than my last approach in terms of reading the file. However, after discussing
        # the issue with Kavin we agreed that this method is the best in this scenario.
        # Since it allows us to store the entire set of data as a value in Redis. Instead of storing batches
        # of rows with a different key for each batch.

        # Additionally, this initalisation code is typically ran once a year, so low efficiency isn't a huge problem here.
        reader = pl.read_csv(file)
        # I chose zstd compression because it's a good balance between speed and compression ratio.
        # Reduces the memory usage of the largest CSV in our dataset when stored in Redis from
        # 167.77 mb to 33.55 mb.
        ipc = reader.write_ipc_stream(None, compression='zstd').getvalue()
        try:
            await r.set(f"caches:dataframes:{file_name}:original", ipc)
        except Exception as e:
            print(e)
        print(f"| Success |: Stored {file_name}'s contents in Redis.")

    # Test reading from Redis. Works as intended.
    # data = await r.get("caches:dataframes:Thermal_characteristics_afterEE:original")
    # polars_data = pl.read_ipc_stream(data)
    # print(polars_data)
async def main():
    await initialise_csv()

if __name__ == "__main__":
    asyncio.run(main())