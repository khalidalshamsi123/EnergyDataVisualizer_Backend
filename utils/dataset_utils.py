import os
import pickle

import polars as pl

from utils.redis_pool import get_redis

data = os.getenv("DATA_DIR")


async def get_csv_frame(file_name: str, columns: list[str] = None):
    redis = get_redis()

    ipc = await redis.get(f"caches:dataframes:{file_name}:original")

    if ipc is not None:
        return pl.read_ipc_stream(ipc, columns=columns)

    # recursively search for the file
    for root, dirs, files in os.walk(data):
        if f"{file_name}.csv" in files:
            df = pl.read_csv(f"{root}/{file_name}.csv")

            ipc = df.write_ipc_stream(None, compression="zstd").getvalue()
            await redis.set(f"caches:dataframes:{file_name}:original", ipc)

            if columns is not None:
                df = pl.read_ipc_stream(ipc, columns=columns)

            return df


async def get_lsoa_geojson():
    import geopandas as gpd

    redis = get_redis()

    ipc = await redis.get("caches:dataframes:lsoa_geojson:original")

    if ipc is not None:
        return pl.read_ipc_stream(ipc)

    # Read the shapefile using GeoPandas
    # We can't use geopolars as the library has some issues with the shapefile we're using.
    lsoa_gdf = gpd.read_file(f'{data}/infuse_lsoa_lyr_2011_simplified.shp')

    # We need to do this to get GeoJSON coordinates
    lsoa_gdf.to_crs(epsg=4326, inplace=True)

    def _tuple_round(coords):
        if isinstance(coords[0], float):
            return tuple([round(c, 4) for c in coords])
        else:
            return tuple([_tuple_round(c) for c in coords])

    lsoa_gdf["geojson"] = lsoa_gdf["geometry"].apply(lambda x: x.__geo_interface__)

    # Round off the coordinates to 4 decimal places
    lsoa_gdf["geojson"] = lsoa_gdf["geojson"].apply(lambda g: {**g, 'coordinates': _tuple_round(g['coordinates'])})

    # convert to pickled geojson
    lsoa_gdf["geojson"] = lsoa_gdf["geojson"].apply(lambda x: pickle.dumps(x))

    df = pl.DataFrame({
        "geo_code": pl.from_pandas(lsoa_gdf["geo_code"]),
        "geometry": pl.from_pandas(lsoa_gdf["geojson"]),
    })

    ipc = df.write_ipc_stream(None, compression="zstd").getvalue()

    await redis.set("caches:dataframes:lsoa_geojson:original", ipc)

    return df
