import asyncio
import copy
import json
import pickle

import polars as pl
from blake3 import blake3
from fastapi import APIRouter, Response

from utils.dataset_utils import get_lsoa_geojson, get_csv_frame
from utils.redis_pool import get_redis

router = APIRouter()


@router.get("/maps/{map_type}")
async def read_map(map_type: str, fields: str):
    fields = set(fields.split(","))

    keys = copy.copy(fields)
    keys.add(map_type)
    keys = ''.join(sorted(keys))

    hasher = blake3()
    hasher.update(keys.encode('utf-8'))
    keys = hasher.hexdigest()[0:8]

    cache_key = f"caches:geojson:maps:{keys}"

    if await get_redis().exists(cache_key):
        return Response(await get_redis().getex(cache_key, 120), media_type="application/json")

    print("Reading map type: " + map_type)

    join_cache_key = f"caches:dataframes:geojoin:{map_type}"

    if await get_redis().exists(join_cache_key):
        gdf = pl.read_ipc_stream(await get_redis().get(join_cache_key))
    else:
        lsoa_geojson = await get_lsoa_geojson()
        # Read the shapefile using GeoPandas

        # Read the CSV
        df = await get_csv_frame(map_type)

        merged_df = lsoa_geojson.join(df, left_on="geo_code", right_on='LSOA11CD', how='inner')

        gdf = merged_df

        asyncio.create_task(get_redis().set(join_cache_key, gdf.write_ipc_stream(None, compression="zstd").getvalue()))

    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    geojson["features"].append({
        "type": "Feature",
        "properties": {
            idx: field for idx, field in enumerate(fields)
        },
    })

    # Iterate through the rows and create the GeoJSON features
    for row in gdf.iter_rows(named=True):
        feature = {
            "type": "Feature",
            "properties": {},
            # Load the pickled GeoJSON
            "geometry": pickle.loads(row['geometry'])
        }

        def _round(x):
            if isinstance(x, float):
                return round(x, 2)
            else:
                return x

        # add all fields in the format index: value
        # this is to save space in the GeoJSON
        properties = {
            idx: _round(row[field]) for idx, field in enumerate(fields)
        }

        feature['properties'] = properties
        geojson['features'].append(feature)

    geojson = json.dumps(geojson)

    asyncio.create_task(get_redis().setex(cache_key, 60, geojson))

    # Return the GeoJSON
    return Response(geojson, media_type="application/json")
