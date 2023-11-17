import os

import polars as pl
import pandas as pd
import geopolars as gpl
import geopandas as gpd
from fastapi import APIRouter, Response

router = APIRouter()

dataset = os.getenv("DATA_DIR")

paths = {
    "ukerc_scotland_data": f"{dataset}/Residential heat demand in LSOAs in Scotland/ukerc_scotland_data.csv",
}


@router.get("/maps/{map_type}")
async def read_map(map_type: str, fields: str):
    # TODO: Remove this, there's a lot of scope for performance optimizations here
    # return Response(open("/home/kavin/output.json", "rb").read(), media_type="application/json")

    if map_type not in paths:
        return {"error": "Invalid map type"}

    fields = fields.split(",")

    print("Reading map type: " + map_type)

    # Read the shapefile using GeoPandas
    lsoa_gdf = gpd.read_file('infuse_lsoa_lyr_2011_simplified.shp')
    # We need to do this to get GeoJSON coordinates
    lsoa_gdf.to_crs(epsg=4326, inplace=True)

    # Read the CSV
    df = pd.read_csv(paths[map_type], index_col=0)

    df.dropna(inplace=True)

    merged_df = lsoa_gdf.merge(df, left_on="geo_code", right_on='LSOA11CD', how='right')

    gdf = merged_df

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
    for _, row in gdf.iterrows():
        feature = {
            "type": "Feature",
            "properties": {},
            # Convert the geometry to GeoJSON using the __geo_interface__ attribute
            "geometry": row['geometry'].__geo_interface__
        }

        def _tuple_round(coords):
            if isinstance(coords[0], float):
                return tuple([round(c, 4) for c in coords])
            else:
                return tuple([_tuple_round(c) for c in coords])

        # Round off the coordinates to 4 decimal places
        feature['geometry']['coordinates'] = _tuple_round(feature['geometry']['coordinates'])

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

    # Return the GeoJSON
    return geojson
