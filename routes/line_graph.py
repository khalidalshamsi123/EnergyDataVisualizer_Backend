import copy
import math

import polars as pl
from fastapi import APIRouter

from utils.dataset_utils import get_csv_frame

router = APIRouter()


@router.get("/api/line-graph")
async def get_line_graph(fields: str):
    fields = fields.split(",")

    df = await get_csv_frame("Half-hourly_profiles_of_heating_technologies", columns=fields)

    # parse index column as datetime
    df = df.with_columns(
        pl.col("index").str.to_datetime(format="%Y-%m-%dT%H:%M:%S").cast(pl.Int64).apply(lambda x: int(x / 1000000)))

    min_val = math.inf
    max_val = -math.inf

    for column in df.columns:
        if column != "index":
            if df[column].dtype == pl.Float64:
                df = df.with_columns(pl.col(column).apply(lambda x: round(x * 1000000, 4)))

                series_min = df[column].min()
                series_max = df[column].max()

                min_val = series_min if series_min < min_val else min_val
                max_val = series_max if series_max > max_val else max_val

    data = {}

    columns = copy.deepcopy(df.columns)

    data["columns"] = fields

    data["minmax"] = [min_val, max_val]

    for column in columns:
        data[column] = df[column].to_list()

    return data
