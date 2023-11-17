import os

from fastapi import APIRouter, Response
import polars as pl

router = APIRouter()

dataset = os.getenv("DATA_DIR")

paths = {
    "ukerc_scotland_data": f"{dataset}/Residential heat demand in LSOAs in Scotland/ukerc_scotland_data.csv",
}


@router.get("/tables/{table_id}")
async def read_table(table_id: str, fields: str):
    if table_id not in paths:
        return {"message": f"Table {table_id} not found."}

    df = pl.read_csv(paths[table_id])

    fields = fields.split(",")
    df = df.select(fields)

    return Response(df.write_json(row_oriented=True), media_type="application/json")
