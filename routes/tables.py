import polars as pl
from fastapi import APIRouter, Response

from utils.dataset_utils import get_csv_frame

router = APIRouter()


@router.get("/tables/{table_id}")
async def read_table(table_id: str, fields: str, dp: int = 2):
    df = await get_csv_frame(table_id, fields.split(","))

    if df is None:
        return {"message": f"Table {table_id} not found."}

    for column in df.columns:
        if df[column].dtype == pl.Float64:
            df = df.with_columns(pl.col(column).apply(lambda x: round(x, dp)))

    return Response(df.write_json(row_oriented=True), media_type="application/json")
