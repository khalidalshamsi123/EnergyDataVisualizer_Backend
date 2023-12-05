from fastapi import APIRouter, Response

from utils.dataset_utils import get_csv_frame

router = APIRouter()


@router.get("/api/line-graph")
async def get_line_graph():

    df = await get_csv_frame("Half-hourly_profiles_of_heating_technologies")

    # creating a dictionary
    return Response(df.write_json(row_oriented=True), media_type="application/json")
