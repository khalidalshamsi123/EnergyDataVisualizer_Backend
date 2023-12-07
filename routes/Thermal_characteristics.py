from fastapi import APIRouter

from utils.methods import get_thermal_characteristics_csv_data

router = APIRouter()

@router.get("/api/bar-chart")
async def get_thermal_characteristic_data_for_charts():
    chart_data = await get_thermal_characteristics_csv_data()
    return chart_data