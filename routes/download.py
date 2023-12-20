from fastapi import APIRouter, Response

from utils.dataset_utils import get_csv_frame

router = APIRouter()


@router.get("/api/download-csv")
async def download_csv(csv_name: str):
    df = await get_csv_frame(csv_name)

    return Response(df.write_csv(), media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={csv_name}.csv"})
