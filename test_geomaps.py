import pytest
from httpx import AsyncClient
from main import app


@pytest.mark.asyncio(scope='session')
async def test_maps():
    async with AsyncClient(app=app, base_url="http://localhost") as ac:
        response = await ac.get(
            "/maps/Annual_heat_demand_LSOA?sql="
            "SELECT%20geometry%2C%20%22Area%20(km2)%22%20FROM%20frame"
            "&lsoa_field=LSOA11CD"
        )
        assert response.status_code == 200

        json = response.json()

        assert json["type"] == "FeatureCollection"

        assert json["features"][0]["type"] == "Feature"
        assert json["features"][0]["properties"]["0"] == "Area (km2)"

        assert json["features"][1]["geometry"]["type"] == "Polygon"
        assert json["features"][1]["properties"]["0"] == 0.35
