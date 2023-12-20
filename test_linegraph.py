import pytest
from httpx import AsyncClient
from main import app


@pytest.mark.asyncio(scope='session')
async def test_maps():
    async with AsyncClient(app=app, base_url="http://localhost") as ac:
        response = await ac.get(
            "/api/line-graph?fields="
            "index,Normalised_Gas_boiler_heat,"
            "Normalised_Resistance_heater_heat,"
            "Normalised_ASHP_heat,Normalised_GSHP_heat"
        )
        assert response.status_code == 200

        json = response.json()

        assert json["columns"] == [
            "index",
            "Normalised_Gas_boiler_heat",
            "Normalised_Resistance_heater_heat",
            "Normalised_ASHP_heat",
            "Normalised_GSHP_heat"
        ]

        assert json["minmax"] == [0.0, 251.4635]
