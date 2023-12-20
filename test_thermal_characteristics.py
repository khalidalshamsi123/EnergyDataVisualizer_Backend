import pytest
from httpx import AsyncClient
from main import app

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_heating_types_before_rows_average_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "heating_type", "rows": "before", "metric": "average"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_mean_values = [15765.225677120825, 22203.95709521921, 6290.303308937876, 15612.598575890848]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_mean_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_heating_types_before_rows_sum_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "heating_type", "rows": "before", "metric": "sum"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_sum_values = [6552248504.570894, 3076469275.3280973, 2255620992.642106, 1939599958.878648]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_sum_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_heating_types_after_rows_average_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "heating_type", "rows": "after", "metric": "average"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_sum_values = [10527.696184971752, 13960.90677065643, 3527.8255074619874, 9667.736665062077]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_sum_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_heating_types_after_rows_sum_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "heating_type", "rows": "after", "metric": "sum"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_sum_values = [4375457922.22085, 1934353437.6083016, 1265032365.2442715, 1201051929.1106572]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_sum_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_dwelling_types_before_rows_average_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "dwelling_type", "rows": "before", "metric": "average"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_mean_values = [20662.334566326088, 7360.218202025956, 13433.8746398528, 11310.990174552595]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_mean_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_dwelling_types_before_rows_sum_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "dwelling_type", "rows": "before", "metric": "sum"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_sum_values = [5393551178.851798, 1762477850.6571355, 3794397892.026423, 2873511809.8843884]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_sum_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_dwelling_types_after_rows_average_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "dwelling_type", "rows": "after", "metric": "average"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_mean_values = [12920.912936647484, 4890.05743293047, 8432.818732430796, 7283.280129297719]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            print(json[key][metric])
            assert json[key][metric][i][1] == expected_mean_values[i]

@pytest.mark.asyncio(scope='session')
async def test_thermal_characteristics_dwelling_types_after_rows_sum_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "dwelling_type", "rows": "after", "metric": "sum"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    async with AsyncClient(app=app, base_url="http://localhost") as client:
        response = await client.post("/api/Thermal_characteristics", json=options_object)

        # Check if the response is 200. Indicating the request was successful.
        assert response.status_code == 200
        json = response.json()
        # Check if the key exists in the json object. If it does not the response is malformed.
        key = f'{filter}:{rows}'
        assert key in json

        expected_sum_values = [3372784666.591903, 1170973152.8895304, 2381849650.9750786, 1850288183.7275686]
        # Loop over each array in the json object under the 'metric' key checking if each value matches the
        # respective 'correct' value the route should have returned.
        for i in range(len(json[key][metric])):
            assert json[key][metric][i][1] == expected_sum_values[i]