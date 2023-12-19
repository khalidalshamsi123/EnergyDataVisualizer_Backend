from fastapi.testclient import TestClient
from main import app

test_client = TestClient(app)

def test_thermal_characteristics_before_rows_average_metric():
    # Create a object containing the options the route expects.
    options_object = [{"filter": "heating_type", "rows": "before", "metric": "average"}]

    # Get the values from the options object.
    filter = options_object[0]["filter"]
    rows = options_object[0]["rows"]
    metric = options_object[0]["metric"]

    response = test_client.post("/api/Thermal_characteristics", json=options_object)

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

# @pytest.mark.asyncio
# async def test_thermal_characteristics_before_rows_sum_metric(event_loop):
#     from main import app
#     async with httpx.AsyncClient(app=app, base_url="http://localhost") as test_client:
#         # Create a object containing the options the route expects.
#         options_object = [{"filter": "heating_type", "rows": "before", "metric": "sum"}]

#         # Get the values from the options object.
#         filter = options_object[0]["filter"]
#         rows = options_object[0]["rows"]
#         metric = options_object[0]["metric"]

#         response = await test_client.post("/api/Thermal_characteristics", json=options_object)

#         # Check if the response is 200. Indicating the request was successful.
#         assert response.status_code == 200
#         json = response.json()
#         # Check if the key exists in the json object. If it does not the response is malformed.
#         key = f'{filter}:{rows}'
#         assert key in json

#         # expected_sum_values = [157652.25677120825, 222039.5709521921, 62903.03308937876, 156125.98575890848]
#         # Loop over each array in the json object under the 'metric' key checking if each value matches the
#         # respective 'correct' value the route should have returned.
#         # for i in range(len(json[key][metric])):
#         #     print(json[key][metric][i][1])
