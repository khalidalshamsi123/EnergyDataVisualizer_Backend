import asyncio
from fastapi import APIRouter
from pydantic import BaseModel

from utils.methods import calculate_word_column_metrics

# Create a class for the options object.
class Option_Object(BaseModel):
    filter: str | None = None
    rows: str | None = None
    metric: str | None = None

router = APIRouter()

def create_data_object():
    data = {}
    data['average'] = {}
    data['sum'] = {}
    return data

@router.post("/api/Thermal_characteristics")
async def get_thermal_characteristic_data_for_charts(options_object: list[Option_Object]):
    dwelling_type_columns = ["detached", "flat", "semi-detached", "terraced"]
    heating_type_columns = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]
    key_words = []
    datasets = {}

    # Loop through the options object. Create a new object for each filter and rows containing the average and sum.
    for object in options_object:
        # Create a unique key based on the filter and rows.
        key = f'{object.filter}:{object.rows}'
        # Create a object containing the keys average or sum.
        data = create_data_object()

        # In this case the values column is the same for all the filters.
        values_column = "Average annual heat demand kWh"

        if object.filter == "heating_type":
            key_words = heating_type_columns
            word_column = "Heating systems"
        elif object.filter == "dwelling_type":
            key_words = dwelling_type_columns
            word_column = "Dwelling forms"

        # Columns to load.
        columns_to_load = [values_column, word_column]

        if object.rows == "before":
            csv_name = "Thermal_characteristics_beforeEE"
            data[f'{object.metric}'] = await calculate_word_column_metrics(key_words, values_column, word_column, columns_to_load, object.metric, csv_name)
        elif object.rows == "after":
            csv_name = "Thermal_characteristics_afterEE"
            data[f'{object.metric}'] = await calculate_word_column_metrics(key_words, values_column, word_column, columns_to_load, object.metric, csv_name)
        elif object.rows == "both":
            # Get the data for before and after.
            csv_name_before = "Thermal_characteristics_beforeEE"
            csv_name_after = "Thermal_characteristics_afterEE"
            results = await asyncio.gather(*[calculate_word_column_metrics(key_words, values_column, word_column, columns_to_load, object.metric, csv_name_before),
                                    calculate_word_column_metrics(key_words, values_column, word_column, columns_to_load, object.metric, csv_name_after)])
            # Add either the sum or average of the before and after data.
            data[f'{object.metric}'] = results

        # Add the key alongside it's data to the overall datasets object.
        datasets[key] = data

    # Return the datasets object.
    return datasets
