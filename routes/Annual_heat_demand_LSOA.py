import polars as pl

from fastapi import APIRouter

from pydantic import BaseModel

from utils.methods import get_averages_for_columns_including_word_regex, get_averages_for_columns_including_word, get_sum_for_columns_including_word_regex, get_sum_for_columns_including_word

# Create a class for the options object.
class Option_Object(BaseModel):
    filter: str | None = None
    rows: str | None = None

router = APIRouter()

before_csv_column_names = [
    "Average heat demand before energy efficiency measures for detached biomass boiler (kWh)",
    "Average heat demand before energy efficiency measures for detached gas boiler (kWh)",
    "Average heat demand before energy efficiency measures for detached oil boiler (kWh)",
    "Average heat demand before energy efficiency measures for detached resistance heating (kWh)",
    "Average heat demand before energy efficiency measures for flat biomass boiler (kWh)",
    "Average heat demand before energy efficiency measures for flat gas boiler (kWh)",
    "Average heat demand before energy efficiency measures for flat oil boiler (kWh)",
    "Average heat demand before energy efficiency measures for flat resistance heating (kWh)",
    "Average heat demand before energy efficiency measures for semi-detached biomass boiler (kWh)",
    "Average heat demand before energy efficiency measures for semi-detached gas boiler (kWh)",
    "Average heat demand before energy efficiency measures for semi-detached oil boiler (kWh)",
    "Average heat demand before energy efficiency measures for semi-detached resistance heating (kWh)",
    "Average heat demand before energy efficiency measures for terraced biomass boiler (kWh)",
    "Average heat demand before energy efficiency measures for terraced gas boiler (kWh)",
    "Average heat demand before energy efficiency measures for terraced oil boiler (kWh)",
    "Average heat demand before energy efficiency measures for terraced resistance heating (kWh)"
]

after_csv_column_names = [
    "Average heat demand after energy efficiency measures for detached biomass boiler (kWh)",
    "Average heat demand after energy efficiency measures for detached gas boiler (kWh)",
    "Average heat demand after energy efficiency measures for detached oil boiler (kWh)",
    "Average heat demand after energy efficiency measures for detached resistance heating (kWh)",
    "Average heat demand after energy efficiency measures for flat biomass boiler (kWh)",
    "Average heat demand after energy efficiency measures for flat gas boiler (kWh)",
    "Average heat demand after energy efficiency measures for flat oil boiler (kWh)",
    "Average heat demand after energy efficiency measures for flat resistance heating (kWh)",
    "Average heat demand after energy efficiency measures for semi-detached biomass boiler (kWh)",
    "Average heat demand after energy efficiency measures for semi-detached gas boiler (kWh)",
    "Average heat demand after energy efficiency measures for semi-detached oil boiler (kWh)",
    "Average heat demand after energy efficiency measures for semi-detached resistance heating (kWh)",
    "Average heat demand after energy efficiency measures for terraced biomass boiler (kWh)",
    "Average heat demand after energy efficiency measures for terraced gas boiler (kWh)",
    "Average heat demand after energy efficiency measures for terraced oil boiler (kWh)",
    "Average heat demand after energy efficiency measures for terraced resistance heating (kWh)"
]

def create_data_object():
    data = {}
    data['average'] = {}
    data['sum'] = {}
    return data

@router.post("/api/Annual_heat_demand_LSOA")
async def get_requested_data_for_charts(options_object: list[Option_Object]):
    dwelling_type_columns = ["detached", "flat", "semi-detached", "terraced"]
    heating_type_columns = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]

    datasets = {}
    # Loop through the options object. Create a new object for each filter and rows containing the average and sum.
    for object in options_object:
        if object.filter == "heating_type" and object.rows == "before":
            # Create a unique key based on the filter and rows.
            key = f'{object.filter}:{object.rows}'
            # Create a object containing the keys average and sum.
            data = create_data_object()
            data['average'] = await get_averages_for_columns_including_word_regex(heating_type_columns, before_csv_column_names, 'Annual_heat_demand_LSOA')
            data['sum'] = await get_sum_for_columns_including_word_regex(heating_type_columns, before_csv_column_names, 'Annual_heat_demand_LSOA')
            # Add the key alongside it's data to the overall datasets object.
            datasets[key] = data
        elif object.filter == "heating_type" and object.rows == "after":
            # Create a unique key based on the filter and rows.
            key = f'{object.filter}:{object.rows}'
            # Create a object containing the keys average and sum.
            data = create_data_object()
            data['average'] = await get_averages_for_columns_including_word_regex(heating_type_columns, after_csv_column_names, 'Annual_heat_demand_LSOA')
            data['sum'] = await get_sum_for_columns_including_word_regex(heating_type_columns, after_csv_column_names, 'Annual_heat_demand_LSOA')
            # Add the key alongside it's data to the overall datasets object.
            datasets[key] = data
        elif object.filter == "dwelling_type" and object.rows == "before":
            # Create a unique key based on the filter and rows.
            key = f'{object.filter}:{object.rows}'
            # Create a object containing the keys average and sum.
            data = create_data_object()
            data['average'] = await get_averages_for_columns_including_word(dwelling_type_columns, before_csv_column_names, 'Annual_heat_demand_LSOA')
            data['sum'] = await get_sum_for_columns_including_word(dwelling_type_columns, before_csv_column_names, 'Annual_heat_demand_LSOA')
            # Add the key alongside it's data to the overall datasets object.
            datasets[key] = data
        elif object.filter == "dwelling_type" and object.rows == "after":
            # Create a unique key based on the filter and rows.
            key = f'{object.filter}:{object.rows}'
            # Create a object containing the keys average and sum.
            data = create_data_object()
            data['average'] = await get_averages_for_columns_including_word(dwelling_type_columns, after_csv_column_names, 'Annual_heat_demand_LSOA')
            data['sum'] = await get_sum_for_columns_including_word(dwelling_type_columns, after_csv_column_names, 'Annual_heat_demand_LSOA')
            # Add the key alongside it's data to the overall datasets object.
            datasets[key] = data

    # Return the datasets array.
    return datasets