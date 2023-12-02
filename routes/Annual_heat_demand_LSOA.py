import polars as pl

from fastapi import APIRouter

from utils.methods import get_averages_for_columns_including_word_regex, get_averages_for_columns_including_word, get_sum_for_columns_including_word_regex, get_sum_for_columns_including_word

router = APIRouter()

csv_column_names = [
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

@router.get("/api/Annual_heat_demand_LSOA/heating_type/before")
async def get_average_heat_demand_per_heating_type():
    column_names = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]
    average_heat_demand_per_heating_type = await get_averages_for_columns_including_word_regex(column_names, csv_column_names, 'Annual_heat_demand_LSOA')
    sum_heat_demand_per_heating_type = await get_sum_for_columns_including_word_regex(column_names, csv_column_names, 'Annual_heat_demand_LSOA')
    return  { "average": average_heat_demand_per_heating_type, "sum": sum_heat_demand_per_heating_type }

@router.get("/api/Annual_heat_demand_LSOA/dwelling_type/before")
async def get_average_heat_demand_per_dwelling_type():
    column_names = ["detached", "flat", "semi-detached", "terraced"]
    average_heat_demand_per_dwelling_type = await get_averages_for_columns_including_word(column_names, csv_column_names, 'Annual_heat_demand_LSOA')
    sum_heat_demand_per_dwelling_type = await get_sum_for_columns_including_word(column_names, csv_column_names, 'Annual_heat_demand_LSOA')
    return { "average": average_heat_demand_per_dwelling_type, "sum": sum_heat_demand_per_dwelling_type }

# Still need the 'after' routes.