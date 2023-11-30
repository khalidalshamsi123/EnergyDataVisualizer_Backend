import polars as pl

from fastapi import APIRouter

from utils.methods import get_averages_for_columns_including_word_regex

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

@router.get("/api/Annual_heat_demand_LSOA/average/heating_type")
async def get_average_heat_demand_per_heating_type():
    column_names = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]
    average_heat_demand_per_heating_type = await get_averages_for_columns_including_word_regex(column_names, csv_column_names, 'Annual_heat_demand_LSOA')
    return average_heat_demand_per_heating_type