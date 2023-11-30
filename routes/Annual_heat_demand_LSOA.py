import polars as pl

from fastapi import APIRouter

from utils.redis_pool import get_redis

router = APIRouter()

@router.get("/api/Annual_heat_demand_LSOA")
async def get_average_heat_demand_per_heating_type():
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

    column_names = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]

    ipc = await get_redis().get('caches:dataframes:Annual_heat_demand_LSOA:original')
    data = pl.read_ipc_stream(ipc, columns=csv_column_names)

    # We iterate over the column names, and select all columns that contain the name of a heating type.
    # For each, returning a tuple containing the heating type and the DataFrame with the columns/data
    # for that heating type.
    sorted_into_heating_types = [
        (
            column_name,
            data.lazy()
            .select(pl.col(f"^.*({column_name}).*$"))
            .collect()
        )
        for column_name in column_names
    ]

    final_averages = []

    # Will iterate over tuple in sorted_into_heating_types. Tuple[0] is the name of the heating type.
    # Tuple[1] is the DataFrame containing the data for that heating type.

    # We iterate over the columns in the DataFrame, and then iterate over the numbers in each column.
    # We add up all the numbers in the DataFrame and divide by the amount of numbers there are.

    # This allows us to calculate the mean for each heating type.
    for tuple in sorted_into_heating_types:
        column_names = tuple[1].columns
        count = 0
        sum = 0
        for column_name in column_names:
            for number in tuple[1][column_name]:
                sum = sum + number
                count = count + 1

        # We divide the numbers we've tallied up so far by the amount
        # of data points there were relating to a particular heating type.
        final_mean = sum / count
        final_averages.append([tuple[0], final_mean])  # Name of heating type. Oil, gas etc. And the mean.

    return final_averages