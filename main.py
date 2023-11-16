from fastapi import FastAPI
import polars as pl
import csv
import logging
import json
app = FastAPI()

from utils.redis_pool import get_redis

r = get_redis()

@app.get("/")
async def root():
    return {"message": "Hello World"}

csv_column_names = [
    "Average energy efficiency improvements costs of detached gas boiler (GBP)",
    "Average energy efficiency improvements costs of detached oil boiler (GBP)",
    "Average energy efficiency improvements costs of detached resistance heating (GBP)",
    "Average energy efficiency improvements costs of detached biomass boiler (GBP)",
    "Average energy efficiency improvements costs of flat gas boiler (GBP)",
    "Average energy efficiency improvements costs of flat oil boiler (GBP)",
    "Average energy efficiency improvements costs of flat resistance heating (GBP)",
    "Average energy efficiency improvements costs of flat biomass boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached gas boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached oil boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached resistance heating (GBP)",
    "Average energy efficiency improvements costs of semi-detached biomass boiler (GBP)",
    "Average energy efficiency improvements costs of terraced gas boiler (GBP)",
    "Average energy efficiency improvements costs of terraced oil boiler (GBP)",
    "Average energy efficiency improvements costs of terraced resistance heating (GBP)",
    "Average energy efficiency improvements costs of terraced biomass boiler (GBP)"
]

@app.get('/api/bob')
async def get_test():
    csv_file_path = "data/Energy_efficiency_improvements_costs_LA.csv"
    pie_chart_json = pl.read_csv(csv_file_path).to_dicts()

    return pie_chart_json

@app.get("/api")
async def get_pie():
    column_names = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]

    ipc = await r.get('caches:dataframes:Energy_efficiency_improvements_costs_LA:original')
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

    # We iterate over each tuple in sorted_into_heating_types.
    # For each we create a new tuple in the array, defined with the following:
    #  - The heating type name.
    #  - A dataframe containing the count of rows in the sorted DataFrame.
    #  - A dataframe containing the sum of all values for each column in the sorted DataFrame.
    sorted_data = [
        (
            tuple[0],
            tuple[1].lazy().select(pl.count())
            .collect(),
            tuple[1].lazy().sum()
            .collect()
        )
        for tuple in sorted_into_heating_types
    ]

    final_averages = []

    # We iterate over each tuple in sorted_data.
    # for each we grab the column name, count of rows in the DataFrame, and a DataFrame
    # containing the sum of all values in each column.
    for tuple in sorted_data:
        column_name = tuple[0]
        sums = tuple[2][0]
        count = tuple[1][0].item()

        # We iterate over each sum, adding it to the total sum.
        total_sum = 0
        for sum in sums:
            total_sum = total_sum + sum.item()

        # There are multiple columns per row related to a heating type.
        # Meaning in order for us to properly generate the mean, we need
        # to multiply the count by the amount of columns that are used.

        # In this context, for this CSV, we know to multiply by 4.
        mean = total_sum / (count * 4)

        # We append the column name and mean to the final averages array.
        # and move onto the next tuple to process.
        final_averages.append([column_name, mean]) # Name of heating type. Oil, gas etc.

    return final_averages