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

@app.get("/api")
async def get_pie():
    column_names = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]

    ipc = await r.get('caches:dataframes:Energy_efficiency_improvements_costs_LA:original')
    data = pl.read_ipc_stream(ipc, columns=csv_column_names)
        
    sorted_into_heating_types = [
        (
            column_name,
            data.lazy()
                .select(pl.col(f"^.*({column_name}).*$"))
                .collect()
        )
        for column_name in column_names
    ]

    averages = [
        (
            tuple[0],
            tuple[1].lazy()
            .mean()
            .collect()
        )
        for tuple in sorted_into_heating_types
    ]

    final_averages = []

    for object in averages:
        column_names = object[1].columns
        count = 0
        sum = 0
        for column_name in column_names:
            for number in object[1][column_name]:
                sum = sum + number
                count = count + 1

        # We divide the numbers we've tallied up so far by the amount
        # of data points there were relating to a particular heating type.
        mini_mean = sum / count
        final_mean = mini_mean / len(column_names)
        final_averages.append([object[0], final_mean]) # Name of heating type. Oil, gas etc.

    print(final_averages)

    return final_averages
    # csv_file_path = "Energy_efficiency_improvements_costs_LA.csv"
    # pie_chart_json = pl.read_csv(csv_file_path)
    # #print(pie_chart_json)