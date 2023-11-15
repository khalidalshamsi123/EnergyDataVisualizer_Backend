from fastapi import FastAPI
import polars as pl

from utils.redis_pool import get_redis

r = get_redis()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/bar-chart")
async def get_bar():
    # Get the IPC stream of bytes from Redis using the relevant key.
    afterEE_Data = await r.get("caches:dataframes:Thermal_characteristics_afterEE:original")
    # Loads the IPC stream of bytes into a DataFrame. Only loads the columns "Average annual heat demand kWh" and "Heating systems" into memory.
    afterEE_Data = pl.read_ipc_stream(afterEE_Data, columns=["Average annual heat demand kWh", "Heating systems"])

    # Essentially here I am setting up a series of operations to be performed on the DataFrame.
    # We select the column Heating systems, and filter out any rows that have values that another
    # row has for that column. We then collect the values.

    # TODO. It would make much more sense to read both tables into memory before this query.
    # then perform a join and get the unique values from the joined tables combined. Since
    # right now we are just creating the list of heating types from one of the CSVs.
    # IMPORTANT: I should only join the tables temporarily. We don't want to use this dataframe in any other parts of this code.
    unique_heating_types = ( 
        afterEE_Data.lazy()
            .select(["Heating systems"])
            .unique(maintain_order=True)
            .collect()["Heating systems"] 
    )

    # We create a list of tuples. Each tuple contains a unique heating type and a mean average.
    # The mean is calculated with the Average annual heat demand kWh columns values for all rows
    # that have the same heating type. Generating a overall mean for each unique heating type.
    afterEE_Heating_Type_Means = [
        (
            heating_type,
            afterEE_Data.lazy()
                    .filter(pl.col("Heating systems") == heating_type)
                    .select([pl.col("Average annual heat demand kWh")])
                    .mean()
                    .collect()['Average annual heat demand kWh']
        )
        for heating_type in unique_heating_types
    ]

    # Get the max value out of all the averages in the list of tuples. This will be used for the y-axis scale.
    # The _ is a throwaway variable. We don't need the heating type here.
    afterEE_Data_Max = max([result.item() for _, result in afterEE_Heating_Type_Means])

    # We create a list of json objects. We iterate through each unique heating type and mean value
    # within the afterEE_Heating_Type_Means list of tuples.
    afterEE_Data = [{"heatingType": heating_type, "average": result.item()}
               for heating_type, result in afterEE_Heating_Type_Means]

    # Get the IPC stream of bytes from Redis using the relevant key.
    beforeEE_Data = await r.get("caches:dataframes:Thermal_characteristics_beforeEE:original")
    # Loads the IPC stream of bytes from Redis into a DataFrame. Only loads the columns "Average annual heat demand kWh" and "Heating systems" into memory.
    beforeEE_Data = pl.read_ipc_stream(beforeEE_Data, columns=["Average annual heat demand kWh", "Heating systems"])

    # We create a list of tuples. Each tuple contains a unique heating type and a mean average.
    # The mean is calculated with the Average annual heat demand kWh columns values for all rows
    # that have the same heating type. Generating a overall mean for each unique heating type.
    beforeEE_Heating_Type_Means = [
        (
            heating_type,
            beforeEE_Data.lazy()
                    .filter(pl.col("Heating systems") == heating_type)
                    .select([pl.col("Average annual heat demand kWh")])
                    .mean()
                    .collect()['Average annual heat demand kWh']
        )
        for heating_type in unique_heating_types
    ]

    # Get the max value out of all the averages in the list of tuples. This will be used for the y-axis scale.
    # The _ is a throwaway variable. We don't need the heating type here.
    beforeEE_Data_Max = max([result.item() for _, result in beforeEE_Heating_Type_Means])

    # We create a list of json objects. We iterate through each unique heating type and mean value
    # within the beforeEE_Heating_Type_Means list of tuples.
    beforeEE_Data = [{"heatingType": heating_type, "average": result.item()}
               for heating_type, result in beforeEE_Heating_Type_Means]

    bar_chart_json = {
        "heating_types": unique_heating_types.to_list(),
        "thermal_characteristics_after_ee": afterEE_Data,
        "after_ee_max_value": afterEE_Data_Max,
        "thermal_characteristics_before_ee": beforeEE_Data,
        "before_ee_max_value": beforeEE_Data_Max
    }

    return bar_chart_json