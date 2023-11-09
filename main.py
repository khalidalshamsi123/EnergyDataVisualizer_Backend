from fastapi import FastAPI
import polars as pl

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/bar-chart")
async def get_bar():
    ## thermal_characteristics_after_ee_json = pl.read_csv(
    ##    'Thermal_characteristics_afterEE.csv').to_dicts()
    
    ## thermal_characteristics_before_ee_json = pl.read_csv(
    ##    'Thermal_characteristics_beforeEE.csv').to_dicts()
    
    ## bar_chart_json = {
    ##    "thermal_characteristics_after_ee": thermal_characteristics_after_ee_json,
    ##    "thermal_characteristics_before_ee": thermal_characteristics_before_ee_json
    ## }

    thermal_characteristics_after_ee_json = pl.read_csv(
        'thermal_after_ee.csv').to_dicts()
    
    thermal_characteristics_before_ee_json = pl.read_csv(
        'thermal_before_ee.csv').to_dicts()
    
    bar_chart_json = {
        "thermal_characteristics_after_ee": thermal_characteristics_after_ee_json,
        "thermal_characteristics_before_ee": thermal_characteristics_before_ee_json
    }

    return bar_chart_json