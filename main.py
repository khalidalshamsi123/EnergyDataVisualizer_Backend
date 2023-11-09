from fastapi import FastAPI
import polars as pl

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/bar-chart")
async def get_bar():
    thermal_characteristics_after_ee_json = pl.read_csv(
        '../../Quantification of inherent flexibility from electrified residential heat sector in England and Wales/\
        01 - Thermal_Characteristics/\
        Thermal_characteristics_afterEE.csv').to_json()
    
    thermal_characteristics_before_ee_json = pl.read_csv(
        '../../Quantification of inherent flexibility from electrified residential heat sector in England and Wales/\
        01 - Thermal_Characteristics/\
        Thermal_characteristics_beforeEE.csv').to_json()
    
    bar_chart_json = {
        "thermal_characteristics_after_ee": thermal_characteristics_after_ee_json,
        "thermal_characteristics_before_ee": thermal_characteristics_before_ee_json
    }
    
    return bar_chart_json