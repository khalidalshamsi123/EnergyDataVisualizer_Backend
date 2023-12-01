from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import maps, tables, Energy_efficiency_improvements_costs_LA, Annual_heat_demand_LSOA

import polars as pl

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(maps.router)
app.include_router(tables.router)
app.include_router(Energy_efficiency_improvements_costs_LA.router)
app.include_router(Annual_heat_demand_LSOA.router)

# Test comment.

@app.get("/")
async def root():
    return {"message": "Hello World"}

#Line Graph API reader
@app.get("/api/line-graph")
async def get_line_graph():
    #reading the csv file 'Half-Hourly-profiles-ofheating-technologies'
    json_line_graph = pl.read_csv("C:\\Users\\c21086065\\OneDrive - Cardiff University\\Y3-Commercial Frameworks\\dataset\\Spatio-temporal heat demand for LSOAs in England and Wales\\Half-hourly_profiles_of_heating_technologies.csv")
    #creating a dictionary 
    return json_line_graph.to_dicts()
