from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import maps, tables, Energy_efficiency_improvements_costs_LA, Annual_heat_demand_LSOA, line_graph, \
    Thermal_characteristics, download

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
app.include_router(Thermal_characteristics.router)
app.include_router(line_graph.router)
app.include_router(download.router)

@app.get("/")
async def root():
    return {"message": "Hello World"}
